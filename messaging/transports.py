import asyncio
import json
import logging
from abc import ABC, abstractmethod
from typing import Any, Awaitable, Callable, Dict, List, Optional

import aio_pika

try:
    from azure.servicebus import ServiceBusMessage
    from azure.servicebus.aio import ServiceBusClient
except ImportError:
    ServiceBusMessage = None
    ServiceBusClient = None


def detect_backend(broker_url: str, transport_backend: str = "") -> str:
    backend = (transport_backend or "").strip().lower()
    if backend in {"rabbitmq", "servicebus"}:
        return backend
    if broker_url.startswith("Endpoint="):
        return "servicebus"
    return "rabbitmq"


def servicebus_body_to_bytes(message: Any) -> bytes:
    body = message.body
    if isinstance(body, bytes):
        return body
    if isinstance(body, str):
        return body.encode("utf-8")
    if body is None:
        return b""

    chunks = []
    for chunk in body:
        if isinstance(chunk, bytes):
            chunks.append(chunk)
        elif isinstance(chunk, str):
            chunks.append(chunk.encode("utf-8"))
        else:
            chunks.append(bytes(chunk))
    return b"".join(chunks)


class ApiTransport(ABC):
    @abstractmethod
    async def startup(self, workers: List[str]) -> None:
        raise NotImplementedError

    @abstractmethod
    async def shutdown(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def publish_job(
        self, model_name: str, job_data: dict, correlation_id: str
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    async def wait_for_result(self, correlation_id: str, timeout: float) -> Dict[str, Any]:
        raise NotImplementedError

    async def get_queue_depth(self, queue_name: str) -> int:
        return 0

    async def get_queue_runtime_stats(self, queue_name: str) -> Dict[str, int]:
        depth = await self.get_queue_depth(queue_name)
        return {
            "ready": depth,
            "unacked": 0,
            "total": depth,
            "consumers": 0,
            "completed": 0,
        }


class RabbitApiTransport(ApiTransport):
    def __init__(
        self,
        broker_url: str,
        results_queue: str,
        queue_stats_provider: Optional[Callable[[str], Dict[str, int]]] = None,
    ):
        self.broker_url = broker_url
        self.results_queue = results_queue
        self.queue_stats_provider = queue_stats_provider
        self.connection: Optional[aio_pika.RobustConnection] = None
        self.channel: Optional[aio_pika.Channel] = None
        self.exchange: Optional[aio_pika.Exchange] = None
        self.reply_queue: Optional[aio_pika.Queue] = None
        self.pending: Dict[str, asyncio.Future] = {}
        self.consumer_task: Optional[asyncio.Task] = None

    async def startup(self, workers: List[str]) -> None:
        self.connection = await aio_pika.connect_robust(self.broker_url)
        self.channel = await self.connection.channel()
        self.exchange = await self.channel.declare_exchange(
            "jobs", aio_pika.ExchangeType.DIRECT, durable=True
        )

        for worker in workers:
            queue_name = f"jobs.{worker}"
            queue = await self.channel.declare_queue(queue_name, durable=True)
            await queue.bind(self.exchange, queue_name)

        self.reply_queue = await self.channel.declare_queue(self.results_queue, durable=True)
        self.consumer_task = asyncio.create_task(self._consume_results())

    async def shutdown(self) -> None:
        if self.consumer_task:
            self.consumer_task.cancel()
            try:
                await self.consumer_task
            except asyncio.CancelledError:
                pass

        for future in self.pending.values():
            if not future.done():
                future.cancel()
        self.pending.clear()

        if self.channel and not self.channel.is_closed:
            await self.channel.close()
        if self.connection and not self.connection.is_closed:
            await self.connection.close()

    async def publish_job(self, model_name: str, job_data: dict, correlation_id: str) -> None:
        if not self.exchange:
            raise RuntimeError("Rabbit transport not initialized")

        queue_name = f"jobs.{model_name}"
        message = aio_pika.Message(
            body=json.dumps(job_data).encode(),
            content_type="application/json",
            reply_to=self.results_queue,
            correlation_id=correlation_id,
        )
        await self.exchange.publish(message, routing_key=queue_name)

    async def wait_for_result(self, correlation_id: str, timeout: float) -> Dict[str, Any]:
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        self.pending[correlation_id] = future
        try:
            return await asyncio.wait_for(future, timeout=timeout)
        except asyncio.TimeoutError:
            return {"status": "timeout"}
        finally:
            self.pending.pop(correlation_id, None)

    async def _consume_results(self) -> None:
        if not self.reply_queue:
            return

        async with self.reply_queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    try:
                        correlation_id = message.correlation_id
                        payload = json.loads(message.body.decode())
                    except Exception:
                        continue

                    if not correlation_id:
                        continue

                    future = self.pending.get(correlation_id)
                    if future and not future.done():
                        future.set_result(payload)

    async def get_queue_depth(self, queue_name: str) -> int:
        if not self.channel:
            return 0
        try:
            declaration = await self.channel.declare_queue(queue_name, passive=True)
            return declaration.declaration_result.message_count
        except Exception:
            return 0

    async def get_queue_runtime_stats(self, queue_name: str) -> Dict[str, int]:
        if self.queue_stats_provider is None:
            return await super().get_queue_runtime_stats(queue_name)
        try:
            return await asyncio.to_thread(self.queue_stats_provider, queue_name)
        except Exception:
            return await super().get_queue_runtime_stats(queue_name)


class ServiceBusApiTransport(ApiTransport):
    def __init__(self, connection_string: str, results_queue: str):
        if ServiceBusClient is None or ServiceBusMessage is None:
            raise RuntimeError(
                "azure-servicebus is not installed. Add it to requirements to use servicebus backend."
            )
        self.connection_string = connection_string
        self.results_queue = results_queue
        self.client: Optional[ServiceBusClient] = None
        self.senders: Dict[str, Any] = {}
        self.receiver: Any = None
        self.pending: Dict[str, asyncio.Future] = {}
        self.consumer_task: Optional[asyncio.Task] = None

    async def startup(self, workers: List[str]) -> None:
        self.client = ServiceBusClient.from_connection_string(self.connection_string)
        for worker in workers:
            queue_name = f"jobs.{worker}"
            self.senders[queue_name] = self.client.get_queue_sender(queue_name=queue_name)
        self.receiver = self.client.get_queue_receiver(queue_name=self.results_queue)
        self.consumer_task = asyncio.create_task(self._consume_results())

    async def shutdown(self) -> None:
        if self.consumer_task:
            self.consumer_task.cancel()
            try:
                await self.consumer_task
            except asyncio.CancelledError:
                pass

        for future in self.pending.values():
            if not future.done():
                future.cancel()
        self.pending.clear()

        for sender in self.senders.values():
            await sender.close()
        self.senders.clear()

        if self.receiver:
            await self.receiver.close()
        if self.client:
            await self.client.close()

    async def publish_job(self, model_name: str, job_data: dict, correlation_id: str) -> None:
        queue_name = f"jobs.{model_name}"
        sender = self.senders.get(queue_name)
        if not sender:
            raise RuntimeError(f"No sender configured for queue {queue_name}")

        message = ServiceBusMessage(
            json.dumps(job_data),
            content_type="application/json",
            correlation_id=correlation_id,
            reply_to=self.results_queue,
        )
        await sender.send_messages(message)

    async def wait_for_result(self, correlation_id: str, timeout: float) -> Dict[str, Any]:
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        self.pending[correlation_id] = future
        try:
            return await asyncio.wait_for(future, timeout=timeout)
        except asyncio.TimeoutError:
            return {"status": "timeout"}
        finally:
            self.pending.pop(correlation_id, None)

    async def _consume_results(self) -> None:
        if not self.receiver:
            return

        while True:
            messages = await self.receiver.receive_messages(
                max_message_count=20, max_wait_time=2
            )
            if not messages:
                await asyncio.sleep(0)
                continue

            for message in messages:
                try:
                    payload = json.loads(servicebus_body_to_bytes(message).decode("utf-8"))
                    correlation_id = message.correlation_id
                except Exception:
                    await self.receiver.complete_message(message)
                    continue

                if correlation_id:
                    future = self.pending.get(correlation_id)
                    if future and not future.done():
                        future.set_result(payload)

                await self.receiver.complete_message(message)


class WorkerTransport(ABC):
    @abstractmethod
    async def run(self, handler: Callable[[Dict[str, Any]], Awaitable[Dict[str, Any]]]) -> None:
        raise NotImplementedError

    @abstractmethod
    async def shutdown(self) -> None:
        raise NotImplementedError


class RabbitWorkerTransport(WorkerTransport):
    def __init__(self, broker_url: str, queue_name: str):
        self.broker_url = broker_url
        self.queue_name = queue_name
        self.connection: Optional[aio_pika.RobustConnection] = None
        self.channel: Optional[aio_pika.Channel] = None

    async def run(self, handler: Callable[[Dict[str, Any]], Awaitable[Dict[str, Any]]]) -> None:
        self.connection = await aio_pika.connect_robust(self.broker_url)
        self.channel = await self.connection.channel()

        exchange = await self.channel.declare_exchange(
            "jobs", aio_pika.ExchangeType.DIRECT, durable=True
        )
        queue = await self.channel.declare_queue(self.queue_name, durable=True)
        await queue.bind(exchange, self.queue_name)

        await self.channel.set_qos(prefetch_count=1)
        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    job = json.loads(message.body.decode())
                    result = await handler(job)
                    if message.reply_to:
                        reply = aio_pika.Message(
                            body=json.dumps(result).encode(),
                            content_type="application/json",
                            correlation_id=message.correlation_id,
                        )
                        await self.channel.default_exchange.publish(
                            reply,
                            routing_key=message.reply_to,
                        )

    async def shutdown(self) -> None:
        if self.channel and not self.channel.is_closed:
            await self.channel.close()
        if self.connection and not self.connection.is_closed:
            await self.connection.close()


class ServiceBusWorkerTransport(WorkerTransport):
    def __init__(self, connection_string: str, queue_name: str):
        if ServiceBusClient is None or ServiceBusMessage is None:
            raise RuntimeError(
                "azure-servicebus is not installed. Add it to requirements to use servicebus backend."
            )
        self.connection_string = connection_string
        self.queue_name = queue_name
        self.client: Optional[ServiceBusClient] = None
        self.receiver: Any = None
        self.sender_cache: Dict[str, Any] = {}

    async def run(self, handler: Callable[[Dict[str, Any]], Awaitable[Dict[str, Any]]]) -> None:
        self.client = ServiceBusClient.from_connection_string(self.connection_string)
        self.receiver = self.client.get_queue_receiver(queue_name=self.queue_name)

        while True:
            messages = await self.receiver.receive_messages(max_message_count=1, max_wait_time=5)
            if not messages:
                await asyncio.sleep(0)
                continue

            for message in messages:
                try:
                    job = json.loads(servicebus_body_to_bytes(message).decode("utf-8"))
                    result = await handler(job)
                    if message.reply_to:
                        sender = self.sender_cache.get(message.reply_to)
                        if sender is None:
                            sender = self.client.get_queue_sender(queue_name=message.reply_to)
                            self.sender_cache[message.reply_to] = sender

                        reply = ServiceBusMessage(
                            json.dumps(result),
                            content_type="application/json",
                            correlation_id=message.correlation_id,
                        )
                        await sender.send_messages(reply)

                    await self.receiver.complete_message(message)
                except Exception:
                    await self.receiver.abandon_message(message)

    async def shutdown(self) -> None:
        for sender in self.sender_cache.values():
            await sender.close()
        self.sender_cache.clear()

        if self.receiver:
            await self.receiver.close()
        if self.client:
            await self.client.close()


def create_api_transport(
    broker_url: str,
    results_queue: str,
    transport_backend: str = "",
    queue_stats_provider: Optional[Callable[[str], Dict[str, int]]] = None,
) -> ApiTransport:
    backend = detect_backend(broker_url, transport_backend)
    if backend == "servicebus":
        return ServiceBusApiTransport(broker_url, results_queue)
    return RabbitApiTransport(
        broker_url,
        results_queue,
        queue_stats_provider=queue_stats_provider,
    )


def create_worker_transport(
    broker_url: str,
    queue_name: str,
    transport_backend: str = "",
) -> WorkerTransport:
    backend = detect_backend(broker_url, transport_backend)
    if backend == "servicebus":
        return ServiceBusWorkerTransport(broker_url, queue_name)
    return RabbitWorkerTransport(broker_url, queue_name)
