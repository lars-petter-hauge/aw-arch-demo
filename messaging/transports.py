import asyncio
import json
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Awaitable, Callable, Dict, List, Optional

import aio_pika

try:
    from azure.servicebus import ServiceBusMessage
    from azure.servicebus.aio import ServiceBusClient
except ImportError:
    ServiceBusMessage = None
    ServiceBusClient = None


# ---------------------------------------------------------------------------
# Alternative: Redis-based heartbeat
# ---------------------------------------------------------------------------
# Instead of using AMQP for worker heartbeats, Redis TTL keys could be used:
#
#   Worker side:
#       await redis.set(f"worker:heartbeat:{worker_name}", timestamp, ex=15)
#
#   API side:
#       is_warm = await redis.exists(f"worker:heartbeat:{worker_name}")
#
# Pros of Redis approach:
#   + Keys auto-expire (TTL) — no manual stale detection needed; if the worker
#     dies the key simply disappears after `ex` seconds with zero cleanup code.
#   + Shared state across all API replicas — every instance sees the same
#     heartbeat data without each needing its own AMQP consumer.
#   + Simpler consumer code — a single EXISTS/GET call vs. a background
#     asyncio task consuming from a fanout exchange.
#   + Survives API restarts — Redis persists the heartbeat state, so a freshly
#     restarted API instance immediately knows which workers are warm.
#
# Cons of Redis approach:
#   + Adds a new dependency pattern — even though Redis already exists in this
#     stack for result caching, mixing signalling semantics into the cache layer
#     conflates two concerns and makes Redis harder to replace or remove later.
#   + Not portable — if Redis is swapped out (e.g. for a managed alternative
#     without pub/sub or TTL support) the heartbeat mechanism breaks silently.
#   + Polling model — the API must actively query Redis; with AMQP the broker
#     pushes heartbeats to the API, which is more event-driven.
#
# The AMQP fanout approach chosen here keeps all messaging in one place and
# avoids adding a second signalling channel to the architecture.
# ---------------------------------------------------------------------------

HEARTBEAT_EXCHANGE = "heartbeat"
HEARTBEAT_INTERVAL = 5  # seconds between worker heartbeat publishes


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


def _require_servicebus_sdk() -> None:
    if ServiceBusClient is None or ServiceBusMessage is None:
        raise RuntimeError(
            "azure-servicebus is not installed. Add it to requirements to use servicebus backend."
        )


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


class BaseApiTransport(ApiTransport):
    def __init__(self) -> None:
        self.pending: Dict[str, asyncio.Future] = {}
        self.consumer_task: Optional[asyncio.Task] = None

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

        await self._shutdown_backend()

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

    def _resolve_pending(self, correlation_id: Optional[str], payload: Dict[str, Any]) -> None:
        if not correlation_id:
            return
        future = self.pending.get(correlation_id)
        if future and not future.done():
            future.set_result(payload)

    @abstractmethod
    async def _shutdown_backend(self) -> None:
        raise NotImplementedError


class RabbitApiTransport(BaseApiTransport):
    """
    API-side AMQP transport using RabbitMQ.

    In addition to job publishing and result consumption, this transport
    subscribes to worker heartbeats via a fanout exchange. Each API instance
    creates an exclusive, auto-delete queue bound to the 'heartbeat' fanout
    exchange. This means:
      - No message accumulation: the queue vanishes when the API disconnects.
      - Multiple API replicas each receive every heartbeat independently.
      - No manual cleanup required.

    Heartbeat messages are used to track which workers are currently warm
    (i.e. running and connected) so the API can apply an appropriate timeout
    when submitting jobs (see is_worker_warm() in main.py).
    """

    def __init__(
        self,
        broker_url: str,
        results_queue: str,
        queue_stats_provider: Optional[Callable[[str], Dict[str, int]]] = None,
    ):
        super().__init__()
        self.broker_url = broker_url
        self.results_queue = results_queue
        self.queue_stats_provider = queue_stats_provider
        self.connection: Optional[aio_pika.RobustConnection] = None
        self.channel: Optional[aio_pika.Channel] = None
        self.exchange: Optional[aio_pika.Exchange] = None
        self.reply_queue: Optional[aio_pika.Queue] = None
        self.heartbeat_task: Optional[asyncio.Task] = None

        # worker_name -> datetime of last received heartbeat
        self.worker_last_seen: Dict[str, datetime] = {}

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

        # Subscribe to worker heartbeats via fanout exchange.
        # The queue is exclusive and auto-delete so it disappears when this
        # API instance disconnects — no stale queues left in the broker.
        heartbeat_exchange = await self.channel.declare_exchange(
            HEARTBEAT_EXCHANGE, aio_pika.ExchangeType.FANOUT, durable=True
        )
        heartbeat_queue = await self.channel.declare_queue(
            "", exclusive=True, auto_delete=True
        )
        await heartbeat_queue.bind(heartbeat_exchange)
        self.heartbeat_task = asyncio.create_task(
            self._consume_heartbeats(heartbeat_queue)
        )

    async def _consume_heartbeats(self, queue: aio_pika.Queue) -> None:
        """Consume heartbeat messages and update worker_last_seen timestamps."""
        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    try:
                        payload = json.loads(message.body.decode())
                        worker_name = payload.get("worker")
                        if worker_name:
                            self.worker_last_seen[worker_name] = datetime.utcnow()
                    except Exception:
                        pass

    async def _shutdown_backend(self) -> None:
        if self.heartbeat_task:
            self.heartbeat_task.cancel()
            try:
                await self.heartbeat_task
            except asyncio.CancelledError:
                pass

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
                    self._resolve_pending(correlation_id, payload)

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


class ServiceBusApiTransport(BaseApiTransport):
    def __init__(self, connection_string: str, results_queue: str):
        _require_servicebus_sdk()
        super().__init__()
        self.connection_string = connection_string
        self.results_queue = results_queue
        self.client: Optional[ServiceBusClient] = None
        self.senders: Dict[str, Any] = {}
        self.receiver: Any = None

        # Azure Service Bus does not support a fanout heartbeat pattern in the
        # same way RabbitMQ does. For Service Bus deployments, worker warmth
        # detection is not implemented and is_worker_warm() will always return
        # True (i.e. assume warm, use normal job timeouts).
        self.worker_last_seen: Dict[str, datetime] = {}

    async def startup(self, workers: List[str]) -> None:
        self.client = ServiceBusClient.from_connection_string(self.connection_string)
        for worker in workers:
            queue_name = f"jobs.{worker}"
            self.senders[queue_name] = self.client.get_queue_sender(queue_name=queue_name)
        self.receiver = self.client.get_queue_receiver(queue_name=self.results_queue)
        self.consumer_task = asyncio.create_task(self._consume_results())

    async def _shutdown_backend(self) -> None:
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

                self._resolve_pending(correlation_id, payload)
                await self.receiver.complete_message(message)


class WorkerTransport(ABC):
    @abstractmethod
    async def run(self, handler: Callable[[Dict[str, Any]], Awaitable[Dict[str, Any]]]) -> None:
        raise NotImplementedError

    @abstractmethod
    async def shutdown(self) -> None:
        raise NotImplementedError

    async def publish_heartbeat(self, worker_name: str) -> None:
        """Publish a heartbeat signal for this worker. No-op by default."""
        pass


class RabbitWorkerTransport(WorkerTransport):
    """
    Worker-side AMQP transport.

    Publishes periodic heartbeat messages to the 'heartbeat' fanout exchange
    so the API can detect whether a worker is warm before submitting jobs.
    The heartbeat is a small JSON payload: {"worker": "<name>"}.
    """

    def __init__(self, broker_url: str, queue_name: str):
        self.broker_url = broker_url
        self.queue_name = queue_name
        self.connection: Optional[aio_pika.RobustConnection] = None
        self.channel: Optional[aio_pika.Channel] = None
        self._heartbeat_exchange: Optional[aio_pika.Exchange] = None

    async def run(self, handler: Callable[[Dict[str, Any]], Awaitable[Dict[str, Any]]]) -> None:
        self.connection = await aio_pika.connect_robust(self.broker_url)
        self.channel = await self.connection.channel()

        exchange = await self.channel.declare_exchange(
            "jobs", aio_pika.ExchangeType.DIRECT, durable=True
        )
        queue = await self.channel.declare_queue(self.queue_name, durable=True)
        await queue.bind(exchange, self.queue_name)

        # Declare the heartbeat fanout exchange so we can publish to it.
        self._heartbeat_exchange = await self.channel.declare_exchange(
            HEARTBEAT_EXCHANGE, aio_pika.ExchangeType.FANOUT, durable=True
        )

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

    async def publish_heartbeat(self, worker_name: str) -> None:
        """Publish a heartbeat to the fanout exchange for this worker."""
        if not self._heartbeat_exchange:
            return
        try:
            payload = json.dumps({"worker": worker_name}).encode()
            message = aio_pika.Message(
                body=payload,
                content_type="application/json",
            )
            await self._heartbeat_exchange.publish(message, routing_key="")
        except Exception:
            pass  # Heartbeat failure is non-fatal

    async def shutdown(self) -> None:
        if self.channel and not self.channel.is_closed:
            await self.channel.close()
        if self.connection and not self.connection.is_closed:
            await self.connection.close()


class ServiceBusWorkerTransport(WorkerTransport):
    def __init__(self, connection_string: str, queue_name: str):
        _require_servicebus_sdk()
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
