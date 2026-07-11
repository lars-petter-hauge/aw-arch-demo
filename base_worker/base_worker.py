"""Base worker module for AMQP job processing."""
import asyncio
import json
import time
import logging
import os
import random
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any

import aio_pika

logger = logging.getLogger(__name__)


class WorkerConfig:
    """Configuration for a worker instance."""

    def __init__(
        self,
        worker_name: str,
        queue_name: str,
        target_duration: float,
        broker_url: Optional[str] = None,
    ):
        self.worker_name = worker_name
        self.queue_name = queue_name
        self.target_duration = target_duration
        self.broker_url = broker_url or os.getenv(
            "BROKER_URL", "amqp://guest:guest@rabbitmq:5672/"
        )


class BaseWorker(ABC):
    """Abstract base class for AMQP workers.

    Subclasses must implement do_work() to define worker-specific computation.
    All common logic (AMQP connection, message processing) is handled here.
    
    Note: Result storage is handled by the API, not the worker.
    Workers only need to process jobs and return results in the message ack/nack.
    """

    def __init__(self, config: WorkerConfig):
        """Initialize worker with configuration.

        Args:
            config: WorkerConfig instance with worker settings.
        """
        self.config = config
        self.logger = logging.getLogger(f"worker.{config.worker_name}")
        self.connection: Optional[aio_pika.RobustConnection] = None
        self.channel: Optional[aio_pika.Channel] = None

    @staticmethod
    def cpu_burn(duration: float) -> float:
        """Burn CPU for approximately `duration` seconds doing real computation.

        Args:
            duration: Target duration in seconds.

        Returns:
            Actual elapsed time.
        """
        start = time.perf_counter()
        x = 1.0001
        while time.perf_counter() - start < duration:
            # Tight math loop - real computation
            for _ in range(10000):
                x = x * 1.0001
                x = x / 1.0001
                x = x + 0.0001
                x = x - 0.0001
        elapsed = time.perf_counter() - start
        return elapsed

    def sample_duration(self) -> float:
        """Sample a randomized runtime around target duration.

        Each job runs between 70% and 130% of the configured target.
        """
        return self.config.target_duration * random.uniform(0.7, 1.3)

    @abstractmethod
    async def do_work(self, job: Dict[str, Any]) -> Dict[str, Any]:
        """Perform worker-specific computation.

        Subclasses override this to implement custom logic.

        Args:
            job: Job data from message.

        Returns:
            Result dictionary to return in message response.
        """
        pass

    async def process_message(
        self, message: aio_pika.IncomingMessage
    ) -> None:
        """Process a single job from the queue.

        Args:
            message: AMQP message containing job data.
        """
        async with message.process():
            try:
                job = json.loads(message.body.decode())
                job_id = job["job_id"]
                simulation = job.get("simulation", 0)
                iteration = job.get("iteration", 0)

                self.logger.info(
                    f"Processing job {job_id} "
                    f"(simulation: {simulation}, iteration: {iteration})"
                )

                # Perform work
                result = await self.do_work(job)

                self.logger.info(
                    f"Completed job {job_id} in {result.get('cpu_seconds', 0):.2f}s"
                )

                # Message is automatically acknowledged after process() context
            except Exception as e:
                self.logger.error(f"Error processing message: {e}", exc_info=True)
                # message.nack(requeue=True) called on exception within process()

    async def setup_amqp(self) -> None:
        """Setup AMQP connection, exchange, and queue."""
        self.logger.info(f"Connecting to {self.config.broker_url}")
        self.connection = await aio_pika.connect_robust(self.config.broker_url)
        self.channel = await self.connection.channel()

        # Declare exchange and queue
        exchange = await self.channel.declare_exchange(
            "jobs", aio_pika.ExchangeType.DIRECT, durable=True
        )
        queue = await self.channel.declare_queue(
            self.config.queue_name, durable=True
        )
        await queue.bind(exchange, self.config.queue_name)

        self.logger.info(f"Queue '{self.config.queue_name}' ready")

    async def run(self) -> None:
        """Main worker loop - connect and process messages."""
        try:
            await self.setup_amqp()

            self.logger.info(
                f"Worker '{self.config.worker_name}' listening on "
                f"'{self.config.queue_name}'"
            )

            # Setup consumer with prefetch
            await self.channel.set_qos(prefetch_count=1)
            queue = await self.channel.get_queue(self.config.queue_name)
            async with queue.iterator() as queue_iter:
                async for message in queue_iter:
                    await self.process_message(message)
        except Exception as e:
            self.logger.error(f"Worker error: {e}", exc_info=True)
        finally:
            if self.connection:
                await self.connection.close()
            self.logger.info("Worker shutdown complete")

    def start(self) -> None:
        """Start the worker (blocking)."""
        asyncio.run(self.run())
