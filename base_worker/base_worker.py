"""Base worker module for AMQP job processing."""
import asyncio
import time
import logging
import os
import random
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional

from messaging import WorkerTransport, create_worker_transport

logger = logging.getLogger(__name__)


class WorkerConfig:
    """Configuration for a worker instance."""

    def __init__(
        self,
        worker_name: str,
        queue_name: str,
        target_duration: float,
        broker_url: Optional[str] = None,
        transport_backend: Optional[str] = None,
    ):
        self.worker_name = worker_name
        self.queue_name = queue_name
        self.target_duration = target_duration
        self.broker_url = broker_url or os.getenv(
            "BROKER_URL", "amqp://guest:guest@rabbitmq:5672/"
        )
        env_backend = os.getenv("TRANSPORT_BACKEND", "").strip().lower()
        chosen_backend = transport_backend or env_backend
        if chosen_backend in {"rabbitmq", "servicebus"}:
            self.transport_backend = chosen_backend
        elif self.broker_url.startswith("Endpoint="):
            self.transport_backend = "servicebus"
        else:
            self.transport_backend = "rabbitmq"


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
        self.transport: WorkerTransport = create_worker_transport(
            self.config.broker_url,
            self.config.queue_name,
            self.config.transport_backend,
        )

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

    async def _handle_job(self, job: Dict[str, Any]) -> Dict[str, Any]:
        """Execute worker-specific logic for a single parsed job payload."""
        job_id = job["job_id"]
        simulation = job.get("simulation", 0)
        iteration = job.get("iteration", 0)

        self.logger.info(
            f"Processing job {job_id} "
            f"(simulation: {simulation}, iteration: {iteration})"
        )

        result = await self.do_work(job)

        self.logger.info(
            f"Completed job {job_id} in {result.get('cpu_seconds', 0):.2f}s"
        )
        return result

    async def run(self) -> None:
        """Main worker loop - connect and process messages."""
        try:
            self.logger.info(
                f"Worker '{self.config.worker_name}' listening on "
                f"'{self.config.queue_name}'"
            )
            await self.transport.run(self._handle_job)
        except Exception as e:
            self.logger.error(f"Worker error: {e}", exc_info=True)
        finally:
            await self.transport.shutdown()
            self.logger.info("Worker shutdown complete")

    def start(self) -> None:
        """Start the worker (blocking)."""
        asyncio.run(self.run())
