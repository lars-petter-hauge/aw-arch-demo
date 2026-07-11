import logging
from typing import Any, Dict

from base_worker import BaseWorker, WorkerConfig

logging.basicConfig(level=logging.INFO)


class WorkerA(BaseWorker):
    """Fast worker: ~2 seconds average CPU burn with runtime jitter."""

    async def do_work(self, job: Dict[str, Any]) -> Dict[str, Any]:
        """Execute CPU-intensive work with +/-30% randomized runtime."""
        elapsed = self.cpu_burn(self.sample_duration())
        return {
            "status": "completed",
            "worker": "A",
            "job_id": job["job_id"],
            "simulation": job.get("simulation", 0),
            "iteration": job.get("iteration", 0),
            "cpu_seconds": round(elapsed, 3),
        }


if __name__ == "__main__":
    config = WorkerConfig(
        worker_name="A",
        queue_name="jobs.worker_a",
        target_duration=2.0,
    )
    worker = WorkerA(config)
    worker.start()
