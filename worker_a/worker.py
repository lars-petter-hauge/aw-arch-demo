import json
import time
import redis

REDIS_URL = "redis://redis:6379"
QUEUE = "queue:worker_a"
TARGET_DURATION = 1.0  # ~1 second of CPU burn


def cpu_burn(duration: float) -> float:
    """Burn CPU for approximately `duration` seconds doing real computation."""
    start = time.perf_counter()
    x = 1.0001
    while time.perf_counter() - start < duration:
        # Tight math loop
        for _ in range(10000):
            x = x * 1.0001
            x = x / 1.0001
            x = x + 0.0001
            x = x - 0.0001
    elapsed = time.perf_counter() - start
    return elapsed


def main():
    r = redis.from_url(REDIS_URL, decode_responses=True)
    print(f"Worker A listening on {QUEUE}")

    while True:
        _, message = r.brpop(QUEUE)
        job = json.loads(message)
        job_id = job["job_id"]
        print(f"Worker A processing job {job_id}")

        elapsed = cpu_burn(TARGET_DURATION)

        result = {
            "status": "completed",
            "worker": "A",
            "job_id": job_id,
            "cpu_seconds": round(elapsed, 3),
        }
        r.set(f"result:{job_id}", json.dumps(result))
        print(f"Worker A completed job {job_id} in {elapsed:.2f}s")


if __name__ == "__main__":
    main()
