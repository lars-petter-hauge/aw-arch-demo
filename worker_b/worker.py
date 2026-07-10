import asyncio
import json
import time
import logging
import os

import aio_pika
import redis.asyncio as aioredis

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BROKER_URL = os.getenv("BROKER_URL", "amqp://guest:guest@rabbitmq:5672/")
RESULT_CACHE_URL = os.getenv("RESULT_CACHE_URL", "redis://redis:6379")
QUEUE_NAME = "jobs.worker_b"
TARGET_DURATION = 25.0  # ~20-30 seconds of CPU burn


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


async def process_message(message: aio_pika.IncomingMessage, r: aioredis.Redis):
    """Process a single job from the queue."""
    async with message.process():
        try:
            job = json.loads(message.body.decode())
            job_id = job["job_id"]
            iteration = job.get("iteration", 0)
            logger.info(f"Worker B processing job {job_id} (iteration {iteration})")

            # Do the CPU-intensive work
            elapsed = cpu_burn(TARGET_DURATION)

            # Store result in Redis
            result = {
                "status": "completed",
                "worker": "B",
                "job_id": job_id,
                "iteration": iteration,
                "cpu_seconds": round(elapsed, 3),
            }
            await r.set(f"result:{job_id}", json.dumps(result))
            logger.info(f"Worker B completed job {job_id} in {elapsed:.2f}s")
            
            # Message is automatically acknowledged after process() context
        except Exception as e:
            logger.error(f"Error processing message: {e}", exc_info=True)
            # message.nack(requeue=True) would be called on exception within process()


async def main():
    logger.info(f"Worker B connecting to {BROKER_URL}")
    
    connection = await aio_pika.connect_robust(BROKER_URL)
    channel = await connection.channel()
    
    # Declare exchange and queue
    exchange = await channel.declare_exchange(
        "jobs", aio_pika.ExchangeType.DIRECT, durable=True
    )
    queue = await channel.declare_queue(QUEUE_NAME, durable=True)
    await queue.bind(exchange, QUEUE_NAME)
    
    # Setup result cache
    r = await aioredis.from_url(RESULT_CACHE_URL, decode_responses=True)
    
    logger.info(f"Worker B listening on {QUEUE_NAME}")
    
    try:
        # Setup consumer with prefetch
        await channel.set_qos(prefetch_count=1)
        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                await process_message(message, r)
    except Exception as e:
        logger.error(f"Worker B error: {e}", exc_info=True)
    finally:
        await r.aclose()
        await connection.close()


if __name__ == "__main__":
    asyncio.run(main())
