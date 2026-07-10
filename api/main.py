import asyncio
import json
import uuid
import os
import logging

import aio_pika
import redis.asyncio as aioredis
from fastapi import FastAPI, BackgroundTasks

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="AW Arch Demo API")

# Configuration
BROKER_URL = os.getenv("BROKER_URL", "amqp://guest:guest@rabbitmq:5672/")
RESULT_CACHE_URL = os.getenv("RESULT_CACHE_URL", "redis://redis:6379")
NUM_ITERATIONS = 10

# AMQP connection pool
amqp_connection = None


async def get_amqp_connection():
    """Get or create AMQP connection."""
    global amqp_connection
    if amqp_connection is None or amqp_connection.is_closed():
        amqp_connection = await aio_pika.connect_robust(BROKER_URL)
    return amqp_connection


async def get_redis():
    """Get Redis connection for result caching."""
    return aioredis.from_url(RESULT_CACHE_URL, decode_responses=True)


async def publish_job(channel: aio_pika.Channel, queue_name: str, job_data: dict):
    """Publish a job to AMQP exchange."""
    exchange = await channel.get_exchange(f"jobs")
    message = aio_pika.Message(
        body=json.dumps(job_data).encode(),
        content_type="application/json",
    )
    await exchange.publish(message, routing_key=queue_name)


async def wait_for_result(r, job_id: str, timeout: float = 120.0):
    """Poll Redis for a job result."""
    elapsed = 0.0
    while elapsed < timeout:
        result = await r.get(f"result:{job_id}")
        if result:
            return json.loads(result)
        await asyncio.sleep(0.2)
        elapsed += 0.2
    return {"status": "timeout"}


async def run_pipeline(pipeline_id: str):
    """Orchestrate 10 iterations of A -> B."""
    try:
        # Setup connections
        conn = await get_amqp_connection()
        channel = await conn.channel()
        r = await get_redis()

        # Declare exchange and queues
        exchange = await channel.declare_exchange(
            "jobs", aio_pika.ExchangeType.DIRECT, durable=True
        )
        queue_a = await channel.declare_queue("jobs.worker_a", durable=True)
        queue_b = await channel.declare_queue("jobs.worker_b", durable=True)
        await queue_a.bind(exchange, "jobs.worker_a")
        await queue_b.bind(exchange, "jobs.worker_b")

        state = {"status": "running", "iteration": 0, "step": None, "results": []}
        await r.set(f"pipeline:{pipeline_id}", json.dumps(state))

        for i in range(NUM_ITERATIONS):
            # Step A
            job_a_id = str(uuid.uuid4())
            state["iteration"] = i + 1
            state["step"] = "A"
            await r.set(f"pipeline:{pipeline_id}", json.dumps(state))
            
            job_a_data = {"job_id": job_a_id, "iteration": i + 1}
            await publish_job(channel, "jobs.worker_a", job_a_data)
            logger.info(f"Published job {job_a_id} to worker_a")
            result_a = await wait_for_result(r, job_a_id, timeout=30.0)

            # Step B
            job_b_id = str(uuid.uuid4())
            state["step"] = "B"
            await r.set(f"pipeline:{pipeline_id}", json.dumps(state))
            
            job_b_data = {
                "job_id": job_b_id,
                "iteration": i + 1,
                "input": result_a,
            }
            await publish_job(channel, "jobs.worker_b", job_b_data)
            logger.info(f"Published job {job_b_id} to worker_b")
            result_b = await wait_for_result(r, job_b_id, timeout=120.0)

            state["results"].append(
                {"iteration": i + 1, "a": result_a, "b": result_b}
            )
            await r.set(f"pipeline:{pipeline_id}", json.dumps(state))

        state["status"] = "completed"
        state["step"] = None
        await r.set(f"pipeline:{pipeline_id}", json.dumps(state))
        logger.info(f"Pipeline {pipeline_id} completed")

    except Exception as e:
        logger.error(f"Error in pipeline {pipeline_id}: {e}", exc_info=True)
        if r:
            state["status"] = "error"
            state["error"] = str(e)
            await r.set(f"pipeline:{pipeline_id}", json.dumps(state))
    finally:
        await r.aclose()


@app.post("/pipeline")
async def create_pipeline(background_tasks: BackgroundTasks):
    pipeline_id = str(uuid.uuid4())
    background_tasks.add_task(run_pipeline, pipeline_id)
    return {"pipeline_id": pipeline_id}


@app.get("/pipeline/{pipeline_id}")
async def get_pipeline(pipeline_id: str):
    r = await get_redis()
    data = await r.get(f"pipeline:{pipeline_id}")
    await r.aclose()
    if not data:
        return {"error": "not found"}
    return json.loads(data)


@app.get("/health")
async def health_check():
    return {"status": "ok"}
