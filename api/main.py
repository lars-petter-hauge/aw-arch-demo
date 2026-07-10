import asyncio
import json
import uuid
import os
import logging
from typing import List

import aio_pika
import redis.asyncio as aioredis
from fastapi import FastAPI, BackgroundTasks
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="AW Arch Demo API")

# Configuration
BROKER_URL = os.getenv("BROKER_URL", "amqp://guest:guest@rabbitmq:5672/")
RESULT_CACHE_URL = os.getenv("RESULT_CACHE_URL", "redis://redis:6379")

# Default worker timeouts (in seconds) by model name
WORKER_TIMEOUTS = {
    "worker_a": 30.0,
    "worker_b": 120.0,
}

# AMQP connection pool
amqp_connection = None


class PipelineRequest(BaseModel):
    """Request body for pipeline execution."""
    models: List[str] = ["worker_a", "worker_b"]  # Model names in sequence
    iterations: int = 10  # Number of iterations


async def get_amqp_connection():
    """Get or create AMQP connection."""
    global amqp_connection
    if amqp_connection is None or amqp_connection.is_closed():
        amqp_connection = await aio_pika.connect_robust(BROKER_URL)
    return amqp_connection


async def get_redis():
    """Get Redis connection for result caching."""
    return aioredis.from_url(RESULT_CACHE_URL, decode_responses=True)


async def publish_job(channel: aio_pika.Channel, model_name: str, job_data: dict):
    """Publish a job to AMQP exchange."""
    queue_name = f"jobs.{model_name}"
    exchange = await channel.get_exchange("jobs")
    message = aio_pika.Message(
        body=json.dumps(job_data).encode(),
        content_type="application/json",
    )
    await exchange.publish(message, routing_key=queue_name)
    logger.info(f"Published job {job_data['job_id']} to {queue_name}")


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


async def run_pipeline(
    pipeline_id: str, models: List[str], iterations: int
):
    """
    Orchestrate a generic pipeline with N iterations across M models.
    
    For each iteration, models run sequentially (A → B → C → ...).
    Results from each model feed into the next one.
    """
    r = None
    try:
        # Setup connections
        conn = await get_amqp_connection()
        channel = await conn.channel()
        r = await get_redis()

        # Declare exchange and all queues
        exchange = await channel.declare_exchange(
            "jobs", aio_pika.ExchangeType.DIRECT, durable=True
        )
        
        for model in models:
            queue_name = f"jobs.{model}"
            queue = await channel.declare_queue(queue_name, durable=True)
            await queue.bind(exchange, queue_name)

        # Initialize pipeline state
        state = {
            "status": "running",
            "iteration": 0,
            "current_step_index": 0,
            "models": models,
            "total_iterations": iterations,
            "results": [],
        }
        await r.set(f"pipeline:{pipeline_id}", json.dumps(state))

        # Main loop: iterate N times
        for i in range(iterations):
            iteration_results = {}
            previous_result = None

            # For each iteration, run all models sequentially
            for step_index, model in enumerate(models):
                job_id = str(uuid.uuid4())
                
                # Update state to show current progress
                state["iteration"] = i + 1
                state["current_step_index"] = step_index
                state["current_model"] = model
                await r.set(f"pipeline:{pipeline_id}", json.dumps(state))

                # Build job data
                job_data = {
                    "job_id": job_id,
                    "iteration": i + 1,
                    "step": step_index,
                    "model": model,
                }
                
                # If there's a previous result, pass it as input
                if previous_result is not None:
                    job_data["input"] = previous_result

                # Publish job
                await publish_job(channel, model, job_data)
                
                # Get timeout for this worker (default to 120s if not specified)
                timeout = WORKER_TIMEOUTS.get(model, 120.0)
                
                # Wait for result
                result = await wait_for_result(r, job_id, timeout=timeout)
                iteration_results[model] = result
                previous_result = result
                
                logger.info(
                    f"Pipeline {pipeline_id}: Iteration {i+1}/{iterations}, "
                    f"Model {model} (step {step_index+1}/{len(models)}) completed"
                )

            # Store complete iteration results
            state["results"].append(
                {"iteration": i + 1, "models": iteration_results}
            )
            await r.set(f"pipeline:{pipeline_id}", json.dumps(state))

        # Mark as completed
        state["status"] = "completed"
        state["current_step_index"] = None
        state["current_model"] = None
        await r.set(f"pipeline:{pipeline_id}", json.dumps(state))
        logger.info(
            f"Pipeline {pipeline_id} completed: {iterations} iterations across "
            f"{len(models)} models"
        )

    except Exception as e:
        logger.error(f"Error in pipeline {pipeline_id}: {e}", exc_info=True)
        if r:
            state["status"] = "error"
            state["error"] = str(e)
            await r.set(f"pipeline:{pipeline_id}", json.dumps(state))
    finally:
        if r:
            await r.aclose()


@app.post("/pipeline")
async def create_pipeline(
    request: PipelineRequest, background_tasks: BackgroundTasks
):
    """
    Create and start a new pipeline.
    
    Args:
        request: PipelineRequest with models (list of strings) and iterations (int)
    
    Example:
        POST /pipeline
        {
            "models": ["worker_a", "worker_b"],
            "iterations": 10
        }
    
    Returns:
        {"pipeline_id": "..."}
    """
    # Validate input
    if not request.models:
        return {"error": "models list cannot be empty"}
    if request.iterations < 1:
        return {"error": "iterations must be at least 1"}

    pipeline_id = str(uuid.uuid4())
    background_tasks.add_task(
        run_pipeline, pipeline_id, request.models, request.iterations
    )
    return {"pipeline_id": pipeline_id}


@app.get("/pipeline/{pipeline_id}")
async def get_pipeline(pipeline_id: str):
    """
    Get the current status of a pipeline.
    
    Example response:
        {
            "status": "running",
            "iteration": 3,
            "current_step_index": 0,
            "current_model": "worker_a",
            "models": ["worker_a", "worker_b"],
            "total_iterations": 10,
            "results": [
                {
                    "iteration": 1,
                    "models": {
                        "worker_a": {"status": "completed", "cpu_seconds": 1.005},
                        "worker_b": {"status": "completed", "cpu_seconds": 25.012}
                    }
                },
                ...
            ]
        }
    """
    r = await get_redis()
    data = await r.get(f"pipeline:{pipeline_id}")
    await r.aclose()
    if not data:
        return {"error": "not found"}
    return json.loads(data)


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "ok"}
