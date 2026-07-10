import asyncio
import json
import uuid
import os
import logging
from typing import List, Dict, Any

import aio_pika
from fastapi import FastAPI, BackgroundTasks
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="AW Arch Demo API")

# Configuration
BROKER_URL = os.getenv("BROKER_URL", "amqp://guest:guest@rabbitmq:5672/")

# Default worker timeouts (in seconds) by model name
WORKER_TIMEOUTS = {
    "worker_a": 30.0,
    "worker_b": 120.0,
    "worker_c": 30.0,
}

# Available workers (define once at startup)
AVAILABLE_WORKERS = ["worker_a", "worker_b", "worker_c"]

# In-memory result storage (POC - simple dictionary for demonstration)
results_store: Dict[str, Dict[str, Any]] = {}

# AMQP connection pool
amqp_connection = None
amqp_channel = None
amqp_exchange = None


class PipelineRequest(BaseModel):
    """Request body for pipeline execution."""
    models: List[str] = ["worker_a", "worker_b"]  # Model names in sequence
    simulations: int = 10  # Number of independent simulations


async def get_amqp_connection():
    """Get or create AMQP connection."""
    global amqp_connection
    if amqp_connection is None or amqp_connection.is_closed():
        amqp_connection = await aio_pika.connect_robust(BROKER_URL)
    return amqp_connection


async def get_amqp_channel():
    """Get or create AMQP channel."""
    global amqp_channel
    if amqp_channel is None or amqp_channel.is_closed():
        conn = await get_amqp_connection()
        amqp_channel = await conn.channel()
    return amqp_channel


async def get_amqp_exchange():
    """Get or create AMQP exchange."""
    global amqp_exchange
    if amqp_exchange is None:
        channel = await get_amqp_channel()
        amqp_exchange = await channel.get_exchange("jobs")
    return amqp_exchange


async def setup_amqp_infrastructure():
    """Initialize AMQP exchange and queues once at startup."""
    try:
        logger.info("Setting up AMQP infrastructure...")
        conn = await get_amqp_connection()
        channel = await get_amqp_channel()
        
        # Create exchange
        global amqp_exchange
        amqp_exchange = await channel.declare_exchange(
            "jobs", aio_pika.ExchangeType.DIRECT, durable=True
        )
        logger.info("Exchange 'jobs' created")
        
        # Create queues for all workers
        for worker in AVAILABLE_WORKERS:
            queue_name = f"jobs.{worker}"
            queue = await channel.declare_queue(queue_name, durable=True)
            await queue.bind(amqp_exchange, queue_name)
            logger.info(f"Queue '{queue_name}' created and bound to exchange")
        
        logger.info("AMQP infrastructure setup complete")
    except Exception as e:
        logger.error(f"Error setting up AMQP infrastructure: {e}", exc_info=True)
        raise


@app.on_event("startup")
async def startup_event():
    """Initialize AMQP infrastructure on app startup."""
    await setup_amqp_infrastructure()


@app.on_event("shutdown")
async def shutdown_event():
    """Close AMQP connection on app shutdown."""
    global amqp_connection, amqp_channel
    if amqp_channel:
        await amqp_channel.close()
    if amqp_connection:
        await amqp_connection.close()
    logger.info("AMQP connection closed")


async def publish_job(channel: aio_pika.Channel, model_name: str, job_data: dict):
    """Publish a job to AMQP exchange."""
    queue_name = f"jobs.{model_name}"
    exchange = await get_amqp_exchange()
    message = aio_pika.Message(
        body=json.dumps(job_data).encode(),
        content_type="application/json",
    )
    await exchange.publish(message, routing_key=queue_name)


async def wait_for_result(job_id: str, timeout: float = 120.0):
    """Poll in-memory store for a job result."""
    elapsed = 0.0
    while elapsed < timeout:
        if job_id in results_store:
            return results_store[job_id]
        await asyncio.sleep(0.2)
        elapsed += 0.2
    return {"status": "timeout"}


async def run_simulation_pipeline(
    pipeline_id: str, simulation_id: int, models: List[str], channel, state_key: str
):
    """
    Run a single simulation through the pipeline.
    Each simulation flows through all models sequentially.
    Multiple simulations run concurrently.
    
    As soon as model_1 completes, model_2 starts immediately (with model_1's output as input).
    """
    try:
        simulation_results = {}
        previous_result = None

        for step_index, model in enumerate(models):
            job_id = str(uuid.uuid4())
            
            # Build job data
            job_data = {
                "job_id": job_id,
                "simulation": simulation_id,
                "step": step_index,
                "model": model,
            }
            
            # If there's a previous result from earlier model in the pipeline, pass it as input
            if previous_result is not None:
                job_data["input"] = previous_result

            # Publish job to queue
            await publish_job(channel, model, job_data)
            logger.info(
                f"Pipeline {pipeline_id}: Published simulation {simulation_id} "
                f"to model {model} (step {step_index + 1}/{len(models)})"
            )
            
            # Get timeout for this worker (default to 120s if not specified)
            timeout = WORKER_TIMEOUTS.get(model, 120.0)
            
            # Wait for THIS simulation's result for this model
            result = await wait_for_result(job_id, timeout=timeout)
            simulation_results[model] = result
            previous_result = result
            
            logger.info(
                f"Pipeline {pipeline_id}: Simulation {simulation_id} completed model "
                f"{model} (step {step_index + 1}/{len(models)})"
            )

        # Store results for this simulation
        if state_key not in results_store:
            results_store[state_key] = {}
        results_store[f"{state_key}:results:{simulation_id}"] = simulation_results
        
        logger.info(
            f"Pipeline {pipeline_id}: Simulation {simulation_id} completed all models"
        )

    except Exception as e:
        logger.error(
            f"Error in pipeline {pipeline_id}, simulation {simulation_id}: {e}",
            exc_info=True
        )
        if state_key not in results_store:
            results_store[state_key] = {}
        results_store[f"{state_key}:results:{simulation_id}"] = {
            "status": "error",
            "error": str(e)
        }


async def run_pipeline(
    pipeline_id: str, models: List[str], simulations: int
):
    """
    Orchestrate N independent simulations through M models.
    
    Each simulation flows through the model pipeline sequentially (A -> B -> C).
    Multiple simulations run concurrently - as soon as sim_i finishes model_j,
    it immediately starts model_j+1, while other simulations are also processing.
    
    This creates a pipelined parallel execution:
    SIM 1: model_a [0-1s]      -> model_b [1-26s]
    SIM 2:            model_a [1-2s]  -> model_b [2-27s]
    SIM 3:                       model_a [2-3s] -> model_b [3-28s]
    ...
    Total time ≈ 1s (first model_a) + N×1s (all model_a) + 25s (final model_b)
    """
    try:
        # Get pre-initialized channel (queues already exist)
        channel = await get_amqp_channel()

        # Initialize pipeline state
        state_key = f"pipeline:{pipeline_id}"
        state = {
            "status": "running",
            "models": models,
            "total_simulations": simulations,
            "completed_simulations": 0,
        }
        results_store[state_key] = state
        
        # Create tasks for all simulations to run concurrently
        # Each simulation flows through the model pipeline independently
        logger.info(
            f"Starting pipeline {pipeline_id} with {simulations} simulations "
            f"across {len(models)} models: {models}"
        )
        
        tasks = [
            run_simulation_pipeline(pipeline_id, sim_id, models, channel, state_key)
            for sim_id in range(1, simulations + 1)
        ]
        
        # Run all simulations concurrently
        await asyncio.gather(*tasks)
        
        # Update state to completed
        state["status"] = "completed"
        results_store[state_key] = state
        logger.info(f"Pipeline {pipeline_id} completed: {simulations} simulations")

    except Exception as e:
        logger.error(f"Error in pipeline {pipeline_id}: {e}", exc_info=True)
        state_key = f"pipeline:{pipeline_id}"
        state = {
            "status": "error",
            "error": str(e),
            "models": models,
            "total_simulations": simulations,
        }
        results_store[state_key] = state


@app.post("/pipeline")
async def create_pipeline(
    request: PipelineRequest, background_tasks: BackgroundTasks
):
    """
    Create and start a new pipeline with concurrent simulations.
    
    Each simulation flows through the models sequentially (A -> B -> C -> ...).
    All simulations run concurrently - as soon as one finishes a model step,
    it immediately starts the next model in the pipeline.
    
    Args:
        request: PipelineRequest with models (list of strings) and simulations (int)
    
    Example:
        POST /pipeline
        {
            "models": ["worker_a", "worker_b"],
            "simulations": 10
        }
    
    Response:
        {"pipeline_id": "..."}
    """
    # Validate input
    if not request.models:
        return {"error": "models list cannot be empty"}
    if request.simulations < 1:
        return {"error": "simulations must be at least 1"}

    pipeline_id = str(uuid.uuid4())
    background_tasks.add_task(
        run_pipeline, pipeline_id, request.models, request.simulations
    )
    return {"pipeline_id": pipeline_id}


@app.get("/pipeline/{pipeline_id}")
async def get_pipeline(pipeline_id: str):
    """
    Get the current status of a pipeline.
    
    Example response (running):
        {
            "status": "running",
            "models": ["worker_a", "worker_b"],
            "total_simulations": 10,
            "completed_simulations": 5,
            "results": {
                "simulation_1": {
                    "worker_a": {"status": "completed", "cpu_seconds": 1.005},
                    "worker_b": {"status": "completed", "cpu_seconds": 25.012}
                },
                "simulation_2": {...}
            }
        }
    
    Example response (completed):
        {
            "status": "completed",
            "models": ["worker_a", "worker_b"],
            "total_simulations": 10,
            "completed_simulations": 10,
            "results": {...all simulations...}
        }
    """
    state_key = f"pipeline:{pipeline_id}"
    
    if state_key not in results_store:
        return {"error": "not found"}
    
    state = results_store[state_key].copy()
    
    # Fetch results for all simulations
    results = {}
    for sim_id in range(1, state.get("total_simulations", 0) + 1):
        result_key = f"{state_key}:results:{sim_id}"
        if result_key in results_store:
            results[f"simulation_{sim_id}"] = results_store[result_key]
    
    state["results"] = results
    
    # Count completed simulations
    state["completed_simulations"] = len(results)
    
    return state


# ═════════════════════════════════════════════════════════════════════════════
# ▼ METRICS SECTION - Monitoring endpoints (separated from business logic) ▼
# ═════════════════════════════════════════════════════════════════════════════

async def get_queue_depth(queue_name: str) -> int:
    """Get message count in a specific queue.
    
    Args:
        queue_name: Name of the queue (e.g., 'jobs.worker_a')
    
    Returns:
        Number of messages in the queue
    """
    try:
        channel = await get_amqp_channel()
        queue = await channel.get_queue(queue_name, ensure=False)
        if queue:
            return queue.declaration_result.method.message_count
        return 0
    except Exception as e:
        logger.warning(f"Could not get queue depth for {queue_name}: {e}")
        return 0


@app.get("/metrics")
async def get_metrics():
    """
    Get real-time metrics for monitoring.
    
    Returns metrics about queue depths (jobs waiting) across all workers.
    Used by the monitoring dashboard and terminal monitor.
    
    Response format:
    {
        "timestamp": "2026-07-10T20:45:30.123456",
        "queues": {
            "jobs.worker_a": 5,
            "jobs.worker_b": 3,
            "jobs.worker_c": 8
        },
        "total_jobs_waiting": 16
    }
    """
    try:
        queues = {}
        for worker in AVAILABLE_WORKERS:
            queue_name = f"jobs.{worker}"
            depth = await get_queue_depth(queue_name)
            queues[queue_name] = depth
        
        total_jobs = sum(queues.values())
        
        return {
            "timestamp": __import__("datetime").datetime.utcnow().isoformat(),
            "queues": queues,
            "total_jobs_waiting": total_jobs,
        }
    except Exception as e:
        logger.error(f"Error collecting metrics: {e}")
        return {
            "timestamp": __import__("datetime").datetime.utcnow().isoformat(),
            "error": str(e),
        }


# ═════════════════════════════════════════════════════════════════════════════
# ▲ METRICS SECTION - Monitoring endpoints (separated from business logic) ▲
# ═════════════════════════════════════════════════════════════════════════════


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "ok"}
