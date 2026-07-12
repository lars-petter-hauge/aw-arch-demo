import asyncio
import json
import uuid
import os
import logging
import time
import base64
import urllib.request
import urllib.parse
from typing import List, Dict, Any, Optional

from fastapi import FastAPI, BackgroundTasks
from pydantic import BaseModel

from messaging import ApiTransport, create_api_transport

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="AW Arch Demo API")

# Configuration
BROKER_URL = os.getenv("BROKER_URL", "amqp://guest:guest@rabbitmq:5672/")
TRANSPORT_BACKEND = os.getenv("TRANSPORT_BACKEND", "").strip().lower()
RESULTS_QUEUE = os.getenv("RESULTS_QUEUE", "jobs.results")
RABBITMQ_MGMT_URL = os.getenv("RABBITMQ_MGMT_URL", "http://rabbitmq:15672")
RABBITMQ_MGMT_USER = os.getenv("RABBITMQ_MGMT_USER", "guest")
RABBITMQ_MGMT_PASS = os.getenv("RABBITMQ_MGMT_PASS", "guest")

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

class PipelineRequest(BaseModel):
    """Request body for pipeline execution."""
    models: List[str] = ["worker_a", "worker_b"]  # Model names in sequence
    simulations: int = 10  # Number of independent simulations


api_transport: Optional[ApiTransport] = None


def get_transport() -> ApiTransport:
    global api_transport
    if api_transport is None:
        api_transport = create_api_transport(
            BROKER_URL,
            RESULTS_QUEUE,
            transport_backend=TRANSPORT_BACKEND,
            queue_stats_provider=_fetch_queue_stats_from_management,
        )
    return api_transport


@app.on_event("startup")
async def startup_event():
    """Initialize messaging infrastructure on app startup."""
    await get_transport().startup(AVAILABLE_WORKERS)


@app.on_event("shutdown")
async def shutdown_event():
    """Close messaging connection on app shutdown."""
    transport = get_transport()
    await transport.shutdown()
    logger.info("Messaging transport closed")


async def run_simulation_pipeline(
    pipeline_id: str,
    simulation_id: int,
    models: List[str],
    state_key: str,
):
    """
    Run a single simulation through the pipeline.
    Each simulation flows through all models sequentially.
    Multiple simulations run concurrently.
    
    As soon as model_1 completes, model_2 starts immediately (with model_1's output as input).
    Uses RPC-style communication: each job gets a unique correlation_id and waits for
    a result on the reply queue.
    """
    try:
        simulation_results = {}
        previous_result = None

        transport = get_transport()

        for step_index, model in enumerate(models):
            job_id = str(uuid.uuid4())
            correlation_id = str(uuid.uuid4())
            
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

            # Publish job to queue with RPC metadata
            await transport.publish_job(model, job_data, correlation_id)
            logger.info(
                f"Pipeline {pipeline_id}: Published simulation {simulation_id} "
                f"to model {model} (step {step_index + 1}/{len(models)})"
            )
            
            # Get timeout for this worker (default to 120s if not specified)
            timeout = WORKER_TIMEOUTS.get(model, 120.0)
            
            # Wait for THIS simulation's result for this model
            result = await transport.wait_for_result(correlation_id, timeout=timeout)
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
    
    Uses RPC-style communication where workers send results back to a reply queue,
    enabling true asynchronous pipelining.
    """
    try:
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
            run_simulation_pipeline(
                pipeline_id, sim_id, models, state_key
            )
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
    state_key = f"pipeline:{pipeline_id}"
    results_store[state_key] = {
        "status": "pending",
        "models": request.models,
        "total_simulations": request.simulations,
        "completed_simulations": 0,
    }
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
        return {"status": "not_found", "error": "Pipeline not found"}
    
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
    return await get_transport().get_queue_depth(queue_name)


def _fetch_queue_stats_from_management(queue_name: str) -> Dict[str, int]:
    """Fetch queue stats from RabbitMQ management API.

    Returns a dictionary with ready and unacked message counts.
    """
    vhost = urllib.parse.quote("/", safe="")
    encoded_queue_name = urllib.parse.quote(queue_name, safe="")
    url = f"{RABBITMQ_MGMT_URL}/api/queues/{vhost}/{encoded_queue_name}"

    credentials = f"{RABBITMQ_MGMT_USER}:{RABBITMQ_MGMT_PASS}".encode("utf-8")
    auth_header = base64.b64encode(credentials).decode("utf-8")

    request = urllib.request.Request(url)
    request.add_header("Authorization", f"Basic {auth_header}")

    with urllib.request.urlopen(request, timeout=2.0) as response:
        payload = json.loads(response.read().decode("utf-8"))

    ready = int(payload.get("messages_ready", payload.get("messages", 0)))
    unacked = int(payload.get("messages_unacknowledged", 0))
    consumers = int(payload.get("consumers", 0))
    message_stats = payload.get("message_stats", {})
    completed = int(message_stats.get("ack", 0))

    return {
        "ready": ready,
        "unacked": unacked,
        "total": ready + unacked,
        "consumers": consumers,
        "completed": completed,
    }


async def get_queue_runtime_stats(queue_name: str) -> Dict[str, int]:
    """Get queue stats including waiting and in-progress counts.

    Falls back to AMQP queue depth if management API is unavailable.
    """
    return await get_transport().get_queue_runtime_stats(queue_name)


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
        "queue_stats": {
            "jobs.worker_a": {"ready": 5, "unacked": 2, "total": 7, "consumers": 1, "completed": 120},
            "jobs.worker_b": {"ready": 3, "unacked": 1, "total": 4, "consumers": 1, "completed": 98},
            "jobs.worker_c": {"ready": 8, "unacked": 0, "total": 8, "consumers": 1, "completed": 45}
        },
        "total_jobs_waiting": 16,
        "total_jobs_processing": 3,
        "total_jobs_in_system": 19
    }
    """
    try:
        queues = {}
        queue_stats = {}
        for worker in AVAILABLE_WORKERS:
            queue_name = f"jobs.{worker}"
            stats = await get_queue_runtime_stats(queue_name)
            queue_stats[queue_name] = stats
            queues[queue_name] = stats["ready"]
        
        total_jobs_waiting = sum(queues.values())
        total_jobs_processing = sum(
            stats["unacked"] for stats in queue_stats.values()
        )
        
        return {
            "timestamp": __import__("datetime").datetime.utcnow().isoformat(),
            "queues": queues,
            "queue_stats": queue_stats,
            "total_jobs_waiting": total_jobs_waiting,
            "total_jobs_processing": total_jobs_processing,
            "total_jobs_in_system": total_jobs_waiting + total_jobs_processing,
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
