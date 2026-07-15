import asyncio
import json
import uuid
import os
import logging
import time
import base64
import urllib.request
import urllib.parse
from datetime import datetime
from typing import List, Dict, Any, Optional

from fastapi import FastAPI, BackgroundTasks
from pydantic import BaseModel

from messaging import ApiTransport, create_api_transport
from messaging.transports import RabbitApiTransport

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

# Default job execution timeouts (in seconds) by model name.
# These cover the expected worker runtime plus a small buffer.
# Used when the worker is already warm (running and connected).
WORKER_TIMEOUTS = {
    "worker_a": 30.0,
    "worker_b": 120.0,
    "worker_c": 30.0,
}

# Cold-start budget: extra time to allow for Kubernetes node provisioning,
# image pull, and container startup before the worker connects to the broker.
# Added on top of the normal job timeout when no heartbeat has been seen.
COLD_START_BUDGET = 90.0  # seconds

# A worker is considered warm if a heartbeat was received within this window.
HEARTBEAT_WARMTH_THRESHOLD = 15  # seconds

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


def is_worker_warm(model: str) -> bool:
    """Return True if a heartbeat has been received from this worker recently.

    A worker is considered warm if its last heartbeat arrived within
    HEARTBEAT_WARMTH_THRESHOLD seconds. If no heartbeat has ever been seen
    (e.g. the worker has never started, or just scaled to zero) this returns
    False.

    For non-RabbitMQ transports (e.g. Azure Service Bus) heartbeats are not
    implemented, so we conservatively assume the worker is warm to avoid
    adding unnecessary latency in production.

    Two-phase timeout logic (used in run_simulation_pipeline):
      - Warm  -> timeout = WORKER_TIMEOUTS[model]
                 (normal job execution budget)
      - Cold  -> timeout = COLD_START_BUDGET + WORKER_TIMEOUTS[model]
                 (extra time for Kubernetes to provision a node + start pod,
                  then the normal job execution budget on top)
    """
    transport = get_transport()

    # Heartbeat tracking is only available on the RabbitMQ transport.
    if not isinstance(transport, RabbitApiTransport):
        return True  # Assume warm for Service Bus / unknown transports

    last_seen: Optional[datetime] = transport.worker_last_seen.get(model)
    if last_seen is None:
        return False  # Never seen — worker has not connected yet

    elapsed = (datetime.utcnow() - last_seen).total_seconds()
    return elapsed <= HEARTBEAT_WARMTH_THRESHOLD


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

    As soon as model_1 completes, model_2 starts immediately (with model_1's
    output as input). Uses RPC-style communication: each job gets a unique
    correlation_id and waits for a result on the reply queue.

    Two-phase timeout:
        Before publishing each job we check whether the target worker has
        sent a heartbeat recently (is_worker_warm). If it has, we use the
        normal job timeout. If not (worker is cold / scaled to zero) we add
        COLD_START_BUDGET on top so the job is not abandoned while Kubernetes
        is provisioning the pod. The pipeline status is set to
        'waiting_for_worker' to give the client visibility during this window.
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

            # If there's a previous result from earlier model in the pipeline,
            # pass it as input.
            if previous_result is not None:
                job_data["input"] = previous_result

            # --- Two-phase timeout ---
            # Check whether the worker is currently warm (has sent a heartbeat
            # within HEARTBEAT_WARMTH_THRESHOLD seconds).
            warm = is_worker_warm(model)
            job_timeout = WORKER_TIMEOUTS.get(model, 120.0)

            if warm:
                # Worker is running — use the normal execution timeout.
                timeout = job_timeout
                logger.info(
                    f"Pipeline {pipeline_id}: Worker '{model}' is warm, "
                    f"using normal timeout {timeout}s"
                )
            else:
                # Worker is cold (scaled to zero or not yet started).
                # Allow COLD_START_BUDGET seconds for Kubernetes to bring up
                # the pod, then the normal job timeout on top.
                timeout = COLD_START_BUDGET + job_timeout
                logger.info(
                    f"Pipeline {pipeline_id}: Worker '{model}' is cold, "
                    f"applying cold-start budget. Total timeout: {timeout}s "
                    f"({COLD_START_BUDGET}s cold-start + {job_timeout}s job)"
                )
                # Signal to polling clients that we are waiting for the worker
                # pod to come up.
                state = results_store.get(state_key, {})
                state["status"] = "waiting_for_worker"
                state["waiting_for"] = model
                results_store[state_key] = state

            # Publish job to queue
            await transport.publish_job(model, job_data, correlation_id)
            logger.info(
                f"Pipeline {pipeline_id}: Published simulation {simulation_id} "
                f"to model {model} (step {step_index + 1}/{len(models)})"
            )

            # Once a job is published, flip status back to running so the
            # client sees progress as soon as the worker picks it up.
            state = results_store.get(state_key, {})
            if state.get("status") == "waiting_for_worker":
                state["status"] = "running"
                state.pop("waiting_for", None)
                results_store[state_key] = state

            # Wait for this simulation's result for this model
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
    Multiple simulations run concurrently - as soon as one finishes a model
    step, it immediately starts the next model in the pipeline, while other
    simulations are also processing.

    Uses RPC-style communication where workers send results back to a reply
    queue, enabling true asynchronous pipelining.
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
    All simulations run concurrently.

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

    Possible status values:
      - "pending"             : pipeline accepted, not yet started
      - "running"             : simulations are in progress
      - "waiting_for_worker"  : a job has been queued but the target worker
                                has not sent a heartbeat recently; the API is
                                waiting for the worker pod to start (cold-start
                                window). The 'waiting_for' field names the model.
      - "completed"           : all simulations finished successfully
      - "error"               : an unhandled exception occurred

    Example response (waiting for cold worker):
        {
            "status": "waiting_for_worker",
            "waiting_for": "worker_b",
            "models": ["worker_a", "worker_b"],
            "total_simulations": 10,
            "completed_simulations": 0,
            "results": {}
        }

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
                }
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
    state["completed_simulations"] = len(results)

    return state


# ════════════════════════════════════════════════════════════════════════════
# ▼ METRICS SECTION - Monitoring endpoints (separated from business logic) ▼
# ════════════════════════════════════════════════════════════════════════════

async def get_queue_depth(queue_name: str) -> int:
    """Get message count in a specific queue."""
    return await get_transport().get_queue_depth(queue_name)


def _fetch_queue_stats_from_management(queue_name: str) -> Dict[str, int]:
    """Fetch queue stats from RabbitMQ management API."""
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
    """Get queue stats including waiting and in-progress counts."""
    return await get_transport().get_queue_runtime_stats(queue_name)


@app.get("/metrics")
async def get_metrics():
    """
    Get real-time metrics for monitoring.

    Returns metrics about queue depths across all workers, plus per-worker
    warmth status derived from heartbeat tracking.

    Response format:
    {
        "timestamp": "...",
        "queues": {"jobs.worker_a": 5, ...},
        "queue_stats": {"jobs.worker_a": {"ready": 5, "unacked": 2, ...}, ...},
        "worker_warmth": {"worker_a": true, "worker_b": false, ...},
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

        # Include per-worker warmth so the dashboard can surface cold workers.
        worker_warmth = {w: is_worker_warm(w) for w in AVAILABLE_WORKERS}

        return {
            "timestamp": __import__("datetime").datetime.utcnow().isoformat(),
            "queues": queues,
            "queue_stats": queue_stats,
            "worker_warmth": worker_warmth,
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


# ════════════════════════════════════════════════════════════════════════════
# ▲ METRICS SECTION - Monitoring endpoints (separated from business logic) ▲
# ════════════════════════════════════════════════════════════════════════════


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "ok"}
