import asyncio
import json
import uuid

import redis.asyncio as aioredis
from fastapi import FastAPI, BackgroundTasks

app = FastAPI(title="AW Arch Demo API")

REDIS_URL = "redis://redis:6379"
NUM_ITERATIONS = 10


def get_redis():
    return aioredis.from_url(REDIS_URL, decode_responses=True)


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
    r = get_redis()
    state = {"status": "running", "iteration": 0, "step": None, "results": []}
    await r.set(f"pipeline:{pipeline_id}", json.dumps(state))

    for i in range(NUM_ITERATIONS):
        # Step A
        job_a_id = str(uuid.uuid4())
        state["iteration"] = i + 1
        state["step"] = "A"
        await r.set(f"pipeline:{pipeline_id}", json.dumps(state))
        await r.lpush("queue:worker_a", json.dumps({"job_id": job_a_id, "iteration": i + 1}))
        result_a = await wait_for_result(r, job_a_id, timeout=30.0)

        # Step B
        job_b_id = str(uuid.uuid4())
        state["step"] = "B"
        await r.set(f"pipeline:{pipeline_id}", json.dumps(state))
        await r.lpush("queue:worker_b", json.dumps({"job_id": job_b_id, "iteration": i + 1, "input": result_a}))
        result_b = await wait_for_result(r, job_b_id, timeout=120.0)

        state["results"].append({"iteration": i + 1, "a": result_a, "b": result_b})
        await r.set(f"pipeline:{pipeline_id}", json.dumps(state))

    state["status"] = "completed"
    state["step"] = None
    await r.set(f"pipeline:{pipeline_id}", json.dumps(state))
    await r.aclose()


@app.post("/pipeline")
async def create_pipeline(background_tasks: BackgroundTasks):
    pipeline_id = str(uuid.uuid4())
    background_tasks.add_task(run_pipeline, pipeline_id)
    return {"pipeline_id": pipeline_id}


@app.get("/pipeline/{pipeline_id}")
async def get_pipeline(pipeline_id: str):
    r = get_redis()
    data = await r.get(f"pipeline:{pipeline_id}")
    await r.aclose()
    if not data:
        return {"error": "not found"}
    return json.loads(data)
