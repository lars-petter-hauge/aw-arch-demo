# AW Architecture Demo

Proof-of-concept demonstrating how to decouple a web API from compute-heavy model workers using an async AMQP job queue. This mirrors the architecture that [AcidWatch](https://github.com/equinor/acidwatch) could use for offloading expensive chemical reaction simulations (NeqSim) from the API layer.

## Problem

In monolithic architectures, the backend both serves the API and runs compute models directly. When a heavy model runs (e.g., 20-30s chemical simulation), the API becomes unresponsive. Additionally:
- Different models have different dependencies (heavyweight physics libraries, large memory footprint)
- It's inefficient to bundle them together
- The API should stay responsive while work happens in the background
- There's no built-in retry or durability mechanism if workers crash

**This demo shows how to:**
1. ✅ Separate the API from worker processes
2. ✅ Use AMQP (a proper message broker protocol) instead of Redis LPUSH/BRPOP
3. ✅ Deploy identical code to both local (RabbitMQ) and production (Azure Service Bus on Radix)
4. ✅ Auto-scale workers based on queue depth using KEDA
5. ✅ Make pipelines **generic** - support any number of models in any order without code changes

## Architecture

```
┌──────────┐       ┌───────┐       ┌────────────────────────────┐
│  Client  │──────▶│  API  │──────▶│        AMQP Broker         │
└──────────┘       └───────┘       │ RabbitMQ (local) or        │
                                    │ Azure Service Bus (Radix)  │
                                    └────┬───────────────┬────────┘
                              ┌─────────┘               └─────────┐
                              ▼                                   ▼
                        ┌──────────────┐              ┌──────────────┐
                        │  Worker A    │              │  Worker B    │
                        │  (~1s CPU)   │              │  (~25s CPU)  │
                        │  1+ replicas │              │  1+ replicas │
                        └──────────────┘              └──────────────┘
                        (scales 0-10)                 (scales 0-10)

Results Cache: Redis (in-memory, for fast polling)
```

### Components

| Component | Description |
|-----------|-------------|
| **API** | FastAPI server listening on port 8000. Accepts generic pipeline requests with model list and iteration count. Does NOT run models itself—all compute is offloaded. |
| **RabbitMQ** (local) / **Azure Service Bus** (prod) | AMQP 0.9.1 (local) or AMQP 1.0 (prod). Jobs published to `jobs` exchange with routing keys `jobs.{model_name}`. |
| **Redis** | In-memory result cache. Stores job results at `result:{job_id}`. API polls here for completion. |
| **Workers** (generic) | N workers consuming from `jobs.{model_name}` queues. Each worker processes jobs, burns CPU, stores results, and acknowledges completion. Add new workers by adding their names to the pipeline request. |

### Why AMQP over Redis LPUSH/BRPOP?

**AMQP Benefits:**
- ✅ **Message acknowledgment**: Jobs stay in queue until explicitly acknowledged. If a worker crashes mid-job, the message is requeued automatically.
- ✅ **Automatic retry**: Failed messages can be nack'd with requeue flag. Dead-letter exchanges handle poison messages.
- ✅ **Production-grade**: Built for reliable message delivery, not as a side effect of a cache.
- ✅ **Native Radix support**: Azure Service Bus is a Radix scaling trigger. KEDA scales based on queue depth without polling.
- ✅ **Same code everywhere**: Use `aio-pika` client library locally and in production. Only the connection string changes.

**Redis LPUSH/BRPOP Limitations:**
- ❌ No acknowledgment—jobs lost if worker crashes after BRPOP but before completion
- ❌ No retry mechanism—manual requeue required
- ❌ KEDA polling adds latency (5s check interval)
- ❌ Redis is a cache, not a queue—no persistence guarantees

### Workflow

The API orchestrates a **generic pipeline of N iterations across M models**:

```
Iteration 1: Model_1 → Model_2 → ... → Model_M → store results
Iteration 2: Model_1 → Model_2 → ... → Model_M → store results
...
Iteration N: Model_1 → Model_2 → ... → Model_M → store results
```

Within each iteration:
- Models run **sequentially** (A finishes, B starts)
- Results **chain**: Model A output becomes Model B input
- No hardcoding—define models at request time

**Example: 10 iterations with [worker_a, worker_b]**
- Total runtime: ~10 × (1s + 25s) ≈ 4-5 minutes

**Example: 5 iterations with [worker_a, worker_b, worker_c]**
- Total runtime: ~5 × (1s + 25s + worker_c_time) ≈ varies

### API Endpoints

#### `POST /pipeline`
Trigger a new pipeline with custom models and iteration count.

**Request Body:**
```json
{
  "models": ["worker_a", "worker_b"],
  "iterations": 10
}
```

**Response:**
```json
{
  "pipeline_id": "550e8400-e29b-41d4-a716-446655440000"
}
```

**Notes:**
- `models`: List of worker names (required, non-empty)
- `iterations`: Number of iterations (required, ≥1)
- Defaults: `models=["worker_a", "worker_b"]`, `iterations=10`

#### `GET /pipeline/{pipeline_id}`
Check pipeline status and retrieve results.

**Response (running):**
```json
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
        "worker_a": {
          "status": "completed",
          "worker": "a",
          "job_id": "...",
          "iteration": 1,
          "cpu_seconds": 1.005
        },
        "worker_b": {
          "status": "completed",
          "worker": "b",
          "job_id": "...",
          "iteration": 1,
          "cpu_seconds": 25.012
        }
      }
    },
    {
      "iteration": 2,
      "models": { ... }
    }
  ]
}
```

**Response (completed):**
```json
{
  "status": "completed",
  "iteration": 10,
  "current_step_index": null,
  "current_model": null,
  "models": ["worker_a", "worker_b"],
  "total_iterations": 10,
  "results": [... all 10 iterations ...]
}
```

**Response (not found):**
```json
{
  "error": "not found"
}
```

#### `GET /health`
Health check.

**Response:**
```json
{
  "status": "ok"
}
```

---

## Running Locally (Docker Compose)

### Prerequisites
- Docker & Docker Compose installed
- ~3-5 minutes to run full pipeline (depends on model count and iterations)

### Quick Start

```bash
# Clone and navigate
git clone https://github.com/lars-petter-hauge/aw-arch-demo.git
cd aw-arch-demo
git checkout feature/async-worker-architecture

# Start all services
docker-compose up --build
```

### What You'll See

```
rabbitmq_1  | Starting RabbitMQ 3.12.11 on Erlang 25.3.2.4
api_1       | INFO:     Uvicorn running on http://0.0.0.0:8000
dashboard_1 | You can now view your Streamlit app in your browser.
worker_a_1  | INFO:root:Worker A listening on jobs.worker_a
worker_b_1  | INFO:root:Worker B listening on jobs.worker_b
```

Dashboard URL: `http://localhost:8501`

### Using the API

In a new terminal:

#### Run default pipeline (2 models, 10 iterations):
```bash
PIPELINE_ID=$(curl -s -X POST http://localhost:8000/pipeline | jq -r '.pipeline_id')
echo "Pipeline ID: $PIPELINE_ID"

# Poll for status
curl http://localhost:8000/pipeline/$PIPELINE_ID | jq .

# Keep polling until completed
watch -n 1 "curl -s http://localhost:8000/pipeline/$PIPELINE_ID | jq '.status, .iteration, .current_model'"
```

#### Run custom pipeline (2 models, 5 iterations):
```bash
PIPELINE_ID=$(curl -s -X POST http://localhost:8000/pipeline \
  -H "Content-Type: application/json" \
  -d '{
    "models": ["worker_a", "worker_b"],
    "iterations": 5
  }' | jq -r '.pipeline_id')

curl http://localhost:8000/pipeline/$PIPELINE_ID | jq .
```

#### Run pipeline with 3+ models:
```bash
# First, ensure worker_c, worker_d, etc. are available
# (add them to docker-compose.yml or build separate containers)

PIPELINE_ID=$(curl -s -X POST http://localhost:8000/pipeline \
  -H "Content-Type: application/json" \
  -d '{
    "models": ["worker_a", "worker_b", "worker_c"],
    "iterations": 3
  }' | jq -r '.pipeline_id')

curl http://localhost:8000/pipeline/$PIPELINE_ID | jq .
```

#### Run single-model pipeline:
```bash
PIPELINE_ID=$(curl -s -X POST http://localhost:8000/pipeline \
  -H "Content-Type: application/json" \
  -d '{
    "models": ["worker_a"],
    "iterations": 20
  }' | jq -r '.pipeline_id')

curl http://localhost:8000/pipeline/$PIPELINE_ID | jq .
```

### Monitoring with RabbitMQ Management UI

Open `http://localhost:15672` in your browser:
- **Username:** `guest`
- **Password:** `guest`

You'll see:
- **Exchanges:** `jobs` (DIRECT type)
- **Queues:** `jobs.worker_a`, `jobs.worker_b`, `jobs.worker_c`, etc.
- **Messages:** Real-time message flow from API → queue → worker → ack

Watch as models are dynamically added based on your request!

### Monitoring Dashboard (Mobile Friendly)

Open `http://localhost:8501` in your browser (or phone on the same network).

The dashboard includes:
- **Quick Launch buttons** for one-tap pipeline starts
- **Advanced Configuration** to choose workers and number of simulations
- **Pipeline tracking** with progress and results
- **Queue metrics** charts and totals
- **Compact mode** toggle in the sidebar for phone-first workflow

### Load Testing (Docker Compose)

Create 5 concurrent pipelines:

```bash
for i in {1..5}; do
  curl -s -X POST http://localhost:8000/pipeline \
    -H "Content-Type: application/json" \
    -d '{
      "models": ["worker_a", "worker_b"],
      "iterations": 5
    }' &
done
wait

# Watch RabbitMQ UI—you'll see queues fill up and drain
```

Mix and match model counts:

```bash
# Trigger varying pipelines
curl -s -X POST http://localhost:8000/pipeline -H "Content-Type: application/json" -d '{"models": ["worker_a"], "iterations": 20}' &
curl -s -X POST http://localhost:8000/pipeline -H "Content-Type: application/json" -d '{"models": ["worker_a", "worker_b"], "iterations": 10}' &
curl -s -X POST http://localhost:8000/pipeline -H "Content-Type: application/json" -d '{"models": ["worker_a", "worker_b", "worker_c"], "iterations": 5}' &
wait
```

### Docker Compose Environment Variables

Edit `docker-compose.yml` to change:

```yaml
environment:
  - BROKER_URL=amqp://guest:guest@rabbitmq:5672/  # AMQP connection
  - RESULT_CACHE_URL=redis://redis:6379            # Redis cache
```

---

## Running Locally (Kubernetes)

### Prerequisites
- Minikube or Kind cluster
- kubectl
- ~5 minutes for initial setup

### Setup

```bash
# Clone repo
git clone https://github.com/lars-petter-hauge/aw-arch-demo.git
cd aw-arch-demo
git checkout feature/async-worker-architecture

# Build images
docker build -t aw-arch-demo-api ./api
docker build -t aw-arch-demo-worker-a ./worker_a
docker build -t aw-arch-demo-worker-b ./worker_b

# Load into minikube
minikube image load aw-arch-demo-api:latest
minikube image load aw-arch-demo-worker-a:latest
minikube image load aw-arch-demo-worker-b:latest

# Apply manifests
kubectl apply -f k8s/

# Verify deployment
kubectl get pods
kubectl logs -f deployment/api
```

### Accessing the API

```bash
# Get the service URL
API_URL=$(minikube service api --url)
echo $API_URL

# Trigger pipeline
curl -X POST $API_URL/pipeline \
  -H "Content-Type: application/json" \
  -d '{
    "models": ["worker_a", "worker_b"],
    "iterations": 5
  }'
```

### Manual Scaling

```bash
# Scale worker_a to 3 replicas
kubectl scale deployment worker-a --replicas=3

# Watch scaling
kubectl get pods -w
```

---

## Auto-Scaling with KEDA

[KEDA](https://keda.sh/) automatically scales workers based on AMQP queue depth. This requires a Kubernetes cluster.

### Install KEDA

```bash
helm repo add kedacore https://kedacore.github.io/charts
helm repo update
helm install keda kedacore/keda --namespace keda --create-namespace
```

### How It Works

- **Metric:** Queue length for each worker (number of messages in `jobs.{model_name}`)
- **Scaling Rule:** `desired_replicas = queue_length / queueLength_threshold`
- **Cool-down:** After queue drains, workers stay for 1 hour before scaling to 0 (avoids rapid cold starts)
- **Min/Max:** 0 minimum (scale-to-zero), 10 maximum per worker

### Example Scaling Behavior

**Worker A** (fast, ~1s per job):
- If 10 messages in queue → 10/5 = **2 replicas** (process 5 jobs each in ~5s)
- If 50 messages in queue → 50/5 = **10 replicas** (max, process ~50 jobs in ~5s)

**Worker B** (slow, ~25s per job):
- If 5 messages in queue → 5/2 = **3 replicas** (process ~3 jobs in ~50s)
- If 10 messages in queue → 10/2 = **5 replicas** (process ~5 jobs in ~50s)

**Worker C** (custom duration, e.g., ~5s):
- If 20 messages in queue → 20/4 = **5 replicas** (process ~4 jobs each in ~20s)

### Tuning Sensitivity

Edit `k8s/scaler_worker_a.yaml`, `k8s/scaler_worker_b.yaml`, etc.:

```yaml
metadata:
  queueLength: "5"  # Lower = scale faster (more aggressive)
                    # Higher = scale slower (more conservative)
```

### Adding New Worker Scalers

For each new worker (e.g., `worker_c`), create `k8s/scaler_worker_c.yaml`:

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: scaler-worker-c
spec:
  scaleTargetRef:
    name: worker-c
  minReplicaCount: 0
  maxReplicaCount: 10
  cooldownPeriod: 3600
  triggers:
    - type: rabbitmq
      metadata:
        queueName: jobs.worker_c
        queueLength: "4"
        connectionFromSecret: rabbitmq-creds
```

Then apply: `kubectl apply -f k8s/scaler_worker_c.yaml`

---

## Deployment to Radix

Radix is Equinor's container orchestration platform. It integrates with Azure Service Bus and KEDA natively.

### Key Differences from Local Setup

| Aspect | Local | Radix |
|--------|-------|-------|
| **Message Broker** | RabbitMQ (self-managed) | Azure Service Bus (managed) |
| **AMQP URL** | `amqp://guest:guest@rabbitmq:5672/` | `amqps://...` (TLS, from secret) |
| **Scaling Trigger** | KEDA + RabbitMQ scaler | KEDA + native Azure Service Bus trigger |
| **Result Cache** | Redis (in-cluster) | Redis (managed or in-cluster) |

### Radix Configuration (radixconfig.yaml)

```yaml
apiVersion: radix.equinor.com/v1
kind: RadixApplication
metadata:
  name: aw-arch-demo
spec:
  environments:
    - name: dev
    - name: prod

  components:
    - name: api
      image: aw-arch-demo-api
      ports:
        - name: http
          port: 8000
      environmentConfig:
        - environment: dev
          variables:
            BROKER_URL: amqps://...  # From secret
            RESULT_CACHE_URL: redis://redis:6379

    - name: worker-a
      image: aw-arch-demo-worker-a
      environmentConfig:
        - environment: dev
          variables:
            BROKER_URL: amqps://...  # From secret
      replicas: 1
      horizontalScaling:
        maxReplicas: 10
        minReplicas: 0
        triggers:
          - type: azure-servicebus
            metadata:
              queueName: jobs.worker_a
              queueLength: "5"
            authenticationRef: servicebus

    - name: worker-b
      image: aw-arch-demo-worker-b
      environmentConfig:
        - environment: dev
          variables:
            BROKER_URL: amqps://...  # From secret
      replicas: 1
      horizontalScaling:
        maxReplicas: 10
        minReplicas: 0
        triggers:
          - type: azure-servicebus
            metadata:
              queueName: jobs.worker_b
              queueLength: "2"
            authenticationRef: servicebus

    - name: redis
      image: redis:7-alpine
      ports:
        - name: tcp
          port: 6379
```

### Deploy to Radix

```bash
# Commit changes to feature/async-worker-architecture
git push origin feature/async-worker-architecture

# Create pull request on GitHub
# Radix will auto-deploy to dev environment on PR

# After merge to main, Radix deploys to prod
```

---

## Testing & Validation

### Health Check

```bash
curl http://localhost:8000/health
# {"status": "ok"}
```

### Single Pipeline Run

```bash
# Start Docker Compose
docker-compose up --build

# Trigger pipeline
PIPELINE_ID=$(curl -s -X POST http://localhost:8000/pipeline | jq -r '.pipeline_id')

# Poll status every 5s
for i in {1..60}; do
  curl -s http://localhost:8000/pipeline/$PIPELINE_ID | jq '.status, .iteration, .current_model'
  sleep 5
done
```

### Test Custom Model Counts

```bash
# Test 1-model pipeline
curl -s -X POST http://localhost:8000/pipeline \
  -H "Content-Type: application/json" \
  -d '{"models": ["worker_a"], "iterations": 5}' | jq .

# Test 3-model pipeline (requires worker_c setup)
curl -s -X POST http://localhost:8000/pipeline \
  -H "Content-Type: application/json" \
  -d '{"models": ["worker_a", "worker_b", "worker_c"], "iterations": 2}' | jq .

# Test different order
curl -s -X POST http://localhost:8000/pipeline \
  -H "Content-Type: application/json" \
  -d '{"models": ["worker_b", "worker_a"], "iterations": 3}' | jq .
```

### Load Test (stress test auto-scaling)

```bash
# Trigger 20 concurrent pipelines with varying model counts
for i in {1..5}; do
  curl -s -X POST http://localhost:8000/pipeline -H "Content-Type: application/json" -d '{"models": ["worker_a"], "iterations": 20}' &
  curl -s -X POST http://localhost:8000/pipeline -H "Content-Type: application/json" -d '{"models": ["worker_a", "worker_b"], "iterations": 10}' &
  curl -s -X POST http://localhost:8000/pipeline -H "Content-Type: application/json" -d '{"models": ["worker_a", "worker_b", "worker_c"], "iterations": 5}' &
done
wait

# Watch in RabbitMQ UI:
# - Multiple queues: jobs.worker_a, jobs.worker_b, jobs.worker_c
# - Queues fill based on pipeline requests
# - If using KEDA: workers scale from 1 → 10 per queue
# - Queues drain as workers process

# Check logs
docker-compose logs -f worker_a worker_b
```

### Error Scenarios

**Worker crash:** Kill a worker pod/container while processing. AMQP requeues the message; another worker picks it up.

**Queue buildup:** Trigger many pipelines at once. Watch auto-scaling in action (Kubernetes/Radix only).

**Invalid model name:** API publishes to non-existent queue. Results in timeout; job stays in queue.

---

## Project Structure

```
aw-arch-demo/
├── api/
│   ├── Dockerfile
│   ├── main.py              # FastAPI application (generic pipeline)
│   └── requirements.txt
├── worker_a/
│   ├── Dockerfile
│   ├── worker.py            # AMQP consumer (fast, ~1s)
│   └── requirements.txt
├── worker_b/
│   ├── Dockerfile
│   ├── worker.py            # AMQP consumer (slow, ~25s)
│   └── requirements.txt
├── k8s/                      # Kubernetes manifests
│   ├── api.yaml
│   ├── worker_a.yaml
│   ├── worker_b.yaml
│   ├── rabbitmq.yaml
│   ├── redis.yaml
│   ├── scaler_worker_a.yaml  # KEDA scaler
│   └── scaler_worker_b.yaml  # KEDA scaler
├── docker-compose.yml
├── radixconfig.yaml          # Radix deployment config
└── README.md                 # This file
```

---

## Code Highlights

### API - Generic Pipeline Request

```python
# api/main.py
class PipelineRequest(BaseModel):
    models: List[str] = ["worker_a", "worker_b"]
    iterations: int = 10

@app.post("/pipeline")
async def create_pipeline(request: PipelineRequest, background_tasks: BackgroundTasks):
    # Validates input, enqueues background task
    background_tasks.add_task(run_pipeline, pipeline_id, request.models, request.iterations)
    return {"pipeline_id": pipeline_id}
```

### API - Generic Pipeline Execution

```python
# api/main.py
async def run_pipeline(pipeline_id: str, models: List[str], iterations: int):
    """Orchestrate N iterations across M models."""
    for i in range(iterations):
        for step_index, model in enumerate(models):
            job_id = str(uuid.uuid4())
            job_data = {"job_id": job_id, "iteration": i+1, "step": step_index, "model": model}
            if previous_result:
                job_data["input"] = previous_result
            await publish_job(channel, model, job_data)
            result = await wait_for_result(r, job_id, timeout=WORKER_TIMEOUTS[model])
            iteration_results[model] = result
            previous_result = result
```

### Worker - Generic AMQP Consumer

```python
# worker_a/worker.py (or any worker_n/worker.py)
async for message in queue_iter:
    async with message.process():  # Auto-ack on success, nack on exception
        job = json.loads(message.body.decode())
        result = do_work(job)
        await cache.set(f"result:{job['job_id']}", json.dumps(result))
```

### Environment Variables

All services respect these env vars:

```bash
BROKER_URL=amqp://guest:guest@rabbitmq:5672/  # Local
BROKER_URL=amqps://...servicebus.windows.net/ # Radix

RESULT_CACHE_URL=redis://redis:6379           # Redis connection
```

---

## Performance Characteristics

| Metric | Value |
|--------|-------|
| **Pipeline runtime** | ~(N iterations) × Σ(model times) |
| **Worker A job time** | ~1.0s |
| **Worker B job time** | ~25s |
| **Message latency** | <100ms (AMQP publish → worker pickup) |
| **Result polling** | 200ms interval |
| **Max concurrent pipelines** | Limited by workers (with auto-scale: unlimited) |
| **Memory per worker** | ~50MB (single Python process) |
| **CPU per worker** | 1 core @ 100% during job processing |

**Example runtimes:**
- 10 iterations × [worker_a (1s) + worker_b (25s)] = 4-5 minutes
- 5 iterations × [worker_a (1s) + worker_b (25s) + worker_c (5s)] = 3-4 minutes
- 20 iterations × [worker_a (1s)] = 20-25 seconds

---

## Troubleshooting

### Workers not picking up jobs

```bash
# Check RabbitMQ logs
docker-compose logs rabbitmq

# Check queue status
docker-compose exec rabbitmq rabbitmqctl list_queues

# Ensure BROKER_URL is correct
docker-compose exec api printenv BROKER_URL
```

### Pipeline stuck at "running"

```bash
# Check worker logs
docker-compose logs worker_a worker_b

# Check Redis cache
docker-compose exec redis redis-cli
> KEYS "result:*"
> GET "result:<job_id>"
```

### High memory usage

- Workers are single-threaded and shouldn't use >100MB
- Check for memory leaks in business logic (not applicable here)
- Reduce worker count or add resource limits

### Auto-scaling not working

- Ensure KEDA is installed: `kubectl get deployment -n keda`
- Check KEDA logs: `kubectl logs -n keda deployment/keda-operator`
- Verify trigger config in `scaler_worker_*.yaml`

### Invalid model in pipeline request

- API publishes to `jobs.{model_name}` regardless
- If queue doesn't exist: AMQP auto-creates it (RabbitMQ behavior)
- If no worker listens: message waits in queue indefinitely
- Results in timeout: `{"status": "timeout"}` returned to API

---

## References

- **[aio-pika Documentation](https://aio-pika.readthedocs.io/)** – Async AMQP client for Python
- **[RabbitMQ Documentation](https://www.rabbitmq.com/documentation.html)** – AMQP message broker (local)
- **[Azure Service Bus Documentation](https://learn.microsoft.com/en-us/azure/service-bus-messaging/)** – Managed AMQP broker (prod)
- **[Radix Platform Documentation](https://radix.equinor.com/)** – Container orchestration platform
- **[KEDA Documentation](https://keda.sh/)** – Kubernetes-based Event Driven Autoscaling
- **[AcidWatch Repository](https://github.com/equinor/acidwatch)** – Real application this demo patterns

---

## License

MIT License
