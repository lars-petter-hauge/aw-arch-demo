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
                        │  1 replica   │              │  1 replica   │
                        └──────────────┘              └──────────────┘
                        (scales 0-10)                 (scales 0-10)

Results Cache: Redis (in-memory, for fast polling)
```

### Components

| Component | Description |
|-----------|-------------|
| **API** | FastAPI server listening on port 8000. Exposes endpoints to trigger pipeline runs and check status. Does NOT run models itself—all compute is offloaded. |
| **RabbitMQ** (local) / **Azure Service Bus** (prod) | AMQP 0.9.1 (local) or AMQP 1.0 (prod). Jobs published to `jobs` exchange with routing keys `jobs.worker_a` and `jobs.worker_b`. |
| **Redis** | In-memory result cache. Stores job results at `result:{job_id}`. API polls here for completion. |
| **Worker A** | Lightweight compute worker. Consumes from `jobs.worker_a` queue. Burns ~1 second of CPU (single core, 100%). Represents fast operations (e.g., data prep). |
| **Worker B** | Heavy compute worker. Consumes from `jobs.worker_b` queue. Burns ~25 seconds of CPU (single core, 100%). Represents slow operations like NeqSim simulations. |

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

The API orchestrates a **pipeline of 10 sequential iterations**:

```
Iteration 1: Job_A1 → wait → Job_B1 → wait
Iteration 2: Job_A2 → wait → Job_B2 → wait
...
Iteration 10: Job_A10 → wait → Job_B10 → wait
```

Each job is published to the AMQP broker. Workers pick them up, burn CPU, store results in Redis, and acknowledge the message. The API polls Redis for results before moving to the next job.

**Total runtime:** ~10 iterations × (1s + 25s) ≈ 4-5 minutes.

### API Endpoints

#### `POST /pipeline`
Trigger a new pipeline run.

**Response:**
```json
{
  "pipeline_id": "550e8400-e29b-41d4-a716-446655440000"
}
```

#### `GET /pipeline/{pipeline_id}`
Check pipeline status.

**Response (running):**
```json
{
  "status": "running",
  "iteration": 3,
  "step": "B",
  "results": [
    {
      "iteration": 1,
      "a": {"status": "completed", "worker": "A", "cpu_seconds": 1.005},
      "b": {"status": "completed", "worker": "B", "cpu_seconds": 25.012}
    },
    {
      "iteration": 2,
      "a": {"status": "completed", "worker": "A", "cpu_seconds": 1.003},
      "b": {"status": "completed", "worker": "B", "cpu_seconds": 25.015}
    }
  ]
}
```

**Response (completed):**
```json
{
  "status": "completed",
  "iteration": 10,
  "step": null,
  "results": [... all 10 iterations ...]
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
- ~3 minutes to run full pipeline

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
redis_1     | Ready to accept connections
api_1       | INFO:     Uvicorn running on http://0.0.0.0:8000
worker_a_1  | INFO:root:Worker A listening on jobs.worker_a
worker_b_1  | INFO:root:Worker B listening on jobs.worker_b
```

### Using the API

In a new terminal:

```bash
# Trigger a pipeline
PIPELINE_ID=$(curl -s -X POST http://localhost:8000/pipeline | jq -r '.pipeline_id')
echo "Pipeline ID: $PIPELINE_ID"

# Poll for status
curl http://localhost:8000/pipeline/$PIPELINE_ID | jq .

# Keep polling until completed
watch -n 1 "curl -s http://localhost:8000/pipeline/$PIPELINE_ID | jq '.status, .iteration, .step'"
```

### Monitoring with RabbitMQ Management UI

Open `http://localhost:15672` in your browser:
- **Username:** `guest`
- **Password:** `guest`

You'll see:
- **Exchanges:** `jobs` (DIRECT type)
- **Queues:** `jobs.worker_a` and `jobs.worker_b`
- **Messages:** Real-time message flow from API → queue → worker → ack

### Load Testing (Docker Compose)

Create 5 concurrent pipelines:

```bash
for i in {1..5}; do
  curl -s -X POST http://localhost:8000/pipeline &
done
wait

# Watch RabbitMQ UI—you'll see queues fill up and drain
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
curl -X POST $API_URL/pipeline
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

- **Metric:** Queue length (number of messages in `jobs.worker_a` or `jobs.worker_b`)
- **Scaling Rule:** `desired_replicas = queue_length / queueLength_threshold`
- **Cool-down:** After queue drains, workers stay for 1 hour before scaling to 0 (avoids rapid cold starts)
- **Min/Max:** 0 minimum (scale-to-zero), 10 maximum

### Example Scaling Behavior

**Worker A** (fast, ~1s per job):
- If 10 messages in queue → 10/5 = **2 replicas** (process 5 jobs each in ~5s)
- If 50 messages in queue → 50/5 = **10 replicas** (max, process ~50 jobs in ~5s)

**Worker B** (slow, ~25s per job):
- If 5 messages in queue → 5/2 = **3 replicas** (process ~3 jobs in ~50s)
- If 10 messages in queue → 10/2 = **5 replicas** (process ~5 jobs in ~50s)

### Tuning Sensitivity

Edit `k8s/scaler_worker_a.yaml` and `k8s/scaler_worker_b.yaml`:

```yaml
metadata:
  queueLength: "5"  # Lower = scale faster (more aggressive)
                    # Higher = scale slower (more conservative)
```

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
  curl -s http://localhost:8000/pipeline/$PIPELINE_ID | jq '.status, .iteration'
  sleep 5
done
```

### Load Test (stress test auto-scaling)

```bash
# Trigger 20 concurrent pipelines
for i in {1..20}; do
  curl -s -X POST http://localhost:8000/pipeline > /dev/null &
done
wait

# Watch in RabbitMQ UI:
# - Queues fill rapidly
# - If using KEDA: workers scale from 1 → 10
# - Queues drain as workers process

# Check logs
docker-compose logs -f worker_a worker_b
```

### Error Scenarios

**Worker crash:** Kill a worker pod/container while processing. AMQP requeues the message; another worker picks it up.

**Queue buildup:** Trigger many pipelines at once. Watch auto-scaling in action (Kubernetes/Radix only).

**Worker timeout:** Increase `BROKER_URL` timeout or set prefetch=1 to ensure one job per worker.

---

## Project Structure

```
aw-arch-demo/
├── api/
│   ├── Dockerfile
│   ├── main.py              # FastAPI application
│   └── requirements.txt
├── worker_a/
│   ├── Dockerfile
│   ├── worker.py            # AMQP consumer (fast)
│   └── requirements.txt
├── worker_b/
│   ├── Dockerfile
│   ├── worker.py            # AMQP consumer (slow)
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

### API Publishing Jobs

```python
# api/main.py
async def publish_job(channel: aio_pika.Channel, queue_name: str, job_data: dict):
    exchange = await channel.get_exchange("jobs")
    message = aio_pika.Message(body=json.dumps(job_data).encode())
    await exchange.publish(message, routing_key=queue_name)
```

### Worker Consuming Jobs

```python
# worker_a/worker.py
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
| **Pipeline runtime** | ~4-5 minutes (10 × (1s + 25s)) |
| **Worker A job time** | ~1.0s (tight loop math) |
| **Worker B job time** | ~25s (tight loop math) |
| **Message latency** | <100ms (AMQP publish → worker pickup) |
| **Result polling** | 200ms interval |
| **Max concurrent pipelines** | Limited by workers (with auto-scale: unlimited) |
| **Memory per worker** | ~50MB (single Python process) |
| **CPU per worker** | 1 core @ 100% during job processing |

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
