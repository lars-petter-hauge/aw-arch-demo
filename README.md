# AW Architecture Demo

Proof-of-concept demonstrating how to decouple a web API from compute-heavy model workers using an async AMQP job queue.

## Problem

In the current setup, the backend both serves the API and runs compute models directly. When a heavy model runs, the API becomes unresponsive. Additionally, different models have different dependencies and resource requirements—it's inefficient to bundle them together.

**This demo shows how to:**
1. Separate the API from worker processes
2. Use AMQP (a proper message broker protocol) instead of Redis LPUSH/BRPOP (which lacks queue semantics)
3. Deploy the same code locally (RabbitMQ) and in production (Azure Service Bus on Radix)

## Architecture

```
┌──────────┐       ┌───────┐       ┌────────────────┐
│  Client  │──────▶│  API  │──────▶│     AMQP       │
└──────────┘       └───────┘       │ RabbitMQ/SvcBus│
                                    └────┬──────────┘
                              ┌─────────┼─────────┐
                              ▼                   ▼
                        ┌──────────┐        ┌──────────┐
                        │ Worker A │        │ Worker B │
                        │  (~1s)   │        │ (~20-30s)│
                        └──────────┘        └──────────┘

Results: Redis Cache (for quick polling)
```

### Components

| Component | Description |
|-----------|----------|
| **API** | FastAPI server. Exposes endpoints to trigger runs and check status. Does not run any models itself. |
| **RabbitMQ** (local) / **Azure Service Bus** (prod) | AMQP message broker. Jobs published to `jobs` exchange with routing keys `jobs.worker_a` and `jobs.worker_b`. |
| **Redis** | Result cache. API polls here for job results via `result:{job_id}`. |
| **Worker A** | Lightweight model. Consumes from `jobs.worker_a` queue. Burns ~1 second of CPU (single core, 100%). |
| **Worker B** | Heavy model. Consumes from `jobs.worker_b` queue. Burns ~20-30 seconds of CPU (single core, 100%). |

### Why AMQP?

**AMQP Benefits over Redis LPUSH/BRPOP:**
- ✅ **Message acknowledgment**: Jobs are only removed from queue after successful processing (no lost jobs)
- ✅ **Automatic requeue**: Failed jobs can be retried without manual intervention
- ✅ **Dead-letter queues**: Poison messages don't get stuck—they're sent to a dead-letter queue
- ✅ **Radix/KEDA native support**: Radix recognizes Azure Service Bus as a scale trigger; no custom KEDA config needed
- ✅ **Same code, different broker**: Use RabbitMQ locally, Azure Service Bus in production—just change the connection string

### Workflow

The API exposes an endpoint that triggers a **pipeline of 10 sequential runs**:

For each of the 10 iterations:
1. **Model A** runs first (~1s CPU)
2. Once Model A completes, **Model B** runs (~20-30s CPU)

So the full sequence is: A → B → A → B → ... (10 pairs).

The API remains responsive throughout — all compute is offloaded to workers via AMQP.

### Endpoints

```
POST /pipeline
```

Returns a pipeline ID. The client can poll for status:

```
GET /pipeline/{pipeline_id}
```

Returns the current state: which iteration we're on, whether each step is pending/running/completed, and final results.

```
GET /health
```

Health check endpoint.

## Running locally (Docker Compose)

```bash
docker-compose up --build
```

The API is available at `http://localhost:8000`.

**Check RabbitMQ Management UI:**
- URL: `http://localhost:15672`
- Username: `guest`
- Password: `guest`

You can see queues being created and messages flowing through in real-time.

## Running locally (Kubernetes)

Requires [minikube](https://minikube.sigs.k8s.io/) or [kind](https://kind.sigs.k8s.io/).

```bash
# Build images
docker build -t aw-arch-demo-api ./api
docker build -t aw-arch-demo-worker-a ./worker_a
docker build -t aw-arch-demo-worker-b ./worker_b

# Load images into minikube
minikube image load aw-arch-demo-api:latest
minikube image load aw-arch-demo-worker-a:latest
minikube image load aw-arch-demo-worker-b:latest

# Deploy
kubectl apply -f k8s/

# Get the API URL
minikube service api --url
```

The API is exposed on NodePort 30080.

### Auto-scaling with KEDA

[KEDA](https://keda.sh/) scales workers based on AMQP queue length — when jobs pile up, more worker pods are created automatically.

**Install KEDA:**

```bash
helm repo add kedacore https://kedacore.github.io/charts
helm repo update
helm install keda kedacore/keda --namespace keda --create-namespace
```

**How it works:**

- Workers scale from **0 to 10 replicas** based on queue depth (scale-to-zero when idle)
- KEDA polls RabbitMQ every 5 seconds
- After the queue drains, workers stay alive for **1 hour** (`cooldownPeriod: 3600`) before scaling back to 0 — this avoids repeated cold starts during bursty workloads

**Tuning `queueLength` (scaling sensitivity):**

The `queueLength` parameter controls how aggressively KEDA scales. It represents the number of queued items per replica — KEDA calculates desired replicas as `queueLength / queueLength_trigger`.

| Worker | Job duration | Jobs/5s/replica | queueLength | Effect |
|--------|-------------|-----------------|------------|--------|
| Worker A | ~1s | ~5 | `5` | Only scales up when backlog exceeds what one replica handles in a poll cycle |
| Worker B | ~25s | ~0.2 | `2` | Scales up quickly since each replica is slow |

**Examples:**
- 10 jobs in `jobs.worker_a` → 10/5 = 2 replicas (each processes ~5 jobs in 5s)
- 100 jobs in `jobs.worker_a` → 100/5 = 20 replicas (but capped at maxReplicaCount)
- 10 jobs in `jobs.worker_b` → 10/2 = 5 replicas (each takes ~25s, so 5 replicas finish in ~50s)

The scalers are configured in `k8s/scaler_worker_a.yaml` and `k8s/scaler_worker_b.yaml` and applied with `kubectl apply -f k8s/`.

## Deployment (Radix)

Deployed to Radix with each component as a separate container, independently scalable. The same AMQP logic is configured via `horizontalScaling` in `radixconfig.yaml` — Radix runs KEDA natively with **Azure Service Bus as the scale trigger** (no Redis, no custom polling).

**Key differences from local setup:**

1. **Message Broker**: Azure Service Bus (managed) replaces RabbitMQ
2. **Connection String**: Uses `BROKER_URL=amqps://...` (TLS-secured AMQP 1.0)
3. **Scale Trigger**: `azure-servicebus` (Radix native) instead of custom RabbitMQ trigger
4. **Result Cache**: Redis can be managed service or remain in-cluster

**Example radixconfig.yaml snippet:**

```yaml
components:
  - name: worker_a
    horizontalScaling:
      maxReplicas: 10
      minReplicas: 0
      triggers:
        - type: azure-servicebus
          metadata:
            queueName: jobs.worker_a
            queueLength: "5"
          authenticationRef: servicebus
```

## Testing

### Local (Docker Compose)

```bash
docker-compose up --build

# In another terminal
curl -X POST http://localhost:8000/pipeline
# Returns: {"pipeline_id": "abc123..."}

# Poll for results
curl http://localhost:8000/pipeline/abc123...
```

### Load Testing

Create 10 concurrent pipelines:

```bash
for i in {1..10}; do
  curl -X POST http://localhost:8000/pipeline &
done
wait
```

Watch the RabbitMQ Management UI to see queue depth and message flow.

### Local (Kubernetes)

```bash
# After deploying with kubectl apply -f k8s/
kubectl get pods -w
# Watch pods scale up as jobs queue

API_URL=$(minikube service api --url)
curl -X POST $API_URL/pipeline
```

## Architecture Comparison

### Before (Redis LPUSH/BRPOP)

```
Pros:
- Simple implementation
- Built-in client libraries

Cons:
- No message acknowledgment → jobs lost if worker crashes
- No built-in retry/dead-letter mechanism
- KEDA poll frequency: 5s (slow)
- Redis is a cache, not a queue → no durability guarantees
```

### After (AMQP)

```
Pros:
- Message ack/nack with automatic requeue
- Dead-letter exchanges for failed jobs
- Radix native Service Bus trigger (fast scale)
- Works locally (RabbitMQ) and production (Service Bus) with identical code
- Proper queue durability and persistence

Cons:
- Slightly more complex to setup (but worth it)
```

## References

- [aio-pika Documentation](https://aio-pika.readthedocs.io/)
- [RabbitMQ Documentation](https://www.rabbitmq.com/documentation.html)
- [Azure Service Bus Documentation](https://learn.microsoft.com/en-us/azure/service-bus-messaging/)
- [Radix Platform Documentation](https://radix.equinor.com/)
- [KEDA Documentation](https://keda.sh/)
