# Architecture Review - aw-arch-demo (branch: K8s)

Date: 2026-07-12
Scope: Architecture robustness review for Acidwatch-style backend on Radix, with focus on simplicity, scale-to-zero, and exposure boundaries.
Constraint: No code modifications; review only.

## Executive Summary

The branch demonstrates a good direction by separating API from compute workers and using queue-based asynchronous processing.

The primary architecture risk is **configuration and platform drift**:
- Runtime code and local/k8s manifests are RabbitMQ-centric AMQP.
- Radix config is Redis-trigger based.
- Documentation claims RabbitMQ local and Azure Service Bus production with no code changes, but current code uses RabbitMQ-specific semantics.

For a simplicity-first production path on Radix with scale-to-zero, the strongest conclusion is:
- Do not move to Redis lists as the long-term job queue merely for simplicity.
- Choose one queue architecture and align all layers (runtime, Radix config, docs, local dev) to it.
- Given Radix + KEDA and your stated goals, **Azure Service Bus as managed production queue/scaler source is simpler operationally than self-managed RabbitMQ**.

## Findings (Ordered by Severity)

### 1) Critical - Claimed RabbitMQ to Azure Service Bus portability is not true in current implementation

**Evidence**
- `README.md` claims identical code local vs prod and treats RabbitMQ + Azure Service Bus as interchangeable.
  - `README.md:16`
  - `README.md:45`
- API declares and binds RabbitMQ exchange/queue entities and uses RabbitMQ management API:
  - `api/main.py:87` (declare exchange)
  - `api/main.py:95` (declare queue)
  - `api/main.py:96` (bind queue to exchange)
  - `api/main.py:455` (RabbitMQ management API path)
- Worker base also relies on exchange/queue declare-bind:
  - `base_worker/base_worker.py:177`
  - `base_worker/base_worker.py:180`
  - `base_worker/base_worker.py:183`

**Why this matters**
Azure Service Bus (AMQP 1.0) has different entity semantics from RabbitMQ (AMQP 0-9-1). The current code depends on RabbitMQ broker features, so this is not a drop-in connection-string swap.

### 2) Critical - Scale-to-zero goal is contradicted by current KEDA manifest settings

**Evidence**
- All scalers define `minReplicaCount: 1`:
  - `k8s/keda-scalers.yaml:14`
  - `k8s/keda-scalers.yaml:37`
  - `k8s/keda-scalers.yaml:60`

**Why this matters**
Workers never scale to zero in the tested k8s setup, so idle behavior and cost profile for scale-to-zero are not being validated.

### 3) High - Radix config and runtime architecture are inconsistent (Redis vs AMQP)

**Evidence**
- Radix uses Redis trigger and Redis env vars:
  - `radixconfig.yaml:28`
  - `radixconfig.yaml:42`
  - `radixconfig.yaml:18`
- Runtime stack uses AMQP broker URL and RabbitMQ manifests:
  - `api/main.py:22`
  - `docker-compose.yml:23`
  - `k8s/workers.yaml:22`

**Why this matters**
This is the largest simplicity and robustness gap. Two architectures are present simultaneously, increasing deployment ambiguity and operational errors.

### 4) High - Per-simulation exclusive reply queue introduces avoidable complexity

**Evidence**
- API creates one exclusive reply queue per simulation with `auto_delete=False`:
  - `api/main.py:208`
  - `api/main.py:209`

**Why this matters**
With higher simulation concurrency, this can cause queue churn and broker metadata overhead. For a simplicity-oriented architecture, this pattern is heavier than necessary.

### 5) High - Missing worker-name validation can silently lose jobs

**Evidence**
- Validation only checks non-empty model list:
  - `api/main.py:362`
- Publish uses routing key constructed from model name:
  - `api/main.py:138`
  - `api/main.py:146`

**Why this matters**
If a model name has no matching queue binding, jobs may be accepted by API but never processed, creating a reliability and observability issue.

### 6) Medium - Pipeline state is in-memory only

**Evidence**
- In-memory dictionary for pipeline and results:
  - `api/main.py:38`

**Why this matters**
API restart loses pipeline state/results; multiple API replicas can yield inconsistent reads unless state is externalized.

### 7) Medium - Broker security and HA posture is PoC-only

**Evidence**
- Single-replica RabbitMQ and default guest credentials:
  - `k8s/rabbitmq.yaml:6`
  - `k8s/rabbitmq.yaml:24`
  - `k8s/rabbitmq.yaml:27`
- Similar defaults in compose:
  - `docker-compose.yml:7`
  - `docker-compose.yml:8`

**Why this matters**
Acceptable for PoC, but not robust for production reliability/security without hardening and persistent storage strategy.

### 8) Medium - Exposure boundary is mostly correct but differs by environment

**Evidence**
- Radix marks API as public:
  - `radixconfig.yaml:16`
- k8s local exposes API as NodePort:
  - `k8s/api.yaml:37`
- Compose also exposes dashboard publicly on host:
  - `docker-compose.yml:29`

**Why this matters**
The intended policy (only API public) is mostly preserved for Radix config, but local environments differ and should be clearly documented as intentional.

## Simplicity and Robustness Verdict

### What is good
- Clear API/worker decoupling.
- Queue-based offloading avoids API thread starvation during heavy compute.
- KEDA-driven autoscaling direction is appropriate for workload bursts.

### What is fragile today
- Architectural duality (Redis-trigger Radix config vs RabbitMQ runtime code).
- RabbitMQ-specific implementation details conflict with “same code” claim for Azure Service Bus.
- Scale-to-zero not actually validated in current k8s manifests.

## Specific Conclusion on RabbitMQ vs Redis for this PoC

1. RabbitMQ was a reasonable choice for the PoC to demonstrate durable queue semantics, acknowledgments, and autoscaling trigger behavior.
2. Redis lists are simpler to start with, but weaker as a long-term durable job-queue foundation for this use case.
3. For Radix production with simplicity + scale-to-zero as core requirements, managed queueing (Azure Service Bus) is operationally simpler than running RabbitMQ yourself.
4. Therefore: **RabbitMQ is not a poor choice conceptually for PoC**, but **the current mixed architecture is too complex**. Simplest robust path is to standardize end-to-end on the same queue architecture across runtime, manifests, and docs.

## Assumptions and Review Boundaries

- This review assumes target platform is Radix with KEDA available and Azure Service Bus triggers supported.
- This is a PoC architecture review; no code changes were made.
- Live Radix operator behavior was not executed in this repository environment during this review.
