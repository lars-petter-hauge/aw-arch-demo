#!/bin/bash
set -e

echo "🚀 Setting up Kubernetes environment with KIND and KEDA..."
echo ""

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
CLUSTER_NAME="aw-arch-demo"
REGISTRY_NAME="${CLUSTER_NAME}-registry"
REGISTRY_PORT="5001"
NAMESPACE="default"

# ═════════════════════════════════════════════════════════════════════════════
# SETUP LOGGING - Track all setup operations
# ═════════════════════════════════════════════════════════════════════════════
LOG_DIR="/tmp/aw-arch-setup"
mkdir -p "${LOG_DIR}"
LOG_FILE="${LOG_DIR}/setup-$(date +%s).log"
echo "Setup log: ${LOG_FILE}"

log_step() {
  local step=$1
  local message=$2
  local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
  echo "[${timestamp}] [STEP-${step}] ${message}" | tee -a "${LOG_FILE}"
}

log_success() {
  local message=$1
  local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
  echo "[${timestamp}] [SUCCESS] ${message}" | tee -a "${LOG_FILE}"
}

log_info() {
  local message=$1
  local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
  echo "[${timestamp}] [INFO] ${message}" | tee -a "${LOG_FILE}"
}

log_error() {
  local message=$1
  local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
  echo "[${timestamp}] [ERROR] ${message}" | tee -a "${LOG_FILE}" >&2
}
# ═════════════════════════════════════════════════════════════════════════════

echo ""
log_step "0" "Starting AW Arch Demo K8s setup with KEDA autoscaling"
echo ""

# Step 1: Check if KIND cluster already exists
echo -e "${BLUE}Step 1: Checking KIND cluster...${NC}"
log_step "1" "Checking KIND cluster status"
if kind get clusters | grep -q "^${CLUSTER_NAME}$"; then
  echo -e "${GREEN}✅ KIND cluster '${CLUSTER_NAME}' already exists${NC}"
  log_success "KIND cluster '${CLUSTER_NAME}' already exists"
else
  log_info "Creating KIND cluster '${CLUSTER_NAME}'"
  echo "Creating KIND cluster '${CLUSTER_NAME}'..."
  kind create cluster --name "${CLUSTER_NAME}" --wait 5m 2>&1 | tee -a "${LOG_FILE}"
  echo -e "${GREEN}✅ KIND cluster created${NC}"
  log_success "KIND cluster '${CLUSTER_NAME}' created successfully"
fi

echo ""

# Step 2: Create local Docker registry (for image caching)
echo -e "${BLUE}Step 2: Setting up local Docker registry...${NC}"
log_step "2" "Setting up Docker registry for image caching"
if [ "$(docker ps -aq -f name=${REGISTRY_NAME})" ]; then
  echo -e "${GREEN}✅ Docker registry '${REGISTRY_NAME}' already running${NC}"
  log_success "Docker registry '${REGISTRY_NAME}' already running"
else
  log_info "Starting Docker registry on port ${REGISTRY_PORT}"
  echo "Starting Docker registry..."
  docker run -d --restart=always -p "127.0.0.1:${REGISTRY_PORT}:5000" \
    --name "${REGISTRY_NAME}" \
    registry:2 2>&1 | tee -a "${LOG_FILE}"
  echo -e "${GREEN}✅ Docker registry started on localhost:${REGISTRY_PORT}${NC}"
  log_success "Docker registry started on localhost:${REGISTRY_PORT}"
fi

echo ""

# Step 3: Connect registry to KIND cluster
echo -e "${BLUE}Step 3: Connecting registry to KIND cluster...${NC}"
log_step "3" "Connecting Docker registry to KIND network"
REGISTRY_IP=$(docker inspect -f '{{.NetworkSettings.IPAddress}}' "${REGISTRY_NAME}")
if [ -z "$REGISTRY_IP" ]; then
  log_info "Connecting registry to kind network"
  docker network connect kind "${REGISTRY_NAME}" 2>&1 | tee -a "${LOG_FILE}" || true
  REGISTRY_IP=$(docker inspect -f '{{.NetworkSettings.Networks.kind.IPAddress}}' "${REGISTRY_NAME}")
fi
echo -e "${GREEN}✅ Registry IP: ${REGISTRY_IP}:5000${NC}"
log_success "Registry connected: ${REGISTRY_IP}:5000"

echo ""

# Step 4: Build Docker images
echo -e "${BLUE}Step 4: Building Docker images...${NC}"
log_step "4" "Building Docker images (api, worker-a, worker-b, worker-c)"

log_info "Building api image"
docker build -t aw-arch-demo-api:latest api/ 2>&1 | tee -a "${LOG_FILE}" | tail -5
echo -e "${GREEN}✅ Built api image${NC}"
log_success "Built api image: aw-arch-demo-api:latest"

log_info "Building worker-a image"
docker build -t aw-arch-demo-worker-a:latest -f worker_a/Dockerfile worker_a/ 2>&1 | tee -a "${LOG_FILE}" | tail -5
echo -e "${GREEN}✅ Built worker-a image${NC}"
log_success "Built worker-a image: aw-arch-demo-worker-a:latest"

log_info "Building worker-b image"
docker build -t aw-arch-demo-worker-b:latest -f worker_b/Dockerfile worker_b/ 2>&1 | tee -a "${LOG_FILE}" | tail -5
echo -e "${GREEN}✅ Built worker-b image${NC}"
log_success "Built worker-b image: aw-arch-demo-worker-b:latest"

log_info "Building worker-c image"
docker build -t aw-arch-demo-worker-c:latest -f worker_c/Dockerfile worker_c/ 2>&1 | tee -a "${LOG_FILE}" | tail -5
echo -e "${GREEN}✅ Built worker-c image${NC}"
log_success "Built worker-c image: aw-arch-demo-worker-c:latest"

echo ""

# Step 5: Load images into KIND
echo -e "${BLUE}Step 5: Loading images into KIND cluster...${NC}"
log_step "5" "Loading Docker images into KIND cluster"

log_info "Loading api image into KIND"
kind load docker-image aw-arch-demo-api:latest --name "${CLUSTER_NAME}" 2>&1 | tee -a "${LOG_FILE}"
echo -e "${GREEN}✅ Loaded api image${NC}"
log_success "Loaded api image into KIND cluster"

log_info "Loading worker-a image into KIND"
kind load docker-image aw-arch-demo-worker-a:latest --name "${CLUSTER_NAME}" 2>&1 | tee -a "${LOG_FILE}"
echo -e "${GREEN}✅ Loaded worker-a image${NC}"
log_success "Loaded worker-a image into KIND cluster"

log_info "Loading worker-b image into KIND"
kind load docker-image aw-arch-demo-worker-b:latest --name "${CLUSTER_NAME}" 2>&1 | tee -a "${LOG_FILE}"
echo -e "${GREEN}✅ Loaded worker-b image${NC}"
log_success "Loaded worker-b image into KIND cluster"

log_info "Loading worker-c image into KIND"
kind load docker-image aw-arch-demo-worker-c:latest --name "${CLUSTER_NAME}" 2>&1 | tee -a "${LOG_FILE}"
echo -e "${GREEN}✅ Loaded worker-c image${NC}"
log_success "Loaded worker-c image into KIND cluster"

echo ""

# Step 6: Install KEDA
echo -e "${BLUE}Step 6: Installing KEDA for queue-based autoscaling...${NC}"
log_step "6" "Installing KEDA (Kubernetes Event Driven Autoscaling)"

log_info "Checking if Helm is installed"
if ! command -v helm &> /dev/null; then
  log_error "Helm is not installed. Please install Helm: https://helm.sh/docs/intro/install/"
  exit 1
fi

log_info "Adding KEDA Helm repository"
helm repo add kedacore https://kedacore.github.io/charts 2>&1 | tee -a "${LOG_FILE}"
helm repo update 2>&1 | tee -a "${LOG_FILE}"

log_info "Installing KEDA via Helm"
helm install keda kedacore/keda --namespace keda --create-namespace 2>&1 | tee -a "${LOG_FILE}"
echo -e "${GREEN}✅ KEDA installed${NC}"
log_success "KEDA installed successfully"

# Wait for KEDA to be ready
log_info "Waiting for KEDA operator to be ready"
kubectl wait --for=condition=available --timeout=300s deployment/keda-operator -n keda 2>&1 | tee -a "${LOG_FILE}" || true
echo -e "${GREEN}✅ KEDA operator ready${NC}"
log_success "KEDA operator deployment ready"

log_info "Waiting for KEDA metrics API server to be ready"
kubectl wait --for=condition=available --timeout=300s deployment/keda-metrics-apiserver -n keda 2>&1 | tee -a "${LOG_FILE}" || true
echo -e "${GREEN}✅ KEDA metrics API server ready${NC}"
log_success "KEDA metrics API server deployment ready"

echo ""

# Step 7: Deploy manifests
echo -e "${BLUE}Step 7: Deploying Kubernetes manifests...${NC}"
log_step "7" "Applying K8s manifests (rabbitmq, api, workers)"

log_info "Deploying RabbitMQ"
kubectl apply -f k8s/rabbitmq.yaml 2>&1 | tee -a "${LOG_FILE}"
echo -e "${GREEN}✅ Deployed RabbitMQ${NC}"
log_success "Deployed RabbitMQ"

log_info "Deploying API"
kubectl apply -f k8s/api.yaml 2>&1 | tee -a "${LOG_FILE}"
echo -e "${GREEN}✅ Deployed API${NC}"
log_success "Deployed API"

log_info "Deploying Workers (A, B, C)"
kubectl apply -f k8s/workers.yaml 2>&1 | tee -a "${LOG_FILE}"
echo -e "${GREEN}✅ Deployed Workers (A, B, C)${NC}"
log_success "Deployed Workers (A, B, C)"

echo ""

# Step 8: Wait for deployments
echo -e "${BLUE}Step 8: Waiting for deployments to be ready...${NC}"
log_step "8" "Waiting for pod readiness (timeout: 5 minutes)"
echo "This may take 1-2 minutes..."

log_info "Waiting for rabbitmq deployment"
kubectl wait --for=condition=available --timeout=300s deployment/rabbitmq 2>&1 | tee -a "${LOG_FILE}" || true
log_success "RabbitMQ deployment ready"

log_info "Waiting for api deployment"
kubectl wait --for=condition=available --timeout=300s deployment/api 2>&1 | tee -a "${LOG_FILE}" || true
log_success "API deployment ready"

log_info "Waiting for worker-a deployment"
kubectl wait --for=condition=available --timeout=300s deployment/worker-a 2>&1 | tee -a "${LOG_FILE}" || true
log_success "Worker-A deployment ready"

log_info "Waiting for worker-b deployment"
kubectl wait --for=condition=available --timeout=300s deployment/worker-b 2>&1 | tee -a "${LOG_FILE}" || true
log_success "Worker-B deployment ready"

log_info "Waiting for worker-c deployment"
kubectl wait --for=condition=available --timeout=300s deployment/worker-c 2>&1 | tee -a "${LOG_FILE}" || true
log_success "Worker-C deployment ready"

echo -e "${GREEN}✅ Deployments ready${NC}"
log_success "All deployments ready"

echo ""

# Step 9: Deploy KEDA ScaledObjects
echo -e "${BLUE}Step 9: Deploying KEDA ScaledObjects for autoscaling...${NC}"
log_step "9" "Applying KEDA ScaledObjects (worker autoscaling)"

log_info "Deploying KEDA ScaledObjects for queue-based autoscaling"
kubectl apply -f k8s/keda-scalers.yaml 2>&1 | tee -a "${LOG_FILE}"
echo -e "${GREEN}✅ Deployed KEDA ScaledObjects${NC}"
log_success "Deployed KEDA ScaledObjects for worker autoscaling"

# Wait for ScaledObjects to be active
log_info "Waiting for KEDA ScaledObjects to be active"
sleep 5
echo -e "${GREEN}✅ KEDA ScaledObjects active${NC}"
log_success "KEDA ScaledObjects are now active and monitoring queue depth"

echo ""

# Step 10: Set up port forwarding
echo -e "${BLUE}Step 10: Setting up port forwarding...${NC}"
log_step "10" "Configuring kubectl port-forward for API and RabbitMQ"
echo "Port forwarding in background (PID logged for reference)..."

# Kill any existing port-forwards
log_info "Cleaning up existing port-forwards"
pkill -f "kubectl port-forward" 2>&1 | tee -a "${LOG_FILE}" || true
sleep 1

# Start new port-forward
log_info "Starting API port-forward on localhost:8000"
kubectl port-forward svc/api 8000:8000 > /tmp/api-portforward.log 2>&1 &
API_PF_PID=$!
echo -e "${GREEN}✅ API port-forward: localhost:8000 (PID: $API_PF_PID)${NC}"
log_success "API port-forward started (PID: $API_PF_PID)"

log_info "Starting RabbitMQ port-forward on localhost:5672 and localhost:15672"
kubectl port-forward svc/rabbitmq 5672:5672 15672:15672 > /tmp/rabbitmq-portforward.log 2>&1 &
RABBIT_PF_PID=$!
echo -e "${GREEN}✅ RabbitMQ port-forward: localhost:5672, localhost:15672 (PID: $RABBIT_PF_PID)${NC}"
log_success "RabbitMQ port-forward started (PID: $RABBIT_PF_PID)"

sleep 2

echo ""

# Step 11: Show status
echo -e "${BLUE}Step 11: Deployment Status${NC}"
log_step "11" "Final deployment status and KEDA configuration"
echo ""
echo "Pods:"
kubectl get pods -o wide 2>&1 | tee -a "${LOG_FILE}"
echo ""
echo "Services:"
kubectl get svc 2>&1 | tee -a "${LOG_FILE}"
echo ""
echo "KEDA ScaledObjects:"
kubectl get scaledobjects 2>&1 | tee -a "${LOG_FILE}"
echo ""

log_success "Deployment status:"
kubectl get pods -o wide 2>&1 | grep -E 'NAME|worker|api|rabbitmq' | tee -a "${LOG_FILE}"

echo ""

# Step 12: Ready to use
echo -e "${GREEN}════════════════════════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}✅ Kubernetes environment with KEDA autoscaling ready!${NC}"
echo -e "${GREEN}════════════════════════════════════════════════════════════════════════════════${NC}"
echo ""

log_success "Setup complete! Kubernetes environment with KEDA ready for testing"

echo -e "${YELLOW}📝 Test the API:${NC}"
echo ""
echo "  # Wait a few seconds for services to fully initialize, then:"
echo "  sleep 5"
echo ""
echo "  # Health check"
echo "  curl http://localhost:8000/health"
echo ""
echo "  # Run 1 simulation"
echo "  curl -X POST http://localhost:8000/pipeline \\"
echo "    -H 'Content-Type: application/json' \\"
echo "    -d '{\"models\": [\"worker_a\", \"worker_b\"], \"simulations\": 1}'"
echo ""
echo "  # Run 100 simulations (watch KEDA scale workers!)"
echo "  curl -X POST http://localhost:8000/pipeline \\"
echo "    -H 'Content-Type: application/json' \\"
echo "    -d '{\"models\": [\"worker_a\", \"worker_b\", \"worker_c\"], \"simulations\": 100}'"
echo ""

echo -e "${YELLOW}📊 Monitor the deployment:${NC}"
echo ""
echo "  # Terminal dashboard (real-time worker + queue + KEDA scaling)"
echo "  bash scripts/monitor.sh"
echo ""
echo "  # Watch KEDA scaling events in real-time"
echo "  kubectl get scaledobjects -w"
echo ""
echo "  # Get pipeline status"
echo "  curl http://localhost:8000/pipeline/{pipeline_id}"
echo ""

echo -e "${YELLOW}🛠️  Useful kubectl commands:${NC}"
echo ""
echo "  kubectl get pods                           # List all pods"
echo "  kubectl get scaledobjects                  # List KEDA scalers"
echo "  kubectl describe scaledobject worker-a-scaler  # Scaler details"
echo "  kubectl logs -f deployment/worker-a       # Watch worker logs"
echo "  kubectl delete all --all                   # Clean up (keeps cluster)"
echo "  kind delete cluster --name ${CLUSTER_NAME}    # Delete cluster"
echo ""

echo -e "${YELLOW}🌐 Access RabbitMQ Management:${NC}"
echo "  http://localhost:15672"
echo "  Username: guest"
echo "  Password: guest"
echo ""

echo -e "${YELLOW}📋 KEDA Autoscaling Info:${NC}"
echo "  Scaling Behavior:"
echo "  - Min replicas: 1 (always at least one worker running)"
echo "  - Max replicas: 5 (scales up to 5 for high load)"
echo "  - Scale trigger: Queue depth > 10 messages"
echo ""
echo "  Queues monitored:"
echo "  - Worker A: jobs.worker_a"
echo "  - Worker B: jobs.worker_b"
echo "  - Worker C: jobs.worker_c"
echo ""
echo "  Check KEDA status:"
echo "  kubectl get scaledobjects"
echo "  kubectl describe scaledobject worker-a-scaler"
echo ""

echo -e "${YELLOW}📁 Setup logs:${NC}"
echo "  ${LOG_FILE}"
echo ""

echo -e "${GREEN}Happy testing! 🚀${NC}"
echo ""

log_success "════════════════════════════════════════════════════════════════════════════════"
log_success "Setup script completed successfully"
log_success "════════════════════════════════════════════════════════════════════════════════"
