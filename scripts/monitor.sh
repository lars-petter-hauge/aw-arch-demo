#!/bin/bash
set -e

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${BLUE}═══════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}  AW Arch Demo - Real-time Worker & Queue Monitor${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════${NC}"
echo ""
echo "Monitoring pod status, resource usage, and queue depth..."
echo "Press Ctrl+C to stop"
echo ""

# Function to get pod metrics
get_pod_metrics() {
  local pod=$1
  local namespace="default"
  
  # Get pod status
  local status=$(kubectl get pod "$pod" -n "$namespace" -o jsonpath='{.status.phase}' 2>/dev/null || echo "N/A")
  
  # Try to get resource usage (requires metrics-server)
  local cpu="N/A"
  local memory="N/A"
  
  if kubectl top pod "$pod" -n "$namespace" &>/dev/null 2>&1; then
    local metrics=$(kubectl top pod "$pod" -n "$namespace" --no-headers 2>/dev/null || echo "N/A")
    if [ "$metrics" != "N/A" ]; then
      cpu=$(echo "$metrics" | awk '{print $1}')
      memory=$(echo "$metrics" | awk '{print $2}')
    fi
  fi
  
  echo "$status|$cpu|$memory"
}

# Function to get queue depth via RabbitMQ API
get_queue_depth() {
  local queue=$1
  
  # Try to query RabbitMQ management API
  local depth=$(curl -s -u guest:guest http://localhost:15672/api/queues/%2F/"$queue" 2>/dev/null | grep -o '"messages":[0-9]*' | cut -d':' -f2 || echo "0")
  echo "$depth"
}

# Function to format numbers
format_number() {
  local num=$1
  printf "%5s" "$num"
}

# Main monitoring loop
while true; do
  clear
  
  echo -e "${BLUE}═══════════════════════════════════════════════════════${NC}"
  echo -e "${BLUE}  Pod Status & Metrics${NC}"
  echo -e "${BLUE}═══════════════════════════════════════════════════════${NC}"
  echo ""
  
  # Header
  printf "%-20s %-12s %-12s %-12s\n" "Pod" "Status" "CPU" "Memory"
  printf "%-20s %-12s %-12s %-12s\n" "───────────────────" "───────────" "───────────" "───────────"
  
  # RabbitMQ
  local rabbitmq_data=$(get_pod_metrics "$(kubectl get pods -o name | grep rabbitmq | cut -d'/' -f2)" 2>/dev/null || echo "N/A|N/A|N/A")
  local rabbitmq_status=$(echo "$rabbitmq_data" | cut -d'|' -f1)
  local rabbitmq_cpu=$(echo "$rabbitmq_data" | cut -d'|' -f2)
  local rabbitmq_mem=$(echo "$rabbitmq_data" | cut -d'|' -f3)
  printf "%-20s %-12s %-12s %-12s\n" "rabbitmq" "${rabbitmq_status}" "${rabbitmq_cpu}" "${rabbitmq_mem}"
  
  # API
  local api_pod=$(kubectl get pods -o name -l app=api 2>/dev/null | head -1 | cut -d'/' -f2 || echo "N/A")
  if [ "$api_pod" != "N/A" ]; then
    local api_data=$(get_pod_metrics "$api_pod")
    local api_status=$(echo "$api_data" | cut -d'|' -f1)
    local api_cpu=$(echo "$api_data" | cut -d'|' -f2)
    local api_mem=$(echo "$api_data" | cut -d'|' -f3)
    printf "%-20s %-12s %-12s %-12s\n" "api" "${api_status}" "${api_cpu}" "${api_mem}"
  fi
  
  # Workers
  for worker in worker-a worker-b worker-c; do
    local pod=$(kubectl get pods -o name -l app="$worker" 2>/dev/null | head -1 | cut -d'/' -f2 || echo "N/A")
    if [ "$pod" != "N/A" ]; then
      local data=$(get_pod_metrics "$pod")
      local status=$(echo "$data" | cut -d'|' -f1)
      local cpu=$(echo "$data" | cut -d'|' -f2)
      local mem=$(echo "$data" | cut -d'|' -f3)
      printf "%-20s %-12s %-12s %-12s\n" "$worker" "${status}" "${cpu}" "${mem}"
    fi
  done
  
  echo ""
  echo -e "${BLUE}═══════════════════════════════════════════════════════${NC}"
  echo -e "${BLUE}  Message Queues (Jobs Waiting)${NC}"
  echo -e "${BLUE}═══════════════════════════════════════════════════════${NC}"
  echo ""
  
  printf "%-30s %-20s\n" "Queue" "Depth (Jobs Waiting)"
  printf "%-30s %-20s\n" "─────────────────────────────" "───────────────────"
  
  # Query queue depths
  local queue_a=$(get_queue_depth "jobs.worker_a")
  local queue_b=$(get_queue_depth "jobs.worker_b")
  local queue_c=$(get_queue_depth "jobs.worker_c")
  
  printf "%-30s %s\n" "jobs.worker_a" "$(format_number $queue_a) jobs"
  printf "%-30s %s\n" "jobs.worker_b" "$(format_number $queue_b) jobs"
  printf "%-30s %s\n" "jobs.worker_c" "$(format_number $queue_c) jobs"
  
  local total_jobs=$((queue_a + queue_b + queue_c))
  echo ""
  printf "%-30s %s\n" "Total" "$(format_number $total_jobs) jobs"
  
  echo ""
  echo -e "${BLUE}═══════════════════════════════════════════════════════${NC}"
  echo -e "${BLUE}  Info${NC}"
  echo -e "${BLUE}═══════════════════════════════════════════════════════${NC}"
  echo ""
  echo -e "${YELLOW}API Endpoint:${NC}         http://localhost:8000"
  echo -e "${YELLOW}RabbitMQ Management:${NC}  http://localhost:15672 (guest/guest)"
  echo -e "${YELLOW}Last Updated:${NC}        $(date '+%Y-%m-%d %H:%M:%S')"
  echo ""
  echo -e "${YELLOW}Commands:${NC}"
  echo -e "  Get pipeline status:     curl http://localhost:8000/pipeline/{id}"
  echo -e "  Submit pipeline:         curl -X POST http://localhost:8000/pipeline -H 'Content-Type: application/json' -d '{\"models\": [\"worker_a\", \"worker_b\"], \"simulations\": 1}'"
  echo ""
  echo -e "${YELLOW}Press Ctrl+C to stop monitoring${NC}"
  echo ""
  
  # Sleep before next update
  sleep 5
done
