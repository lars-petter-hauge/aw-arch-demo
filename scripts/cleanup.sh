#!/bin/bash

echo "🧹 Cleaning up K8s resources..."
echo ""

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

CLUSTER_NAME="aw-arch-demo"

echo -e "${YELLOW}Options:${NC}"
echo "1) Delete all pods/services (keep cluster)"
echo "2) Delete entire cluster"
echo "3) Cancel"
echo ""
read -p "Choose option (1-3): " choice

case $choice in
  1)
    echo ""
    echo "Deleting all pods and services..."
    kubectl delete all --all 2>&1
    echo -e "${GREEN}✅ All pods and services deleted${NC}"
    echo "KIND cluster still active. Run 'bash scripts/setup-k8s.sh' to redeploy."
    ;;
  2)
    echo ""
    read -p "Are you sure you want to delete the entire '${CLUSTER_NAME}' cluster? (y/n): " confirm
    if [ "$confirm" = "y" ]; then
      echo "Deleting KIND cluster..."
      kind delete cluster --name "${CLUSTER_NAME}" 2>&1
      echo -e "${GREEN}✅ KIND cluster '${CLUSTER_NAME}' deleted${NC}"
    else
      echo "Cancelled."
    fi
    ;;
  3)
    echo "Cancelled."
    ;;
  *)
    echo "Invalid option."
    ;;
esac

echo ""
echo -e "${GREEN}Done!${NC}"
