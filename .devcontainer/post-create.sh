#!/bin/bash
set -e

echo "🚀 Setting up AW Arch Demo development environment..."

# Update pip, setuptools, wheel
echo "📦 Upgrading Python tools..."
pip install --upgrade pip setuptools wheel

# Install project dependencies
echo "📦 Installing API dependencies..."
pip install -r api/requirements.txt

echo "📦 Installing Worker A dependencies..."
pip install -r worker_a/requirements.txt

echo "📦 Installing Worker B dependencies..."
pip install -r worker_b/requirements.txt

echo "📦 Installing Worker C dependencies..."
pip install -r worker_c/requirements.txt

# Install development tools
echo "🛠️  Installing development tools..."
pip install \
  pytest pytest-asyncio pytest-cov \
  black flake8 pylint mypy \
  ipython ipdb \
  docker docker-compose

# Install curl and jq for testing
echo "🛠️  Installing CLI tools..."
apt-get update && apt-get install -y \
  curl \
  jq \
  git \
  wget

echo ""
echo "✅ Development environment ready!"
echo ""
echo "📝 Next steps:"
echo "  1. Start services: docker-compose up --build"
echo "  2. In another terminal, test the API:"
echo "     curl -s -X POST http://localhost:8000/pipeline | jq ."
echo ""
echo "📚 Useful commands:"
echo "  - docker-compose up --build      # Start all services"
echo "  - docker-compose logs -f         # Watch logs"
echo "  - docker-compose ps              # Check service status"
echo "  - curl http://localhost:8000/health  # Health check"
echo ""
