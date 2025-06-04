#!/bin/bash
set -e # Exit immediately if a command exits with a non-zero status.

# Clear screen for better readability
clear

echo "--- Building Docker images ---"

# Build and tag Orion image
echo "Building mrschrodingers/pipeboard-orion:latest..."
docker build --no-cache \
  -f docker/Dockerfile.orion \
  -t mrschrodingers/pipeboard-orion:latest .

# Build and tag Worker image
echo "Building mrschrodingers/pipeboard-worker:latest..."
docker build --no-cache \
  -f infrastructure/prefect/worker/Dockerfile.worker \
  -t mrschrodingers/pipeboard-worker:latest .

# Build and tag Flow image (used by Prefect deployments)
echo "Building mrschrodingers/pipeboard-flow:latest..."
docker build --no-cache \
  -f infrastructure/prefect/flow/Dockerfile.flow \
  -t mrschrodingers/pipeboard-flow:latest .

# Build and tag Setup image
echo "Building mrschrodingers/pipeboard-setup:latest..."
docker build --no-cache \
  -f docker/Dockerfile.setup \
  -t mrschrodingers/pipeboard-setup:latest .

echo "--- Pushing images to Docker Hub ---"
# Ensure you are logged into Docker Hub: docker login

echo "Pushing mrschrodingers/pipeboard-orion:latest..."
docker push mrschrodingers/pipeboard-orion:latest

echo "Pushing mrschrodingers/pipeboard-worker:latest..."
docker push mrschrodingers/pipeboard-worker:latest

echo "Pushing mrschrodingers/pipeboard-flow:latest..."
docker push mrschrodingers/pipeboard-flow:latest

echo "Pushing mrschrodingers/pipeboard-setup:latest..."
docker push mrschrodingers/pipeboard-setup:latest

echo "--- Restarting Docker Compose services ---"
# Navigate to the directory containing docker-compose.yml
cd docker

echo "Bringing down existing services, volumes, and removing orphans..."
# docker compose down --volumes --remove-orphans

echo "Bringing up services in detached mode with --build..."
# This will use the build contexts defined in docker-compose.yml
# and the image names (now updated to mrschrodingers/*)
docker compose up -d --build

# Navigate back to the project root
cd ..

clear
echo "--- Deployment script finished ---"
echo "Prefect UI should be available at http://localhost:${HOST_PREFECT_PORT:-44200} (check your .env for HOST_PREFECT_PORT)"
echo "Grafana should be available at http://localhost:${HOST_GRAFANA_PORT:-34444} (check your .env for HOST_GRAFANA_PORT)"

