#!/bin/bash
set -e
IMAGE_NAME="oos-webapp-be"
CONTAINER_NAME="oos-webapp-be"
PORT="8000"
echo "=== Starting deployment for $IMAGE_NAME ==="
if ! command -v docker &> /dev/null; then
    echo "ERROR: Docker is not installed or not in PATH"
    exit 1
fi
if ! docker info &> /dev/null; then
    echo "ERROR: Docker daemon is not running"
    exit 1
fi
echo "✓ Docker is available and running"
if docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}"$; then
    echo "Stopping existing container: $CONTAINER_NAME"
    docker stop "$CONTAINER_NAME" || true
    echo "Removing existing container: $CONTAINER_NAME"
    docker rm "$CONTAINER_NAME" || true
    echo "✓ Existing container removed"
else
    echo "✓ No existing container found"
fi
if docker images --format '{{.Repository}}' | grep -q "^${IMAGE_NAME}"$; then
    echo "Removing existing image: $IMAGE_NAME"
    docker rmi "$IMAGE_NAME" || true
    echo "✓ Existing image removed"
else
    echo "✓ No existing image found"
fi
ENV_FILE=".env"
if [ -f "$ENV_FILE" ]; then
    echo "✓ Found .env file, will load environment variables"
    ENV_FILE_ABS=$(cd "$(dirname "$ENV_FILE")" && pwd)/$(basename "$ENV_FILE")
    echo "  Using .env file at: $ENV_FILE_ABS"
else
    echo "ERROR: .env file not found at: $ENV_FILE"
    echo "       The .env file is required for the application to run correctly."
    echo "       Please create a .env file with the required environment variables."
    exit 1
fi
echo "Building Docker image: $IMAGE_NAME"
if docker build -t "$IMAGE_NAME" .; then
    echo "✓ Docker image built successfully"
else
    echo "ERROR: Failed to build Docker image"
    exit 1
fi
echo "Starting container: $CONTAINER_NAME"
echo "  Using environment file: $ENV_FILE_ABS"
if docker run -d \
    --name "$CONTAINER_NAME" \
    -p "$PORT:8000" \
    --env-file "$ENV_FILE_ABS" \
    "$IMAGE_NAME"; then
    echo "✓ Container started successfully"
else
    echo "ERROR: Failed to start container"
    exit 1
fi
sleep 2
if docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}"$; then
    echo "✓ Container is running"
    echo ""
    echo "=== Deployment successful ==="
    echo "Container name: $CONTAINER_NAME"
    echo "Image name: $IMAGE_NAME"
    echo "Port mapping: $PORT:8000"
    echo ""
    echo "View logs with: docker logs $CONTAINER_NAME"
    echo "Stop container with: docker stop $CONTAINER_NAME"
else
    echo "ERROR: Container started but is not running"
    echo "Checking container logs:"
    docker logs "$CONTAINER_NAME" || true
    exit 1
fi
