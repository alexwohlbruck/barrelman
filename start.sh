#!/bin/bash

# Simple script to run Barrelman in development or production mode

show_usage() {
    echo "Usage: $0 [dev|prod] [options]"
    echo ""
    echo "Modes:"
    echo "  dev   - Development mode with hot-reload and local source mounts"
    echo "  prod  - Production mode using published Docker images"
    echo ""
    echo "Options:"
    echo "  --build  - Force rebuild of images (dev mode only)"
    echo "  --down   - Stop and remove containers"
    echo ""
    echo "Examples:"
    echo "  $0 dev           # Start development environment"
    echo "  $0 dev --build   # Start development with rebuild"
    echo "  $0 prod          # Start production environment"
    echo "  $0 dev --down    # Stop development environment"
    exit 1
}

MODE=""
BUILD_FLAG=""
DOWN_FLAG=""

# Parse arguments
for arg in "$@"; do
    case $arg in
        dev|prod)
            MODE=$arg
            ;;
        --build)
            BUILD_FLAG="--build"
            ;;
        --down)
            DOWN_FLAG="--down"
            ;;
        *)
            echo "Unknown argument: $arg"
            show_usage
            ;;
    esac
done

# Show usage if no mode provided
if [ -z "$MODE" ]; then
    show_usage
fi

# Set compose files based on mode
if [ "$MODE" = "dev" ]; then
    COMPOSE_CMD="docker compose -f docker-compose.yml -f docker-compose.dev.yml"
    echo "Starting Barrelman in development mode..."
elif [ "$MODE" = "prod" ]; then
    COMPOSE_CMD="docker compose -f docker-compose.yml"
    echo "Starting Barrelman in production mode..."
    if [ -n "$BUILD_FLAG" ]; then
        echo "Build flag ignored in production mode (uses published images)"
        BUILD_FLAG=""
    fi
fi

# Handle down flag
if [ -n "$DOWN_FLAG" ]; then
    echo "Stopping Barrelman services..."
    $COMPOSE_CMD down
    exit 0
fi

# Start services
$COMPOSE_CMD up $BUILD_FLAG -d

echo ""
echo "Barrelman started successfully!"
echo "API: http://localhost:5001"
echo "Martin tiles: internal only (proxied via API)"
echo "GraphHopper: http://localhost:5001/graphhopper/* (proxied)  |  http://localhost:8989 (direct, debug)"
echo ""
echo "To stop: $0 $MODE --down"
