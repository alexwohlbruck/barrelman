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
            # Anonymous volumes hold node_modules so the container's Linux
            # installs never fight the host's macOS ones — but Compose reuses
            # them across recreation, so a rebuild alone leaves a stale
            # node_modules masking the freshly built image. Renewing them here
            # makes --build mean what everyone expects it to: pick up new
            # dependencies. (Diagnosed the hard way: the API crash-looped on a
            # module that was present in the image and absent in the volume.)
            BUILD_FLAG="--build --renew-anon-volumes"
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

    # The marketing site lives in a sibling repo. Enable its compose profile
    # only when that checkout exists, so cloning barrelman alone still works.
    if [ -d "../barrelman-landing" ]; then
        COMPOSE_CMD="$COMPOSE_CMD --profile landing"
        LANDING_ENABLED=1
    else
        echo "  (../barrelman-landing not found — skipping the marketing site)"
    fi
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
echo ""
echo "  API            http://localhost:5001"
echo "  API docs       http://localhost:5001/docs"
if [ "$MODE" = "dev" ]; then
    # Trailing slash matters here — Vite's base is '/console/' and the
    # slashless form 404s.
    echo "  Console        http://localhost:5199/console/  (HMR)"
else
    echo "  Console        http://localhost:5001/console"
fi
if [ -n "$LANDING_ENABLED" ]; then
    echo "  Landing site   http://localhost:5200            (HMR)"
fi
echo ""
echo "  Martin tiles   internal only (proxied via the API)"
echo "  GraphHopper    http://localhost:5001/graphhopper/*  |  :5003 direct"
echo ""
echo "To stop: $0 $MODE --down"
