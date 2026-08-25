#!/bin/sh

# If argument is specifically "worker", run only worker
if [ "$1" = "worker" ]; then
    echo "Starting RabbitMQ RPC Worker Node only..."
    exec ./worker
fi

# If argument is specifically "server", run only server
if [ "$1" = "server" ]; then
    echo "Starting Teams SFU API Server only..."
    exec ./server
fi



# Default mode: Start RPC Worker in background and run API Server in foreground
echo "========================================================"
echo " Starting Backend Services (Server + Worker) "
echo "========================================================"



# Start RPC worker in background if RABBITMQ_URL is present
if [ -n "$RABBITMQ_URL" ]; then
    echo "[Info] Launching RabbitMQ RPC Worker in background..."
    ./worker &
    WORKER_PID=$!
fi

# Handle graceful shutdown of background processes
trap "echo 'Stopping processes...'; kill -TERM $WORKER_PID 2>/dev/null; exit 0" SIGINT SIGTERM

# Start API Server in foreground
echo "[Info] Launching API Server on port ${PORT:-8080}..."
./server &
SERVER_PID=$!

# Wait for server process
wait $SERVER_PID
