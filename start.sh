#!/bin/bash
echo "🚀 Starting FuelMetrics API..."
PORT=${PORT:-8000}
echo "Using port: $PORT"
exec uvicorn app.main:app --host 0.0.0.0 --port $PORT
