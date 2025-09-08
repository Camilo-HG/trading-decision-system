import os
import time
import json
import logging
from prometheus_client import start_http_server, Counter, Gauge, Histogram
from fastapi import FastAPI, Depends, HTTPException, Request
from fastapi.responses import JSONResponse
from redis import Redis
from loguru import logger
import uvloop

# Set uvloop as the default event loop
uvloop.install()

# --- Configuration ---
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
PROMETHEUS_PORT = int(os.getenv("PROMETHEUS_PORT", 9000))

# --- Custom Logging ---
# You can use a custom logger here or just a basic one for now
# For this example, we'll use a simple setup with loguru
logger.add("file.log", rotation="500 MB")

# --- Prometheus Metrics ---
# Counters to track total requests and cache hits/misses
TOTAL_REQUESTS = Counter(
    "fastapi_requests_total", "Total number of requests to the API"
)
CACHE_HITS = Counter("fastapi_cache_hits_total", "Total number of cache hits")
CACHE_MISSES = Counter(
    "fastapi_cache_misses_total", "Total number of cache misses"
)

# Gauge to monitor Redis connection status
REDIS_CONNECTION_STATUS = Gauge(
    "redis_connection_status", "Status of Redis connection (1=up, 0=down)"
)

# Histogram for request latency
REQUEST_LATENCY = Histogram(
    "fastapi_request_latency_seconds",
    "API request latency in seconds",
    buckets=[0.1, 0.5, 1.0, 2.5, 5.0, 10.0],
)

# --- FastAPI Application ---
app = FastAPI()

def get_redis_connection():
    try:
        redis_client = Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
        redis_client.ping()
        REDIS_CONNECTION_STATUS.set(1)
        return redis_client
    except Exception as e:
        logger.error(f"Could not connect to Redis: {e}")
        REDIS_CONNECTION_STATUS.set(0)
        raise HTTPException(
            status_code=500, detail="Could not connect to Redis"
        )

# --- Middleware for request latency tracking ---
@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    REQUEST_LATENCY.observe(process_time)
    logger.info(
        f"Request to {request.url.path} took {process_time:.4f}s",
        extra={"request_path": request.url.path, "latency_seconds": process_time},
    )
    return response

# --- API Endpoints ---
@app.get("/health")
def health_check():
    return {"status": "ok"}

@app.get("/metrics")
def metrics():
    return JSONResponse(content={}) # Placeholder as prometheus-client handles it

@app.get("/decide/{asset_ticker}/{event_timestamp}")
def get_decision(
    asset_ticker: str,
    event_timestamp: int,
    redis_client: Redis = Depends(get_redis_connection),
):
    TOTAL_REQUESTS.inc()
    cache_key = f"{asset_ticker}:{event_timestamp}"
    
    try:
        # Attempt to retrieve data from the cache
        cached_data = redis_client.get(cache_key)

        if cached_data:
            CACHE_HITS.inc()
            data = json.loads(cached_data)
            logger.info(f"Cache hit for key: {cache_key}")
            return data
        else:
            CACHE_MISSES.inc()
            logger.warning(f"Cache miss for key: {cache_key}")
            # In a real scenario, this is where a fallback to the slow-path
            # or a synchronous re-computation would occur.
            raise HTTPException(status_code=404, detail="Data not found in cache")

    except Exception as e:
        logger.error(f"Unhandled error: {e}", extra={"error": str(e)})
        raise HTTPException(status_code=500, detail="Internal Server Error")