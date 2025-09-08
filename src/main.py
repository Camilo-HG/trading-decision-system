import json
import os
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import PlainTextResponse
from prometheus_client import generate_latest
from pydantic import BaseModel
from redis import Redis
from redis.exceptions import ConnectionError

# Pydantic models for request and response data
class InsightResponse(BaseModel):
    signal: str
    conviction: float
    causality: str
    sentiment_score: float
    key_market_drivers: str
    risk_score: float
    event_timestamp: str

class LatestEventResponse(BaseModel):
    event_timestamp: str

app = FastAPI()

# Redis connection
redis_client: Optional[Redis] = None
REDIS_CIRCUIT_BREAKER_OPEN = False
REDIS_CIRCUIT_FAILURES = 0
REDIS_MAX_FAILURES = 5

def get_redis_client():
    """Initializes and returns a Redis client with a simple circuit breaker."""
    global redis_client, REDIS_CIRCUIT_BREAKER_OPEN
    if REDIS_CIRCUIT_BREAKER_OPEN:
        return None
    if not redis_client:
        redis_host = os.getenv("REDIS_HOST", "redis")
        redis_port = int(os.getenv("REDIS_PORT", 6379))
        try:
            client = Redis(host=redis_host, port=redis_port, db=0, decode_responses=True)
            client.ping()
            redis_client = client
        except ConnectionError as e:
            print(f"Could not connect to Redis: {e}")
            REDIS_CIRCUIT_BREAKER_OPEN = True
            return None
    return redis_client

# Helper function to get the latest event timestamp
def get_latest_event_timestamp(asset_ticker: str) -> str:
    """
    Performs a lookup in Redis for the latest event timestamp for a given asset.
    """
    redis_client = get_redis_client()
    if not redis_client:
        raise HTTPException(
            status_code=503,
            detail="Redis cache service is unavailable."
        )

    try:
        latest_timestamp = redis_client.get(f"latest_event:{asset_ticker}")
        if not latest_timestamp:
            raise HTTPException(
                status_code=404,
                detail=f"No latest event found for asset: {asset_ticker}"
            )
        return latest_timestamp
    except ConnectionError:
        raise HTTPException(
            status_code=503,
            detail="Redis connection failed."
        )

# API Endpoints
@app.get("/{asset_ticker}/latest_event", response_model=LatestEventResponse)
async def get_latest_event(asset_ticker: str):
    """
    Retrieves the latest event timestamp for a given asset ticker.
    """
    event_timestamp = get_latest_event_timestamp(asset_ticker)
    return LatestEventResponse(event_timestamp=event_timestamp)

@app.get("/insights/{asset_ticker}", response_model=InsightResponse)
async def get_insights(asset_ticker: str):
    """
    Retrieves the full pre-computed insight for a given asset ticker.
    This endpoint uses the latest event timestamp to perform a second lookup.
    """
    redis_client = get_redis_client()
    if not redis_client:
        raise HTTPException(
            status_code=503,
            detail="Redis cache service is unavailable."
        )

    try:
        # Use the helper function to get the latest timestamp
        latest_timestamp = get_latest_event_timestamp(asset_ticker)
        
        # Use the timestamp to get the full insight data
        insight_key = f"{asset_ticker}:{latest_timestamp}"
        insight_data_bytes = redis_client.get(insight_key)
        
        if not insight_data_bytes:
            raise HTTPException(
                status_code=404,
                detail=f"Insight data not found for key: {insight_key}"
            )
            
        insight_data = json.loads(insight_data_bytes)
    except ConnectionError:
        raise HTTPException(
            status_code=503,
            detail="Redis connection failed."
        )
    except json.JSONDecodeError:
        raise HTTPException(
            status_code=500,
            detail="Error decoding JSON from Redis."
        )
    else:
        return InsightResponse(**insight_data)

# Metrics endpoint for Prometheus.
@app.get("/metrics")
def metrics():
    """
    Endpoint that exposes application metrics in Prometheus format.
    """
    return PlainTextResponse(content=generate_latest().decode("utf-8"))

# A simple health check for the service.
@app.get("/health")
def health_check():
    """
    Health check endpoint for the service and Redis connection.
    """
    client = get_redis_client()
    redis_status = "ok" if client and client.ping() else "not_available"
    return {"status": "ok", "redis": redis_status}