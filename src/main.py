import json
import os
from typing import Optional
from datetime import datetime

from fastapi import FastAPI, HTTPException
from fastapi.responses import PlainTextResponse
from prometheus_client import generate_latest
from pydantic import BaseModel
from redis import Redis
from redis.exceptions import ConnectionError

# Pydantic models for request and response data
class InsightResponse(BaseModel):
    signal: int
    conviction: float
    causality: str
    # sentiment_score: float
    # key_market_drivers: str
    # risk_score: float
    event_timestamp: str # Changed to str to match Redis data format

class LatestEventResponse(BaseModel):
    event_timestamp: str # Changed to str to match Redis data format

class DecisionResponse(BaseModel):
    asset_ticker: str
    time_difference_seconds: float

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

# Helper functions to retrieve data from Redis
def get_latest_event_timestamp_from_db(asset_ticker: str) -> str:
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

def get_latest_insight_from_db(asset_ticker: str) -> InsightResponse:
    """
    Retrieves the full pre-computed latest insight for a given asset ticker.
    This helper function is used by multiple endpoints to avoid routing issues.
    """
    redis_client = get_redis_client()
    if not redis_client:
        raise HTTPException(
            status_code=503,
            detail="Redis cache service is unavailable."
        )

    try:
        # Use the helper function to get the latest timestamp
        latest_timestamp = get_latest_event_timestamp_from_db(asset_ticker)
        
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

# API Endpoints
@app.get("/{asset_ticker}/latest_event_timestamp", response_model=LatestEventResponse)
async def get_latest_event(asset_ticker: str):
    """
    Retrieves the latest event timestamp for a given asset ticker.
    """
    event_timestamp = get_latest_event_timestamp_from_db(asset_ticker)
    return LatestEventResponse(event_timestamp=event_timestamp)

@app.get("/insights/{asset_ticker}/latest", response_model=InsightResponse)
async def get_insights_latest(asset_ticker: str):
    """
    Retrieves the full pre-computed insight for a given asset ticker.
    This endpoint uses the latest event timestamp to perform a second lookup.
    """
    return get_latest_insight_from_db(asset_ticker)


@app.get("/insights/{asset_ticker}/{event_timestamp}", response_model=InsightResponse)
async def get_insights_by_timestamp(asset_ticker: str, event_timestamp: datetime):
    """
    Retrieves the full pre-computed insight for a given asset ticker and event timestamp.
    This endpoint uses the provided event timestamp to perform a second lookup.

    Notes:
        - event_timestamp is expected to be in ISO 8601 format (e.g., '2023-10-01T12:00:00').
        - Internally, Redis stores timestamps in Unix epoch format (integer).
    """
    redis_client = get_redis_client()
    if not redis_client:
        raise HTTPException(
            status_code=503,
            detail="Redis cache service is unavailable."
        )

    # Redis stores timestamps in Unix epoch timestamp (integer)
    # Change provided event_timestamp string to integer
    event_timestamp_unix = int(event_timestamp.timestamp())

    try:
        # Use the timestamp to get the full insight data
        insight_key = f"{asset_ticker}:{event_timestamp_unix}"
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

@app.get("/decide/{asset_ticker}", response_model=DecisionResponse)
async def get_decision_metrics(asset_ticker: str):
    """
    Returns the time difference between the current time and the latest event timestamp.
    """
    # Call the new helper function to get the latest insights
    try:
        insights = get_latest_insight_from_db(asset_ticker)
    except HTTPException as e:
        # Pass through the error from the insights endpoint
        raise e

    # Calculate the time difference
    # event_timestamp from insights is a string, so we cast it to int before converting to datetime
    latest_event_time = datetime.strptime(insights.event_timestamp, "%Y-%m-%dT%H:%M:%S")
    current_time = datetime.now()
    time_difference = (current_time - latest_event_time).total_seconds()

    return DecisionResponse(
        asset_ticker=asset_ticker,
        time_difference_seconds=time_difference
    )

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
