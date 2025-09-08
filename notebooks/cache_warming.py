# Databricks Slow-Path Pipeline - Cache Warming
# This code is designed to run within a continuous Databricks job, warming the Redis cache.

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import json
import redis
import os

def _connect_to_redis() -> redis.Redis:
    """Establishes a connection to the Redis cache."""
    # This function is defined inside the worker process,
    # so it creates a connection per worker, which is efficient.
    # We use environment variables for a real-world scenario.
    redis_host = os.getenv("REDIS_HOST", "localhost")
    redis_port = int(os.getenv("REDIS_PORT", 6379))
    try:
        r = redis.Redis(host=redis_host, port=redis_port, db=0)
        r.ping()
        return r
    except redis.exceptions.ConnectionError as e:
        print(f"Could not connect to Redis: {e}")
        raise

def _write_batch_to_redis(partition):
    """
    Writes a partition of pre-computed insights to Redis.
    This function is executed by a Spark worker for a given partition.
    """
    r = _connect_to_redis()
    pipe = r.pipeline()
    max_timestamp = 0
    asset_to_latest_timestamp = {}

    for row in partition:
        # Create the composite key: {asset_ticker}:{event_timestamp}
        composite_key = f"{row.normalized_asset}:{row.event_timestamp}"

        # Create a dictionary of the pre-computed features
        precomputed_features = {
            "signal": row.signal,
            "conviction": row.conviction,
            "causality": row.causality,
            "sentiment_score": row.sentiment_score,
            "key_market_drivers": row.key_market_drivers,
            "risk_score": row.risk_score,
            "event_timestamp": row.event_timestamp,
        }
        
        # Write the features as a JSON string to Redis
        # Using HSET is an alternative for structured data:
        # pipe.hmset(composite_key, precomputed_features)
        
        # Here we use SET for simplicity with JSON string
        pipe.set(composite_key, json.dumps(precomputed_features))

        # Update the latest event timestamp for the asset
        asset = row.normalized_asset
        if asset not in asset_to_latest_timestamp or row.event_timestamp > asset_to_latest_timestamp[asset]:
            asset_to_latest_timestamp[asset] = row.event_timestamp

    # Execute all commands in the pipeline
    pipe.execute()

    # Update the "latest_event" key for each asset in the batch
    # This must be done separately to avoid race conditions with the pipeline
    for asset, latest_ts in asset_to_latest_timestamp.items():
        latest_key = f"latest_event:{asset}"
        r.set(latest_key, latest_ts)

def load_to_cache(spark, gold_table_name: str, checkpoint_location: str, redis_config: dict):
    """
    Reads from the Gold Delta table and writes pre-computed insights to Redis.

    Args:
        spark: The PySpark session object.
        gold_table_name (str): The name of the Gold Delta table.
        checkpoint_location (str): The cloud storage path for the stream's checkpoint.
        redis_config (dict): A dictionary with Redis connection details.
    """
    # Set Redis environment variables so workers can access them
    os.environ["REDIS_HOST"] = redis_config.get("host", "localhost")
    os.environ["REDIS_PORT"] = str(redis_config.get("port", 6379))
    
    print(f"Reading from gold layer {gold_table_name} and warming the Redis cache...")

    gold_df = spark.readStream.table(gold_table_name)
    
    # Use foreachBatch to process each micro-batch as a standard DataFrame
    def process_batch(micro_batch_df, batch_id):
        if micro_batch_df.count() > 0:
            print(f"Processing batch {batch_id} for cache warming with {micro_batch_df.count()} records.")
            
            # Apply the cache-writing logic to each partition
            micro_batch_df.rdd.foreachPartition(_write_batch_to_redis)

    # Start the continuous stream to the Redis cache
    query = (
        gold_df.writeStream
            .option("checkpointLocation", checkpoint_location)
            .foreachBatch(process_batch)
            .start()
    )
    
    print("Redis cache warming stream started. This will run continuously.")
    return query

# --- Example Usage (Databricks Notebook Cell) ---
# gold_table_name = "gold_llm_insights"
# checkpoint_location = "s3://your-bucket/checkpoints/cache_warming"
# redis_config = {"host": "your-redis-host", "port": 6379}
#
# load_to_cache(spark, gold_table_name, checkpoint_location, redis_config)
