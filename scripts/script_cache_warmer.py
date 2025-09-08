import os
import pandas as pd
import redis
import json
from datetime import datetime

# Environment variables
REDIS_HOST = os.environ.get('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.environ.get('REDIS_PORT', 6379))
REDIS_DB = int(os.environ.get('REDIS_DB', 0))

# Path to the data file
DATA_FILE_PATH = "data/df_events.parquet"

# Connect to Redis
try:
    r = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
    r.ping()
    print("Successfully connected to Redis.")
except redis.exceptions.ConnectionError as e:
    print(f"Error connecting to Redis: {e}")
    exit(1)

def load_data_to_redis(df: pd.DataFrame, redis_client: redis.StrictRedis):
    """
    Loads pre-computed features from a DataFrame into a Redis cache.
    The cache is keyed by '{asset_ticker}:{event_timestamp}'.
    """
    pipe = redis_client.pipeline()
    latest_event_timestamps = {}

    for index, row in df.iterrows():
        # Create a rich set of pre-computed features to 
        #
        # NOTE:
        # sentiment_score, key_market_drivers, risk_score
        # are not included for now since df_events.parquet
        # does not have these columns.
        # They can be added later when available.
        features = {
            "signal": row.get("signal"),
            "conviction": row.get("conviction"),
            "causality": row.get("causality"),
            # "sentiment_score": row.get("sentiment_score"),
            # "key_market_drivers": row.get("key_market_drivers"),
            # "risk_score": row.get("risk_score"),
            "event_timestamp": row.get("datetime_est")
        }

        # Extract asset_ticker and event_timestamp
        #
        # NOTE:
        # - asset_ticker is expected to be a string.
        #   - df_events.parquet does not have this column, it can be added later.
        #     For now, we place it by hand and assume all rows belong to the same asset: BTC.
        # - event_timestamp is expected to be in UNIX epoch format (int).
        #   - df_events.parquet has a datetime_est column with format %Y-%m-%d %H:%M:%S
        #     we must convert it to UNIX epoch.
        asset_ticker = "BTC"
        # asset_ticker = row.get("asset_ticker")
        event_timestamp = row.get("datetime_est")
        dt = datetime.strptime(event_timestamp, "%Y-%m-%d %H:%M:%S")
        event_timestamp = int(dt.timestamp())
        
        # Check if asset_ticker and event_timestamp are not None
        if asset_ticker is None or event_timestamp is None:
            print(f"Skipping row with missing asset_ticker or event_timestamp: {row.to_dict()}")
            continue

        # Composite key: {asset_ticker}:{event_timestamp}
        composite_key = f"{asset_ticker}:{event_timestamp}"

        # Write the features as a JSON string
        pipe.set(composite_key, json.dumps(features))

        # Update the latest event timestamp for the asset
        if asset_ticker not in latest_event_timestamps or event_timestamp > latest_event_timestamps[asset_ticker]:
            latest_event_timestamps[asset_ticker] = event_timestamp

    # Execute the pipeline to perform all SET operations in a single round-trip
    pipe.execute()
    print(f"Loaded {len(df)} records into Redis.")

    # Update the latest_event key for each asset
    pipe_latest = redis_client.pipeline()
    for asset, timestamp in latest_event_timestamps.items():
        latest_key = f"latest_event:{asset}"
        pipe_latest.set(latest_key, timestamp)
    pipe_latest.execute()
    print("Updated latest event timestamps in Redis.")

def main():
    """Main function to load data from Parquet and warm the Redis cache."""
    try:
        print(f"Reading data from {DATA_FILE_PATH}...")
        df = pd.read_parquet(DATA_FILE_PATH)
        print(f"Read {len(df)} records from the Parquet file.")
        
        load_data_to_redis(df, r)

    except FileNotFoundError:
        print(f"Error: The file {DATA_FILE_PATH} was not found.")
        exit(1)
    except Exception as e:
        print(f"An error occurred: {e}")
        exit(1)

if __name__ == "__main__":
    main()
