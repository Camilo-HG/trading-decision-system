# Databricks Slow-Path Pipeline - Bronze and Silver Layers
# This notebook is designed to run in two separate cells within a Databricks environment.

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import json
import dlt

# --- Step 2: Populate the Bronze Layer ---

# Define the schema for the raw data files in the Landing Zone.
# Auto Loader can infer the schema, but defining it is often more robust.
bronze_schema = StructType([
    StructField("content", StringType(), True),
    StructField("source_url", StringType(), True),
    StructField("source_type", StringType(), True),
    StructField("fetch_timestamp", TimestampType(), True)
])

def populate_bronze_layer(spark, landing_zone_path: str, bronze_table_name: str):
    """
    Reads from the Landing Zone using Structured Streaming and Auto Loader
    and populates a durable, versioned Bronze Delta Table.
    
    Args:
        spark: The PySpark session object.
        landing_zone_path (str): The cloud storage path for the raw files.
        bronze_table_name (str): The name of the Bronze Delta table.
    """
    print(f"Reading from landing zone {landing_zone_path} and populating {bronze_table_name}...")
    
    # Configure and start the Structured Streaming job with Auto Loader.
    # We use cloudFiles.format("binaryFile") to read the raw file content.
    streaming_df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .option("cloudFiles.schemaLocation", f"{landing_zone_path}/schema-location")
        .load(landing_zone_path)
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )
    
    # Write the streaming data to the Bronze Delta table.
    # The 'checkpointLocation' option is crucial for fault tolerance.
    # We use 'append' mode to add new data as it arrives.
    query = (
        streaming_df.writeStream.format("delta")
        .option("checkpointLocation", f"{landing_zone_path}/checkpoint-bronze")
        .outputMode("append")
        .toTable(bronze_table_name)
    )
    
    # Start the stream and wait for it to be ready.
    print("Bronze layer population stream started.")
    return query

# --- Step 3: Normalization into a Silver Layer (Declarative Pipeline) ---

# This step is now implemented with Delta Live Tables (DLT)
# using a declarative syntax. This code is designed to be part of a DLT pipeline.

@dlt.table(
    name="silver_normalized_news",
    comment="Clean, normalized news data ready for LLM calls."
)
def transform():
    """
    Applies normalization to the raw data from the Bronze layer and
    returns a DataFrame for the Silver Delta Table.
    """
    print("Normalizing data from the bronze layer...")

    # A DLT pipeline reads directly from the source table.
    bronze_df = dlt.read("bronze_news_data")

    # Conceptual normalization logic:
    # We would parse the raw content based on the source_type.
    # This is a highly simplified example.
    silver_df = (
        bronze_df
        .withColumn("parsed_content", F.when(F.col("source_type") == "json_api", F.from_json(F.col("content"), bronze_schema))
                                     .when(F.col("source_type") == "rss", F.col("content"))
                                     .when(F.col("source_type") == "webpage", F.col("content"))
                                     .otherwise(None)
        )
        .withColumn("event_timestamp", F.current_timestamp())
        # Add a placeholder for a normalized asset ticker, which would be
        # determined by parsing the content.
        .withColumn("normalized_asset", F.lit("BTC")) 
    )

    return silver_df

# Example usage within a Databricks notebook:

# This cell would start the Bronze pipeline
# landing_zone_path = "s3://your-bucket/landing-zone"
# bronze_table_name = "bronze_news_data"
# populate_bronze_layer(spark, landing_zone_path, bronze_table_name)

# A DLT pipeline is a separate artifact. To run the 'transform' function,
# you would define it in a DLT pipeline configuration and start that pipeline.
# No explicit call is needed in the notebook for this DLT table.
# Note: The above code assumes that the necessary configurations for Auto Loader
# and Delta Live Tables are already set up in your Databricks environment.