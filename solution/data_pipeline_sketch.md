
# Part 3 — Data Pipeline Sketch

Challenge:

> Sketch a simple Databricks pipeline (pipeline.py) that ingests news from a source (RSS feed, JSON API, or webpage), normalizes it, and loads it into a Delta table. Include methods for extract_from_news, transform, and load_to_cache, and provide a Databricks Job JSON to orchestrate it.

## Solution

All the notebooks are located inside the `databricks` folder.

I have implemented a sketch of the following notebooks:

- `ingestion.py`: which is responsible for pulling data from various sources and writing it to the landing zone.
- `normalization.py`: which populates the Bronze and Silver Delta tables. It depends on the successful completion of the ingestion task.
- `enrichment.py`: which orchestrates the parallel LLM agent calls and writes the results to the Gold Delta table. It depends on the normalization task.
- `cache_warning.py`: which streams the final insights from the Gold table to the Redis cache. It depends on the enrichment task.

The command to deploy the jobs from the JSON file using the Databricks CLI is:

```shell
databricks jobs create --json-file databricks/databricks_jobs_decoupled.json
```

This command will create four separate jobs in your Databricks workspace, with each job corresponding to one of the tasks in the JSON array.