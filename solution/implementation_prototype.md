### Part 2 — Implementation Prototype

Challenge:

> Implement a simplified prototype (Docker Compose with 2 services):
>  
> - **Redis** — cache for pre-computed features or agent outputs.  (Find them in the data folder)
> - **Trading API** — FastAPI service that:  
>   - `POST /decide` returns a trading decision in <500ms.  
>   - Uses cached/pre-computed values (simulate LLM outputs or use the ones in the data folder).
>   - Use prices data from the folder as aditional indicators or to make hybrid vectors.
>   - Includes latency, model version, cache hit/miss, fallback info.
> 
> **Infrastructure Requirements**
> - Docker Compose with environment configs (dev/prod).  
> - Health checks & resource limits.  
> - Structured logging & basic monitoring (`/metrics` in Prometheus format).
>  
> **Acceptance Criteria**
> - `docker-compose up --build` works.  
> - P95 latency <500ms (simulate 30 requests).  
> - Cache hit rate ≥80%.  
> - No unhandled 500s.

## Solution

### Project structure

The project has the following files:

- `.env`:
  - A file to manage environment-specific configurations.

> Indeed, what we have is a `.env.example` file. It has to be renamed to `.env` and the real environment configurations must me set.

- `pyproject.toml`
  - The Python project file.
  - It includes the necessary dependencies for the project
  - The project is managed with `uv`

- `uv.lock`
  - Lock file created by `uv`.
  - It enables:
    - **Reproducibility**
      - Committing `uv.lock` ensures that everyone working on the project, including CI/CD pipelines and other developers, uses the exact same versions of dependencies. This eliminates "it works on my machine" issues and provides consistent environments.
    - **Auditability**
      - The lock file precisely records the resolved dependencies, including their versions and hashes, which is crucial for security audits and understanding the project's dependency graph.
    - **Faster Installations**
      - When `uv.lock` is present, `uv` can directly install the locked dependencies without needing to perform a full dependency resolution, leading to faster environment setups.

- **Docker Compose**

  - `docker/docker-compose.yml`:
    - It orchestrates the services: `redis`, `cache-warmer`, `fastapi` and `prometheus`.
    - It includes a `healthcheck` for all services to ensure they are ready before downstream services start.
    - It also has a `deploy` section on the FastAPI service to set resource limits.
    - The `prometheus` service is added to scrape the metrics endpoint from the FastAPI service.

- **Redis service**

  - `scripts/script_cache_warming.py`:
    - A Python script to populate Redis cache.
    - The Python script will read a parquet file containing sample pre-computed features and load them into Redis.
  
  -  `docker/Dockerfile.cache-warmer`:
     - This Dockerfile is specifically for the cache-warmer service.
     - Its logic is minimal as it only installs the dependencies needed to read the parquet file and write to Redis.
     -  Since the `redis:7.2.5-alpine3.19` image is based on Alpine Linux and is a minimal image that only contains the Redis server and its necessary dependencies. It does not include Python. This is why the `docker-compose.yml` file needs a separate cache-warmer service with its own Dockerfile to install Python and the required libraries to the your main.py script. The Redis image is kept small and focused, which is a key advantage of using official images.


- **FastAPI Service**

  - `docker/Dockerfile.fastapi`:
    - This Dockerfile is for the FastAPI service.
    - It installs a different set of dependencies for the web application and also handles the entrypoint for the `uvicorn` server.
    - It includes instructions for health checks and exposes the port used by the FastAPI server:
      - `HEALTHCHECK` instruction
      - `EXPOSE` for the application port.

  - `src/main.py`:
    - It contains the FastAPI application.
    - It includes the `/decide` and `/metrics` endpoints, Redis integration, and structured logging.

- **Prometheus Service**

  - `docker/prometheus.yml`: A configuration file for the Prometheus service.
  - `src/logging.py`: A file to set up structured logging with `loguru`.


### How to run the prototype

To run the entire project, first navigate to the docker directory from your terminal:

```shell
cd trading_api/docker
```

Once inside the docker directory, run the following command to build the images and start the containers in the background:

```shell
docker-compose up --build -d
```

Then, Docker Compose will perform the following steps:

- **Start Redis.** It will pull the redis:7.2.5-alpine3.19 image from Docker Hub and start the Redis container.

- **Build cache-warmer**. It will build the image for the cache-warmer service, install dependencies using UV, and then run the script_cache_warmer.py script. This service will run, populate the Redis cache, and then exit.

- **Build FastAPI.** It will build the image for the FastAPI application, install dependencies, and start the Uvicorn server.

- **Start Prometheus.** It will pull the prom/prometheus:v2.53.0 image and start the Prometheus server, configured to scrape metrics from the FastAPI service.

After the command completes, one can check the status of the services by running:

```shell
docker-compose ps
````

The FastAPI service will be accessible at http://localhost:8000, and the Prometheus dashboard will be available at http://localhost:9090.





