"""Shared config + helper for launching Spark jobs in Docker via Dagster Pipes."""
import os
from pathlib import Path

import dagster as dg
from dagster_docker import PipesDockerClient
from dotenv import load_dotenv

IMAGE = "arte-spark:4.1.1"
NETWORK = "iceberg_net"  # matches the network name in docker-compose.yml

# Repo root: this file is orchestration/arte_dagster/assets/_common.py -> up 3
REPO_ROOT = Path(__file__).resolve().parents[3]
DATA_DIR = str(REPO_ROOT / "data")

# Load the central .env so host-only secrets (e.g. the Socrata app token) flow
# through to the containers we launch.
load_dotenv(REPO_ROOT / ".env")

# Monthly partitions drive both modes: backfill = materialize a range of months,
# delta = a schedule materializing the newest month. end_date is optional and
# exclusive (dataset wrvz-psew is frozen at 2013-2023, so we bound it there).
_end = os.environ.get("TAXI_BACKFILL_END", "").strip() or None
TAXI_PARTITIONS = dg.MonthlyPartitionsDefinition(
    start_date=os.environ.get("TAXI_BACKFILL_START", "2023-10-01"),
    end_date=_end,
)


def window_env(context: "dg.AssetExecutionContext") -> dict:
    """Map the current monthly partition to the [start, end) window env vars."""
    tw = context.partition_time_window
    return {
        "WINDOW_START": tw.start.strftime("%Y-%m-%dT%H:%M:%S"),
        "WINDOW_END": tw.end.strftime("%Y-%m-%dT%H:%M:%S"),
    }

# Connection settings as seen from INSIDE the docker network. Switching to AWS S3
# means changing these values (endpoint/keys/path-style) - no code changes.
CATALOG_ENV = {
    "ICEBERG_CATALOG_URI": "http://lakekeeper:8181/catalog",
    "ICEBERG_WAREHOUSE": "lakehouse",
    "ICEBERG_TOKEN": "dummy",
    "S3_ENDPOINT": "http://minio:9000",
    "S3_KEY": "minioadmin",
    "S3_SECRET": "minioadmin",
    "S3_REGION": "us-east-1",
    "S3_PATH_STYLE": "true",
    "S3_BUCKET": os.environ.get("S3_BUCKET", "lakehouse"),
    # Chicago Taxi source config (app token pulled from the host .env)
    "TAXI_DATASET_ID": os.environ.get("TAXI_DATASET_ID", "wrvz-psew"),
    "TAXI_API_BASE": os.environ.get("TAXI_API_BASE", "https://data.cityofchicago.org/resource"),
    "TAXI_RAW_PREFIX": os.environ.get("TAXI_RAW_PREFIX", "raw/taxi"),
    "CHICAGO_DATA_PORTAL_TOKEN": os.environ.get("CHICAGO_DATA_PORTAL_TOKEN", ""),
}


def run_spark_job(
    context: dg.AssetExecutionContext,
    client: PipesDockerClient,
    script: str,
    extra_env: dict | None = None,
    mount_data: bool = False,
    python_entrypoint: bool = False,
):
    """Launch a job in a container joined to the lakehouse network via Pipes.

    python_entrypoint=True runs the script with plain python3 (for the pure-Python
    extractor); otherwise it runs through spark-submit.
    """
    # NOTE: no auto_remove. dagster-docker's PipesDockerClient calls
    # container.wait()/stop() after the run; auto_remove races with those and
    # raises a 404 the instant the (successful) container exits. Exited
    # containers can be reaped later with `docker container prune`.
    container_kwargs: dict = {"network": NETWORK}
    if mount_data:
        container_kwargs["volumes"] = {DATA_DIR: {"bind": "/data", "mode": "ro"}}
    runner = ["python3"] if python_entrypoint else ["/opt/spark/bin/spark-submit"]
    return client.run(
        image=IMAGE,
        command=[*runner, f"/app/spark_jobs/{script}"],
        env={**CATALOG_ENV, **(extra_env or {})},
        container_kwargs=container_kwargs,
        context=context,
    )
