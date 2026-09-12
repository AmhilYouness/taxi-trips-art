"""Maintenance: Iceberg compaction + manifest rewrite + snapshot expiry."""
import dagster as dg
from dagster_docker import PipesDockerClient

from ._common import run_spark_job


@dg.asset(
    key=["maintenance", "iceberg_optimize"],
    deps=[dg.AssetKey(["gold", "taxi_marts"])],
    group_name="maintenance",
    compute_kind="pyspark",
    description="rewrite_data_files + rewrite_manifests + expire_snapshots across bronze/silver/gold.",
)
def iceberg_optimize(
    context: dg.AssetExecutionContext, spark_pipes: PipesDockerClient
) -> dg.MaterializeResult:
    return run_spark_job(
        context, spark_pipes, script="maintenance.py"
    ).get_materialize_result()
