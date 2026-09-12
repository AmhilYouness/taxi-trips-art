"""Raw layer: standalone extractor landing taxi CSV in MinIO (upstream of bronze)."""
import dagster as dg
from dagster_docker import PipesDockerClient

from ._common import TAXI_PARTITIONS, run_spark_job, window_env


@dg.asset(
    key=["raw", "taxi_trips"],
    partitions_def=TAXI_PARTITIONS,
    group_name="raw",
    compute_kind="python",
    description="Raw taxi CSV pulled from the SODA API into s3://lakehouse/raw/taxi/month=YYYY-MM/.",
    check_specs=[
        # ERROR (blocking): no rows landed means the extract is broken.
        dg.AssetCheckSpec(name="rows_landed_positive", asset=["raw", "taxi_trips"], blocking=True),
        # WARN: monthly volume outside the expected band (soft anomaly).
        dg.AssetCheckSpec(name="volume_in_band", asset=["raw", "taxi_trips"]),
    ],
)
def raw_taxi_trips(context: dg.AssetExecutionContext, spark_pipes: PipesDockerClient):
    yield from run_spark_job(
        context,
        spark_pipes,
        script="extract_taxi_raw.py",
        extra_env=window_env(context),
        python_entrypoint=True,
    ).get_results()
