"""Bronze layer: raw taxi CSV -> typed Iceberg table (month-partitioned)."""
import dagster as dg
from dagster_docker import PipesDockerClient

from ._common import TAXI_PARTITIONS, run_spark_job, window_env


@dg.asset(
    key=["bronze", "taxi_trips"],
    partitions_def=TAXI_PARTITIONS,
    deps=[dg.AssetKey(["raw", "taxi_trips"])],
    group_name="bronze",
    compute_kind="pyspark",
    description="Typed raw trips in lakehouse.bronze.taxi_trips, partitioned by months(trip_start_timestamp).",
    check_specs=[
        # ERROR (blocking): correctness of the typed load.
        dg.AssetCheckSpec(name="row_count_positive", asset=["bronze", "taxi_trips"], blocking=True),
        dg.AssetCheckSpec(name="trip_start_not_null", asset=["bronze", "taxi_trips"], blocking=True),
        # WARN: soft anomalies on volume / raw reconciliation.
        dg.AssetCheckSpec(name="bronze_matches_raw", asset=["bronze", "taxi_trips"]),
        dg.AssetCheckSpec(name="volume_in_band", asset=["bronze", "taxi_trips"]),
    ],
)
def bronze_taxi_trips(context: dg.AssetExecutionContext, spark_pipes: PipesDockerClient):
    yield from run_spark_job(
        context,
        spark_pipes,
        script="bronze_taxi_ingest.py",
        extra_env=window_env(context),
    ).get_results()
