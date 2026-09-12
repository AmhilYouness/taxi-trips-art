"""Silver layer: cleaned/enriched taxi trips (month-partitioned) + DQ checks."""
import dagster as dg
from dagster_docker import PipesDockerClient

from ._common import TAXI_PARTITIONS, run_spark_job, window_env


@dg.asset(
    key=["silver", "taxi_trips"],
    partitions_def=TAXI_PARTITIONS,
    deps=[dg.AssetKey(["bronze", "taxi_trips"])],
    group_name="silver",
    compute_kind="pyspark",
    description="Cleaned, deduplicated, enriched trips in lakehouse.silver.taxi_trips.",
    check_specs=[
        # ERROR (blocking): correctness / freshness of the cleaned month.
        dg.AssetCheckSpec(name="row_count_positive", asset=["silver", "taxi_trips"], blocking=True),
        dg.AssetCheckSpec(name="trip_id_unique", asset=["silver", "taxi_trips"], blocking=True),
        dg.AssetCheckSpec(name="fare_non_negative", asset=["silver", "taxi_trips"], blocking=True),
        dg.AssetCheckSpec(name="window_completeness", asset=["silver", "taxi_trips"], blocking=True),
        # WARN: soft anomalies (null rate, ranges, volume drift vs bronze).
        dg.AssetCheckSpec(name="pickup_area_null_rate", asset=["silver", "taxi_trips"]),
        dg.AssetCheckSpec(name="speed_in_range", asset=["silver", "taxi_trips"]),
        dg.AssetCheckSpec(name="tip_pct_in_range", asset=["silver", "taxi_trips"]),
        dg.AssetCheckSpec(name="volume_in_band", asset=["silver", "taxi_trips"]),
    ],
)
def silver_taxi_trips(context: dg.AssetExecutionContext, spark_pipes: PipesDockerClient):
    yield from run_spark_job(
        context,
        spark_pipes,
        script="silver_taxi_transform.py",
        extra_env=window_env(context),
    ).get_results()
