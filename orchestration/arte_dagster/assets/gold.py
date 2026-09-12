"""Gold layer: taxi analytics marts (full recompute from silver) + DQ checks."""
import dagster as dg
from dagster_docker import PipesDockerClient

from ._common import run_spark_job


@dg.asset(
    key=["gold", "taxi_marts"],
    deps=[dg.AssetKey(["silver", "taxi_trips"])],
    group_name="gold",
    compute_kind="pyspark",
    description="Analytics marts: trips_daily, trips_by_hour, trips_by_company, payment_mix, top_routes, trips_by_pickup_area.",
    check_specs=[
        # ERROR (blocking): core mart present + revenue correct and reconciled.
        dg.AssetCheckSpec(name="trips_daily_non_empty", asset=["gold", "taxi_marts"], blocking=True),
        dg.AssetCheckSpec(name="revenue_non_negative", asset=["gold", "taxi_marts"], blocking=True),
        dg.AssetCheckSpec(name="revenue_reconciles_silver", asset=["gold", "taxi_marts"], blocking=True),
        # WARN: the remaining marts should be non-empty + trip counts reconcile.
        dg.AssetCheckSpec(name="trips_by_hour_non_empty", asset=["gold", "taxi_marts"]),
        dg.AssetCheckSpec(name="trips_by_company_non_empty", asset=["gold", "taxi_marts"]),
        dg.AssetCheckSpec(name="payment_mix_non_empty", asset=["gold", "taxi_marts"]),
        dg.AssetCheckSpec(name="top_routes_non_empty", asset=["gold", "taxi_marts"]),
        dg.AssetCheckSpec(name="trips_by_pickup_area_non_empty", asset=["gold", "taxi_marts"]),
        dg.AssetCheckSpec(name="volume_in_band", asset=["gold", "taxi_marts"]),
    ],
)
def gold_taxi_marts(context: dg.AssetExecutionContext, spark_pipes: PipesDockerClient):
    # mount_data=True exposes /data/community_areas.csv for name/airport enrichment.
    yield from run_spark_job(
        context,
        spark_pipes,
        script="gold_taxi_aggregate.py",
        mount_data=True,
    ).get_results()
