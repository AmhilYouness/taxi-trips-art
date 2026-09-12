"""Gold: taxi analytics marts consumed by the DuckDB/FastAPI BI layer.

Full recompute from lakehouse.silver.taxi_trips (cheap at the default scope):
  gold.trips_daily          day-level KPIs
  gold.trips_by_hour        hour x weekday demand heatmap
  gold.trips_by_company     operator leaderboard
  gold.payment_mix          payment-type share
  gold.top_routes           busiest pickup->dropoff pairs (named)
  gold.trips_by_pickup_area pickup community-area ranking (named)

Community-area names/airport flags come from /data/community_areas.csv (mounted).
"""
import os
import time

from dagster_pipes import PipesContext, open_dagster_pipes
from pyspark.sql import functions as F

from common.obs import check, timed_step
from common.session import build_spark

AREAS_CSV = "/data/community_areas.csv"

# Gold revenue should reconcile with silver within this tolerance (rounding).
REVENUE_TOLERANCE = float(os.environ.get("GOLD_REVENUE_TOLERANCE", "0.01"))


def write_mart(ctx, spark, df, name: str, bucket: str) -> None:
    """Write a mart as gold/<name>/ in the bucket via CTAS with explicit LOCATION."""
    with timed_step(ctx, f"gold.write.{name}"):
        view = f"_gold_{name}"
        df.createOrReplaceTempView(view)
        spark.sql(
            f"CREATE OR REPLACE TABLE lakehouse.gold.{name} "
            f"USING iceberg LOCATION 's3://{bucket}/gold/{name}' "
            f"AS SELECT * FROM {view}"
        )


def main() -> None:
    with open_dagster_pipes():
        ctx = PipesContext.get()
        t0 = time.monotonic()
        spark = build_spark("gold_taxi")
        bucket = os.environ.get("S3_BUCKET", "lakehouse")
        spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")
        ctx.log.info("gold aggregate: rebuilding marts from lakehouse.silver.taxi_trips")

        silver = spark.table("lakehouse.silver.taxi_trips")
        areas = (
            spark.read.option("header", "true").csv(AREAS_CSV)
            .select(
                F.col("community_area").cast("int").alias("area_no"),
                F.col("area_name"),
                (F.col("is_airport").cast("int") == 1).alias("area_airport"),
            )
        )

        # ---- trips_daily ----
        trips_daily = (
            silver.groupBy(F.col("trip_date").alias("day"))
            .agg(
                F.count("*").alias("trips"),
                F.round(F.sum("trip_total"), 2).alias("revenue"),
                F.round(F.avg("fare"), 2).alias("avg_fare"),
                F.round(F.avg("tip_pct"), 4).alias("avg_tip_pct"),
                F.round(F.avg("trip_miles"), 2).alias("avg_miles"),
                F.round(F.avg("duration_min"), 2).alias("avg_duration_min"),
            )
            .orderBy("day")
        )
        write_mart(ctx, spark, trips_daily, "trips_daily", bucket)

        # ---- trips_by_hour (hour x weekday) ----
        trips_by_hour = (
            silver.groupBy("dow_num", "day_name", "trip_hour")
            .agg(
                F.count("*").alias("trips"),
                F.round(F.avg("fare"), 2).alias("avg_fare"),
            )
            .orderBy("dow_num", "trip_hour")
        )
        write_mart(ctx, spark, trips_by_hour, "trips_by_hour", bucket)

        # ---- trips_by_company ----
        trips_by_company = (
            silver.groupBy(F.coalesce(F.col("company"), F.lit("Unknown")).alias("company"))
            .agg(
                F.count("*").alias("trips"),
                F.round(F.sum("trip_total"), 2).alias("revenue"),
                F.round(F.avg("fare"), 2).alias("avg_fare"),
            )
            .orderBy(F.col("trips").desc())
        )
        write_mart(ctx, spark, trips_by_company, "trips_by_company", bucket)

        # ---- payment_mix ----
        total_trips = silver.count()
        payment_mix = (
            silver.groupBy(F.coalesce(F.col("payment_type"), F.lit("Unknown")).alias("payment_type"))
            .agg(
                F.count("*").alias("trips"),
                F.round(F.sum("trip_total"), 2).alias("revenue"),
            )
            .withColumn(
                "pct_of_trips",
                F.round(F.col("trips") / F.lit(max(total_trips, 1)), 4),
            )
            .orderBy(F.col("trips").desc())
        )
        write_mart(ctx, spark, payment_mix, "payment_mix", bucket)

        # ---- top_routes (named) ----
        routes = (
            silver.filter(
                F.col("pickup_community_area").isNotNull()
                & F.col("dropoff_community_area").isNotNull()
            )
            .groupBy("pickup_community_area", "dropoff_community_area")
            .agg(
                F.count("*").alias("trips"),
                F.round(F.avg("fare"), 2).alias("avg_fare"),
                F.round(F.avg("trip_miles"), 2).alias("avg_miles"),
            )
            .orderBy(F.col("trips").desc())
            .limit(50)
        )
        pu = areas.select(
            F.col("area_no").alias("pu_no"), F.col("area_name").alias("pickup_area")
        )
        do = areas.select(
            F.col("area_no").alias("do_no"), F.col("area_name").alias("dropoff_area")
        )
        top_routes = (
            routes
            .join(F.broadcast(pu), routes.pickup_community_area == pu.pu_no, "left")
            .join(F.broadcast(do), routes.dropoff_community_area == do.do_no, "left")
            .select(
                "pickup_community_area", "pickup_area",
                "dropoff_community_area", "dropoff_area",
                "trips", "avg_fare", "avg_miles",
            )
            .orderBy(F.col("trips").desc())
        )
        write_mart(ctx, spark, top_routes, "top_routes", bucket)

        # ---- trips_by_pickup_area (named) ----
        by_pickup = (
            silver.filter(F.col("pickup_community_area").isNotNull())
            .groupBy("pickup_community_area")
            .agg(
                F.count("*").alias("trips"),
                F.round(F.sum("trip_total"), 2).alias("revenue"),
                F.round(F.avg("fare"), 2).alias("avg_fare"),
            )
        )
        trips_by_pickup_area = (
            by_pickup
            .join(F.broadcast(areas), by_pickup.pickup_community_area == areas.area_no, "left")
            .select(
                "pickup_community_area",
                F.col("area_name"),
                F.col("area_airport").alias("is_airport"),
                "trips", "revenue", "avg_fare",
            )
            .orderBy(F.col("trips").desc())
        )
        write_mart(ctx, spark, trips_by_pickup_area, "trips_by_pickup_area", bucket)

        # ---- report + per-step DQ + volume tests ----
        with timed_step(ctx, "gold.checks") as info:
            daily = spark.table("lakehouse.gold.trips_daily")
            daily_agg = daily.agg(
                F.count(F.lit(1)).alias("rows"),
                F.min("revenue").alias("min_rev"),
                F.sum("revenue").alias("gold_revenue"),
                F.sum("trips").alias("gold_trips"),
            ).collect()[0]
            daily_rows = int(daily_agg["rows"])
            min_rev = daily_agg["min_rev"]
            gold_revenue = float(daily_agg["gold_revenue"] or 0.0)
            gold_trips = int(daily_agg["gold_trips"] or 0)

            silver_revenue = float(
                silver.agg(F.round(F.sum("trip_total"), 2).alias("rev")).collect()[0]["rev"] or 0.0
            )

            # per-mart row counts (for *_non_empty checks + metadata)
            mart_counts = {
                "trips_daily": daily_rows,
                "trips_by_hour": spark.table("lakehouse.gold.trips_by_hour").count(),
                "trips_by_company": spark.table("lakehouse.gold.trips_by_company").count(),
                "payment_mix": spark.table("lakehouse.gold.payment_mix").count(),
                "top_routes": spark.table("lakehouse.gold.top_routes").count(),
                "trips_by_pickup_area": spark.table("lakehouse.gold.trips_by_pickup_area").count(),
            }
            info["marts"] = len(mart_counts)

            duration = round(time.monotonic() - t0, 1)
            ctx.log.info(
                f"gold marts built; trips_daily rows={daily_rows}, total_trips={total_trips}, "
                f"gold_revenue={gold_revenue} silver_revenue={silver_revenue} in {duration}s"
            )
            ctx.report_asset_materialization(
                metadata={
                    "total_trips": total_trips,
                    "gold_revenue": gold_revenue,
                    "silver_revenue": silver_revenue,
                    "duration_seconds": duration,
                    **{f"rows_{name}": n for name, n in mart_counts.items()},
                },
                data_version=str(total_trips),
            )

            # ---- ERROR (blocking): trips_daily must exist + revenue must reconcile ----
            check(ctx, "trips_daily_non_empty", passed=daily_rows > 0,
                  severity="ERROR", metadata={"rows": daily_rows})
            check(ctx, "revenue_non_negative",
                  passed=(min_rev is not None and float(min_rev) >= 0),
                  severity="ERROR",
                  metadata={"min_revenue": float(min_rev) if min_rev is not None else -1.0})
            rev_diff = abs(gold_revenue - silver_revenue)
            rev_ok = silver_revenue == 0 or (rev_diff / silver_revenue) <= REVENUE_TOLERANCE
            check(ctx, "revenue_reconciles_silver", passed=rev_ok,
                  severity="ERROR",
                  metadata={"gold_revenue": gold_revenue, "silver_revenue": silver_revenue,
                            "abs_diff": round(rev_diff, 2), "tolerance": REVENUE_TOLERANCE})

            # ---- WARN: remaining marts non-empty + trip-count reconciliation ----
            for name in ("trips_by_hour", "trips_by_company", "payment_mix",
                         "top_routes", "trips_by_pickup_area"):
                check(ctx, f"{name}_non_empty", passed=mart_counts[name] > 0,
                      severity="WARN", metadata={"rows": mart_counts[name]})
            check(ctx, "volume_in_band", passed=(gold_trips == total_trips),
                  severity="WARN",
                  metadata={"gold_trips": gold_trips, "silver_trips": total_trips})
        spark.stop()


if __name__ == "__main__":
    main()
