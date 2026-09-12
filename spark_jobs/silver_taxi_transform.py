"""Silver: clean, dedupe, and enrich one month of bronze taxi trips.

Reads the month window from lakehouse.bronze.taxi_trips (range filter -> Iceberg
partition pruning), derives analytics-friendly fields, dedupes by trip_id, and
writes lakehouse.silver.taxi_trips via a dynamic partition overwrite. DQ checks
are reported back to Dagster over Pipes.

Env: WINDOW_START, WINDOW_END (ISO month window).
"""
import os
import time
from datetime import datetime

from dagster_pipes import PipesContext, open_dagster_pipes
from pyspark.sql import functions as F

from common.obs import check, timed_step
from common.session import build_spark

# Soft-anomaly thresholds (WARN only) - override per environment if needed.
NULL_PICKUP_AREA_MAX = float(os.environ.get("SILVER_NULL_PICKUP_AREA_MAX", "0.5"))
SPEED_MPH_MAX = float(os.environ.get("SILVER_SPEED_MPH_MAX", "150"))
TIP_PCT_MAX = float(os.environ.get("SILVER_TIP_PCT_MAX", "2.0"))
VOLUME_DRIFT_MAX = float(os.environ.get("SILVER_VOLUME_DRIFT_MAX", "0.1"))

SILVER_COLS = [
    "trip_id", "taxi_id", "trip_start_timestamp", "trip_end_timestamp",
    "trip_date", "trip_hour", "dow_num", "day_name",
    "trip_seconds", "duration_min", "trip_miles", "speed_mph",
    "pickup_community_area", "dropoff_community_area",
    "fare", "tips", "tolls", "extras", "trip_total", "tip_pct",
    "payment_type", "company",
    "pickup_centroid_latitude", "pickup_centroid_longitude",
    "dropoff_centroid_latitude", "dropoff_centroid_longitude",
    "is_airport",
]


def _ensure_table(spark) -> None:
    bucket = os.environ.get("S3_BUCKET", "lakehouse")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS lakehouse.silver.taxi_trips (
            trip_id                      STRING,
            taxi_id                      STRING,
            trip_start_timestamp         TIMESTAMP,
            trip_end_timestamp           TIMESTAMP,
            trip_date                    DATE,
            trip_hour                    INT,
            dow_num                      INT,
            day_name                     STRING,
            trip_seconds                 INT,
            duration_min                 DOUBLE,
            trip_miles                   DOUBLE,
            speed_mph                    DOUBLE,
            pickup_community_area        INT,
            dropoff_community_area       INT,
            fare                         DOUBLE,
            tips                         DOUBLE,
            tolls                        DOUBLE,
            extras                       DOUBLE,
            trip_total                   DOUBLE,
            tip_pct                      DOUBLE,
            payment_type                 STRING,
            company                      STRING,
            pickup_centroid_latitude     DOUBLE,
            pickup_centroid_longitude    DOUBLE,
            dropoff_centroid_latitude    DOUBLE,
            dropoff_centroid_longitude   DOUBLE,
            is_airport                   BOOLEAN
        ) USING iceberg
        PARTITIONED BY (months(trip_start_timestamp))
        LOCATION 's3://{bucket}/silver/taxi_trips'
        TBLPROPERTIES ('write.distribution-mode'='hash', 'format-version'='2')
        """
    )
    # Sort within partitions for data skipping on common analytic predicates.
    spark.sql(
        "ALTER TABLE lakehouse.silver.taxi_trips "
        "WRITE ORDERED BY (trip_start_timestamp, pickup_community_area)"
    )


def main() -> None:
    with open_dagster_pipes():
        ctx = PipesContext.get()
        t0 = time.monotonic()
        spark = build_spark("silver_taxi_trips")
        _ensure_table(spark)

        start = os.environ["WINDOW_START"]
        end = os.environ["WINDOW_END"]
        month = start[:7]
        ctx.log.info(f"silver transform month={month} window=[{start}, {end})")

        with timed_step(ctx, "silver.read_bronze") as info:
            bronze = spark.table("lakehouse.bronze.taxi_trips").filter(
                (F.col("trip_start_timestamp") >= F.to_timestamp(F.lit(start)))
                & (F.col("trip_start_timestamp") < F.to_timestamp(F.lit(end)))
            )
            bronze_rows = bronze.count()
            info["bronze_rows"] = bronze_rows

        with timed_step(ctx, "silver.transform"):
            cleaned = (
                bronze
                .filter(F.col("trip_id").isNotNull())
                .filter(F.col("trip_start_timestamp").isNotNull())
                .dropDuplicates(["trip_id"])
                # Guard against obviously invalid measurements.
                .withColumn("trip_seconds", F.when(F.col("trip_seconds") < 0, None).otherwise(F.col("trip_seconds")))
                .withColumn("trip_miles", F.when(F.col("trip_miles") < 0, None).otherwise(F.col("trip_miles")))
            )

            derived = (
                cleaned
                .withColumn("trip_date", F.to_date("trip_start_timestamp"))
                .withColumn("trip_hour", F.hour("trip_start_timestamp"))
                .withColumn("dow_num", F.dayofweek("trip_start_timestamp"))
                .withColumn("day_name", F.date_format("trip_start_timestamp", "EEE"))
                .withColumn("duration_min", F.round(F.col("trip_seconds") / 60.0, 2))
                .withColumn(
                    "speed_mph",
                    F.when(
                        F.col("trip_seconds") > 0,
                        F.round(F.col("trip_miles") / (F.col("trip_seconds") / 3600.0), 2),
                    ),
                )
                .withColumn(
                    "tip_pct",
                    F.when(F.col("fare") > 0, F.round(F.col("tips") / F.col("fare"), 4)),
                )
                .withColumn(
                    "is_airport",
                    F.col("pickup_community_area").isin(76, 56)
                    | F.col("dropoff_community_area").isin(76, 56),
                )
                .select(*SILVER_COLS)
            )

        with timed_step(ctx, "silver.write") as info:
            derived.writeTo("lakehouse.silver.taxi_trips").overwritePartitions()
            info["written"] = True

        # ---- gather profile stats over the freshly written month (one pass) ----
        with timed_step(ctx, "silver.checks") as info:
            month_df = spark.table("lakehouse.silver.taxi_trips").filter(
                (F.col("trip_start_timestamp") >= F.to_timestamp(F.lit(start)))
                & (F.col("trip_start_timestamp") < F.to_timestamp(F.lit(end)))
            )
            agg = month_df.agg(
                F.count(F.lit(1)).alias("rows"),
                F.countDistinct("trip_id").alias("distinct_ids"),
                F.min("fare").alias("min_fare"),
                F.min("trip_start_timestamp").alias("min_ts"),
                F.max("trip_start_timestamp").alias("max_ts"),
                F.sum(F.when(F.col("pickup_community_area").isNull(), 1).otherwise(0)).alias("null_pickup"),
                F.max("speed_mph").alias("max_speed"),
                F.max("tip_pct").alias("max_tip_pct"),
                F.min("tip_pct").alias("min_tip_pct"),
            ).collect()[0]

            rows = int(agg["rows"])
            distinct_ids = int(agg["distinct_ids"])
            min_fare = agg["min_fare"]
            null_pickup_rate = (int(agg["null_pickup"]) / rows) if rows else 0.0
            max_speed = agg["max_speed"]
            max_tip_pct = agg["max_tip_pct"]
            min_tip_pct = agg["min_tip_pct"]
            info["rows"] = rows

            duration = round(time.monotonic() - t0, 1)
            ctx.log.info(f"silver.taxi_trips month={month} rows={rows} (bronze={bronze_rows}) in {duration}s")
            ctx.report_asset_materialization(
                metadata={
                    "month": month,
                    "rows": rows,
                    "bronze_rows": bronze_rows,
                    "distinct_trip_ids": distinct_ids,
                    "null_pickup_area_rate": round(null_pickup_rate, 4),
                    "max_speed_mph": float(max_speed) if max_speed is not None else 0.0,
                    "max_tip_pct": float(max_tip_pct) if max_tip_pct is not None else 0.0,
                    "duration_seconds": duration,
                },
                data_version=f"{month}:{rows}",
            )

            # ---- ERROR (blocking): correctness ----
            check(ctx, "row_count_positive", passed=rows > 0,
                  severity="ERROR", metadata={"rows": rows})
            check(ctx, "trip_id_unique", passed=(distinct_ids == rows),
                  severity="ERROR",
                  metadata={"distinct_trip_ids": distinct_ids, "rows": rows})
            check(ctx, "fare_non_negative",
                  passed=(min_fare is None or float(min_fare) >= 0),
                  severity="ERROR",
                  metadata={"min_fare": float(min_fare) if min_fare is not None else -1.0})
            # Data freshness for a frozen dataset = the month's data covers the
            # window. Compare real datetimes (Spark renders TIMESTAMP with a
            # space separator, so lexical vs the ISO 'T' window would be wrong).
            start_dt = datetime.fromisoformat(start)
            end_dt = datetime.fromisoformat(end)
            min_ts, max_ts = agg["min_ts"], agg["max_ts"]
            in_window = (
                rows == 0
                or (min_ts is not None and min_ts >= start_dt and max_ts < end_dt)
            )
            check(ctx, "window_completeness", passed=in_window,
                  severity="ERROR",
                  metadata={"min_ts": str(min_ts), "max_ts": str(max_ts),
                            "window_start": start, "window_end": end})

            # ---- WARN (soft anomalies) ----
            check(ctx, "pickup_area_null_rate", passed=(null_pickup_rate <= NULL_PICKUP_AREA_MAX),
                  severity="WARN",
                  metadata={"null_rate": round(null_pickup_rate, 4), "threshold": NULL_PICKUP_AREA_MAX})
            check(ctx, "speed_in_range",
                  passed=(max_speed is None or float(max_speed) <= SPEED_MPH_MAX),
                  severity="WARN",
                  metadata={"max_speed_mph": float(max_speed) if max_speed is not None else 0.0,
                            "threshold": SPEED_MPH_MAX})
            tip_ok = (max_tip_pct is None) or (float(max_tip_pct) <= TIP_PCT_MAX and float(min_tip_pct) >= 0)
            check(ctx, "tip_pct_in_range", passed=tip_ok,
                  severity="WARN",
                  metadata={"max_tip_pct": float(max_tip_pct) if max_tip_pct is not None else 0.0,
                            "min_tip_pct": float(min_tip_pct) if min_tip_pct is not None else 0.0,
                            "threshold": TIP_PCT_MAX})
            drift = abs(bronze_rows - rows) / bronze_rows if bronze_rows else (0.0 if rows == 0 else 1.0)
            check(ctx, "volume_in_band", passed=(drift <= VOLUME_DRIFT_MAX),
                  severity="WARN",
                  metadata={"silver_rows": rows, "bronze_rows": bronze_rows,
                            "drift_pct": round(drift * 100, 2), "threshold_pct": VOLUME_DRIFT_MAX * 100})
        spark.stop()


if __name__ == "__main__":
    main()
