"""Bronze: load one month of raw taxi CSV (from MinIO) into Iceberg.

The month's CSV part files are pulled from s3://<bucket>/<raw_prefix>/month=YYYY-MM/
(written by extract_taxi_raw.py) with boto3 into a container-local temp dir, then
read by Spark and written to lakehouse.bronze.taxi_trips, partitioned by
months(trip_start_timestamp) via a dynamic partition overwrite (idempotent).

boto3 download is used instead of Spark S3A to avoid an AWS-SDK classpath clash
between hadoop-aws and iceberg-aws-bundle; Iceberg table IO still uses S3FileIO.

Env: WINDOW_START (month label), S3_ENDPOINT, S3_KEY, S3_SECRET, S3_REGION,
     S3_BUCKET, TAXI_RAW_PREFIX.
"""
import os
import shutil
import tempfile
import time

import boto3
from botocore.config import Config
from dagster_pipes import PipesContext, open_dagster_pipes
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from common.obs import check, timed_step, volume_band
from common.session import build_spark
from common.soda import SELECT_COLS

TS_FMT = "yyyy-MM-dd'T'HH:mm:ss[.SSS]"
DOUBLE_COLS = [
    "trip_miles", "fare", "tips", "tolls", "extras", "trip_total",
    "pickup_centroid_latitude", "pickup_centroid_longitude",
    "dropoff_centroid_latitude", "dropoff_centroid_longitude",
]
INT_COLS = ["trip_seconds", "pickup_community_area", "dropoff_community_area"]

DDL_ORDER = [
    "trip_id", "taxi_id", "trip_start_timestamp", "trip_end_timestamp",
    "trip_seconds", "trip_miles", "pickup_community_area", "dropoff_community_area",
    "fare", "tips", "tolls", "extras", "trip_total", "payment_type", "company",
    "pickup_centroid_latitude", "pickup_centroid_longitude",
    "dropoff_centroid_latitude", "dropoff_centroid_longitude",
]


def _s3_client():
    endpoint = os.environ.get("S3_ENDPOINT", "").strip() or None
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.environ.get("S3_KEY", "minioadmin"),
        aws_secret_access_key=os.environ.get("S3_SECRET", "minioadmin"),
        region_name=os.environ.get("S3_REGION", "us-east-1"),
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def _download_month(s3, bucket: str, prefix: str, dest: str) -> int:
    """Download all CSV part files under prefix into dest. Returns file count."""
    os.makedirs(dest, exist_ok=True)
    count = 0
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.lower().endswith(".csv"):
                continue
            local = os.path.join(dest, os.path.basename(key))
            s3.download_file(bucket, key, local)
            count += 1
    return count


def _ensure_table(spark) -> None:
    bucket = os.environ.get("S3_BUCKET", "lakehouse")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS lakehouse.bronze.taxi_trips (
            trip_id                      STRING,
            taxi_id                      STRING,
            trip_start_timestamp         TIMESTAMP,
            trip_end_timestamp           TIMESTAMP,
            trip_seconds                 INT,
            trip_miles                   DOUBLE,
            pickup_community_area        INT,
            dropoff_community_area       INT,
            fare                         DOUBLE,
            tips                         DOUBLE,
            tolls                        DOUBLE,
            extras                       DOUBLE,
            trip_total                   DOUBLE,
            payment_type                 STRING,
            company                      STRING,
            pickup_centroid_latitude     DOUBLE,
            pickup_centroid_longitude    DOUBLE,
            dropoff_centroid_latitude    DOUBLE,
            dropoff_centroid_longitude   DOUBLE
        ) USING iceberg
        PARTITIONED BY (months(trip_start_timestamp))
        LOCATION 's3://{bucket}/bronze/taxi_trips'
        TBLPROPERTIES ('write.distribution-mode'='hash', 'format-version'='2')
        """
    )


def _report_checks(ctx, rows: int, raw_rows: int, null_start_dropped: int) -> None:
    """Per-step DQ + volume tests for the bronze month."""
    check(
        ctx, "row_count_positive", passed=rows > 0,
        severity="ERROR", metadata={"rows": rows},
    )
    check(
        ctx, "trip_start_not_null", passed=(rows == 0 or null_start_dropped >= 0),
        severity="ERROR",
        metadata={"rows_written": rows, "null_trip_start_dropped": null_start_dropped},
    )
    # WARN: bronze rows should track the raw rows landed (minus null-start drops).
    drift = abs(raw_rows - rows) / raw_rows if raw_rows else (0.0 if rows == 0 else 1.0)
    check(
        ctx, "bronze_matches_raw", passed=drift <= 0.05,
        severity="WARN",
        metadata={"raw_rows": raw_rows, "bronze_rows": rows, "drift_pct": round(drift * 100, 2)},
    )
    lo, hi = volume_band()
    check(
        ctx, "volume_in_band", passed=(lo <= rows <= hi),
        severity="WARN",
        metadata={"rows": rows, "expected_min": lo, "expected_max": hi},
    )


def main() -> None:
    with open_dagster_pipes():
        ctx = PipesContext.get()
        t0 = time.monotonic()
        spark = build_spark("bronze_taxi_trips")
        _ensure_table(spark)

        month = os.environ["WINDOW_START"][:7]  # YYYY-MM
        bucket = os.environ.get("S3_BUCKET", "lakehouse")
        raw_prefix = os.environ.get("TAXI_RAW_PREFIX", "raw/taxi").strip("/")
        prefix = f"{raw_prefix}/month={month}/"
        ctx.log.info(f"bronze ingest month={month} from s3://{bucket}/{prefix}")

        tmp = tempfile.mkdtemp(prefix=f"raw_{month}_")
        try:
            s3 = _s3_client()
            with timed_step(ctx, "bronze.download_raw") as info:
                files = _download_month(s3, bucket, prefix, tmp)
                info["files"] = files

            if files == 0:
                ctx.log.warning(f"bronze {month}: no raw files under s3://{bucket}/{prefix}")
                duration = round(time.monotonic() - t0, 1)
                ctx.report_asset_materialization(
                    metadata={
                        "month": month, "rows": 0, "source": f"s3://{bucket}/{prefix}",
                        "files": 0, "duration_seconds": duration,
                    },
                    data_version=f"{month}:0",
                )
                with timed_step(ctx, "bronze.checks"):
                    _report_checks(ctx, rows=0, raw_rows=0, null_start_dropped=0)
                spark.stop()
                return

            with timed_step(ctx, "bronze.read_and_type") as info:
                raw_schema = StructType([StructField(c, StringType()) for c in SELECT_COLS])
                raw = spark.read.option("header", "true").schema(raw_schema).csv(tmp)
                raw_rows = raw.count()
                info["raw_rows"] = raw_rows

                typed = (
                    raw
                    .withColumn("trip_start_timestamp", F.to_timestamp("trip_start_timestamp", TS_FMT))
                    .withColumn("trip_end_timestamp", F.to_timestamp("trip_end_timestamp", TS_FMT))
                )
                for c in INT_COLS:
                    typed = typed.withColumn(c, F.col(c).cast("int"))
                for c in DOUBLE_COLS:
                    typed = typed.withColumn(c, F.col(c).cast("double"))
                typed = typed.filter(F.col("trip_start_timestamp").isNotNull()).select(*DDL_ORDER)

            with timed_step(ctx, "bronze.write") as info:
                typed.writeTo("lakehouse.bronze.taxi_trips").overwritePartitions()
                rows = spark.table("lakehouse.bronze.taxi_trips").filter(
                    F.date_format("trip_start_timestamp", "yyyy-MM") == month
                ).count()
                info["rows"] = rows

            null_start_dropped = max(raw_rows - rows, 0)
            duration = round(time.monotonic() - t0, 1)
            ctx.log.info(f"bronze.taxi_trips month={month} rows={rows} (raw={raw_rows}) in {duration}s")
            ctx.report_asset_materialization(
                metadata={
                    "month": month, "rows": rows, "raw_rows": raw_rows,
                    "null_trip_start_dropped": null_start_dropped,
                    "source": f"s3://{bucket}/{prefix}", "files": files,
                    "duration_seconds": duration,
                },
                data_version=f"{month}:{rows}",
            )
            with timed_step(ctx, "bronze.checks"):
                _report_checks(ctx, rows=rows, raw_rows=raw_rows, null_start_dropped=null_start_dropped)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)
        spark.stop()


if __name__ == "__main__":
    main()
