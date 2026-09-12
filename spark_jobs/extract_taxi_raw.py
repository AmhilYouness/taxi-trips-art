"""Raw extractor (separate from bronze): SODA API -> CSV files in MinIO.

Pages one month window from the Chicago Taxi API and uploads each page as a CSV
part file to the raw landing, partitioned by month folder:

    s3://<bucket>/<raw_prefix>/month=YYYY-MM/part-NNNNN.csv

Idempotent: the month prefix is emptied before writing. Pure Python (requests +
boto3), no Spark. Reports row/file counts back to Dagster via Pipes.

Env: WINDOW_START, WINDOW_END (ISO, [start, end) month window),
     S3_ENDPOINT, S3_KEY, S3_SECRET, S3_REGION, S3_BUCKET, TAXI_RAW_PREFIX.
"""
import os
import time

import boto3
from botocore.config import Config
from dagster_pipes import PipesContext, open_dagster_pipes

from common.obs import check, timed_step, volume_band
from common.soda import fetch_pages


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


def _delete_prefix(s3, bucket: str, prefix: str) -> int:
    """Delete all objects under a prefix (idempotent re-land). Returns count."""
    deleted = 0
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        objs = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if objs:
            s3.delete_objects(Bucket=bucket, Delete={"Objects": objs})
            deleted += len(objs)
    return deleted


def main() -> None:
    with open_dagster_pipes():
        ctx = PipesContext.get()
        t0 = time.monotonic()

        start = os.environ["WINDOW_START"]
        end = os.environ["WINDOW_END"]
        month = start[:7]  # YYYY-MM
        bucket = os.environ.get("S3_BUCKET", "lakehouse")
        raw_prefix = os.environ.get("TAXI_RAW_PREFIX", "raw/taxi").strip("/")
        prefix = f"{raw_prefix}/month={month}/"
        ctx.log.info(f"raw extract month={month} -> s3://{bucket}/{prefix}")

        s3 = _s3_client()
        with timed_step(ctx, "raw.clear_prefix") as info:
            removed = _delete_prefix(s3, bucket, prefix)
            info["removed_objects"] = removed

        total_rows = 0
        files = 0
        with timed_step(ctx, "raw.fetch_and_upload") as info:
            for page_index, csv_text, rows in fetch_pages(start, end):
                key = f"{prefix}part-{page_index:05d}.csv"
                s3.put_object(Bucket=bucket, Key=key, Body=csv_text.encode("utf-8"))
                total_rows += rows
                files += 1
                ctx.log.info(f"uploaded s3://{bucket}/{key} ({rows} rows)")
            info["rows"] = total_rows
            info["files"] = files

        duration = round(time.monotonic() - t0, 1)
        ctx.log.info(f"raw extract {month}: {total_rows} rows across {files} file(s) in {duration}s")
        ctx.report_asset_materialization(
            metadata={
                "month": month,
                "rows": total_rows,
                "files": files,
                "s3_prefix": f"s3://{bucket}/{prefix}",
                "window_start": start,
                "window_end": end,
                "duration_seconds": duration,
            },
            data_version=f"{month}:{total_rows}",
        )

        # ---- per-step DQ + volume tests ----
        with timed_step(ctx, "raw.checks"):
            check(
                ctx, "rows_landed_positive", passed=total_rows > 0,
                severity="ERROR", metadata={"rows": total_rows},
            )
            lo, hi = volume_band()
            check(
                ctx, "volume_in_band", passed=(lo <= total_rows <= hi),
                severity="WARN",
                metadata={"rows": total_rows, "expected_min": lo, "expected_max": hi},
            )


if __name__ == "__main__":
    main()
