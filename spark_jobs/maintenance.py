"""Iceberg table maintenance: compaction + manifest rewrite + snapshot expiry.

Keeps the lakehouse healthy after monthly loads: merges small files, rewrites
manifests, and expires old snapshots. Safe to run repeatedly. Reports a summary
back to Dagster over Pipes.
"""
import time
from datetime import datetime, timedelta, timezone

from dagster_pipes import PipesContext, open_dagster_pipes

from common.obs import timed_step
from common.session import build_spark

TABLES = [
    "bronze.taxi_trips",
    "silver.taxi_trips",
    "gold.trips_daily",
    "gold.trips_by_hour",
    "gold.trips_by_company",
    "gold.payment_mix",
    "gold.top_routes",
    "gold.trips_by_pickup_area",
]
RETAIN_LAST = 5
EXPIRE_OLDER_THAN_DAYS = 7


def main() -> None:
    with open_dagster_pipes():
        ctx = PipesContext.get()
        t0 = time.monotonic()
        spark = build_spark("iceberg_maintenance")
        cutoff = (
            datetime.now(timezone.utc) - timedelta(days=EXPIRE_OLDER_THAN_DAYS)
        ).strftime("%Y-%m-%d %H:%M:%S")
        ctx.log.info(f"iceberg maintenance: {len(TABLES)} candidate table(s), expire cutoff={cutoff}")

        summary: dict = {}
        errors = 0
        for tbl in TABLES:
            full = f"lakehouse.{tbl}"
            if not spark.catalog.tableExists(full):
                ctx.log.info(f"skip {tbl}: table does not exist yet")
                continue
            try:
                with timed_step(ctx, f"maintenance.{tbl}") as info:
                    rd = spark.sql(
                        f"CALL lakehouse.system.rewrite_data_files(table => '{tbl}')"
                    ).collect()
                    spark.sql(f"CALL lakehouse.system.rewrite_manifests(table => '{tbl}')")
                    spark.sql(
                        "CALL lakehouse.system.expire_snapshots("
                        f"table => '{tbl}', older_than => TIMESTAMP '{cutoff}', "
                        f"retain_last => {RETAIN_LAST})"
                    )
                    rewritten = rd[0]["rewritten_data_files_count"] if rd else 0
                    added = rd[0]["added_data_files_count"] if rd else 0
                    summary[tbl] = {"rewritten": int(rewritten), "added": int(added)}
                    info["rewritten"] = int(rewritten)
                    info["added"] = int(added)
            except Exception as err:  # noqa: BLE001 - report and continue
                errors += 1
                summary[tbl] = {"error": str(err)}
                ctx.log.warning(f"maintenance failed for {tbl}: {err}")

        duration = round(time.monotonic() - t0, 1)
        ctx.log.info(f"iceberg maintenance done: {len(summary)} table(s), {errors} error(s) in {duration}s")
        ctx.report_asset_materialization(
            metadata={
                "tables_maintained": len(summary),
                "errors": errors,
                "duration_seconds": duration,
                "detail": str(summary),
            }
        )
        spark.stop()


if __name__ == "__main__":
    main()
