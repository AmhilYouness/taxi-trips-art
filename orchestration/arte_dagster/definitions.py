"""Dagster code location: taxi assets, resources, jobs, schedules.

Modes:
  - Backfill (history): materialize a range of monthly partitions of `taxi_ingest`
    (Dagster UI backfill or `dagster job backfill -j taxi_ingest --partition ...`).
  - Delta (incremental): `taxi_delta_schedule` runs the newest month each period;
    `taxi_lookback_schedule` re-runs the last 2 months for late/corrected trips.
  - Serving: `taxi_post` rebuilds gold marts then runs Iceberg maintenance.
"""
from datetime import timedelta

import dagster as dg
from dagster_docker import PipesDockerClient

from arte_dagster.alerts import send_alarm
from arte_dagster.assets import bronze, gold, maintenance, raw, silver
from arte_dagster.assets._common import TAXI_PARTITIONS

all_assets = dg.load_assets_from_modules([raw, bronze, silver, gold, maintenance])

# Partitioned ingest DAG: raw -> bronze -> silver (all monthly).
taxi_ingest_job = dg.define_asset_job(
    name="taxi_ingest",
    selection=dg.AssetSelection.groups("raw", "bronze", "silver"),
)

# Unpartitioned post-processing: gold marts -> Iceberg maintenance.
taxi_post_job = dg.define_asset_job(
    name="taxi_post",
    selection=dg.AssetSelection.groups("gold", "maintenance"),
)

# Delta: run the most recent completed month on the partition's natural cadence.
taxi_delta_schedule = dg.build_schedule_from_partitioned_job(taxi_ingest_job)


# Lookback: weekly re-materialize the last 2 months to absorb corrections.
@dg.schedule(job=taxi_ingest_job, cron_schedule="0 5 * * 1")
def taxi_lookback_schedule(context: dg.ScheduleEvaluationContext):
    keys = TAXI_PARTITIONS.get_partition_keys(context.scheduled_execution_time)
    for key in keys[-2:]:
        yield dg.RunRequest(partition_key=key, run_key=f"lookback-{key}")


# Rebuild marts + maintain daily (after ingest has landed the latest month).
taxi_post_schedule = dg.ScheduleDefinition(
    name="taxi_post_schedule",
    job=taxi_post_job,
    cron_schedule="0 6 * * *",
)


# ---- Alerting: one unified alarm for run failures AND blocking ERROR-check
# failures (which manifest as run failures). WARN checks never reach here. ----
@dg.run_failure_sensor(
    name="alarm_on_run_failure",
    default_status=dg.DefaultSensorStatus.RUNNING,
)
def alarm_on_run_failure(context: dg.RunFailureSensorContext) -> None:
    run = context.dagster_run
    partition = run.tags.get("dagster/partition", "-")
    lines = []
    for event in context.get_step_failure_events():
        err = None
        data = event.event_specific_data
        if data is not None and getattr(data, "error", None) is not None:
            err = data.error.to_string()
        lines.append(f"- {event.step_key}: {err or 'step failed'}")
    detail = "\n".join(lines) if lines else (context.failure_event.message or "run failed")
    send_alarm(
        title=f"Run failed: {run.job_name}",
        body=f"partition={partition}\n{detail}",
        run_id=run.run_id,
    )


# ---- Data freshness for the daily-rebuilt gold marts. Silver relies on its
# per-partition window_completeness check instead (the source is frozen). ----
gold_freshness_checks = dg.build_last_update_freshness_checks(
    assets=[dg.AssetKey(["gold", "taxi_marts"])],
    # taxi_post_schedule rebuilds gold daily at 06:00; flag it stale after ~30h.
    lower_bound_delta=timedelta(hours=30),
    severity=dg.AssetCheckSeverity.WARN,
)
gold_freshness_sensor = dg.build_sensor_for_freshness_checks(
    freshness_checks=gold_freshness_checks,
    default_status=dg.DefaultSensorStatus.RUNNING,
)

defs = dg.Definitions(
    assets=all_assets,
    asset_checks=[*gold_freshness_checks],
    jobs=[taxi_ingest_job, taxi_post_job],
    schedules=[taxi_delta_schedule, taxi_lookback_schedule, taxi_post_schedule],
    sensors=[alarm_on_run_failure, gold_freshness_sensor],
    resources={
        "spark_pipes": PipesDockerClient(),
    },
)
