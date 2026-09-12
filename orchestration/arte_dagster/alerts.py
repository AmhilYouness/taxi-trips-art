"""Alarm delivery for pipeline / data failures.

Alarms are LOG-ONLY for now: they are emitted at ERROR level, so they surface in
the Dagster daemon + sensor-tick logs and (via the persistent instance) in the
UI. This module is the ONE place to wire a real channel later - replace the body
of `send_alarm` with a Microsoft Teams / Slack incoming webhook, or switch the
sensor in definitions.py to `dagster.make_email_on_run_failure_sensor` once SMTP
is available.

Scope (matches the chosen alerting policy): alarms fire on run failures AND on
any ERROR-severity data-quality/volume check failure. ERROR checks are declared
`blocking=True`, so a failed ERROR check fails the run and is caught here too;
WARN checks only log + show in the UI and never raise an alarm.
"""
from __future__ import annotations

import logging

_log = logging.getLogger("arte.alerts")


def send_alarm(title: str, body: str, *, run_id: str | None = None) -> None:
    """Emit an alarm. Currently logs at ERROR; drop-in point for a real channel."""
    ref = f" (run_id={run_id})" if run_id else ""
    _log.error("ALARM: %s%s\n%s", title, ref, body)
    # --- To deliver to Microsoft Teams later, replace the log above with: -------
    # import os, requests
    # url = os.environ.get("TEAMS_WEBHOOK_URL")
    # if url:
    #     requests.post(url, json={"title": title, "text": f"{body}{ref}"}, timeout=10)
    # ---------------------------------------------------------------------------
