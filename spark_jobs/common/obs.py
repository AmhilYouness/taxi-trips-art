"""Shared observability helpers for the Spark/Python jobs.

One consistent way to log steps and report data-quality checks across every job:

  - get_logger(name)      uniform stderr logger (used where there is no handy
                          PipesContext, e.g. the pure-Python SODA client).
  - timed_step(ctx, name) context manager that brackets a stage with START/DONE
                          logs + elapsed seconds (and logs FAILED on exception).
  - check(...)            report an asset check via Pipes with consistent
                          WARN/ERROR logging so "anything happening" is visible.

Messages go through the Pipes logger (ctx.log.*) so they show up as first-class
log events in the Dagster UI; get_logger writes to stderr, which Dagster also
captures as compute logs.
"""
from __future__ import annotations

import logging
import os
import sys
import time
from contextlib import contextmanager
from typing import Any, Iterator, Mapping, Tuple

_FMT = "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"
_DATEFMT = "%Y-%m-%dT%H:%M:%S"


def volume_band() -> Tuple[int, int]:
    """Expected [min, max] monthly row count for the volume_in_band checks.

    Deliberately wide (a single month of Chicago taxi trips has ranged from a
    few hundred thousand to ~3M rows across 2013-2023). Override per environment
    with TAXI_VOLUME_MIN / TAXI_VOLUME_MAX.
    """
    lo = int(os.environ.get("TAXI_VOLUME_MIN", "5000"))
    hi = int(os.environ.get("TAXI_VOLUME_MAX", "5000000"))
    return lo, hi


def get_logger(name: str = "job") -> logging.Logger:
    """Return a stderr logger with a uniform ``ts | level | name | msg`` format."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(logging.Formatter(_FMT, _DATEFMT))
        logger.addHandler(handler)
        logger.propagate = False
    logger.setLevel(logging.INFO)
    return logger


@contextmanager
def timed_step(ctx, name: str) -> Iterator[dict]:
    """Bracket a stage with START/DONE logs and record its elapsed time.

    Yields a mutable dict; anything the caller stores in it (e.g. row counts) is
    echoed on the DONE line. On exception a FAILED line with the elapsed time is
    logged before the error propagates. ``ctx`` is a dagster_pipes PipesContext.
    """
    ctx.log.info(f"[{name}] START")
    start = time.monotonic()
    info: dict = {}
    try:
        yield info
    except Exception as err:  # noqa: BLE001 - log then re-raise
        elapsed = time.monotonic() - start
        ctx.log.error(f"[{name}] FAILED after {elapsed:.1f}s: {err}")
        raise
    else:
        elapsed = time.monotonic() - start
        detail = " ".join(f"{k}={v}" for k, v in info.items())
        ctx.log.info(f"[{name}] DONE in {elapsed:.1f}s{(' ' + detail) if detail else ''}")


def check(
    ctx,
    name: str,
    passed: bool,
    severity: str = "ERROR",
    metadata: Mapping[str, Any] | None = None,
) -> bool:
    """Report an asset check over Pipes with consistent logging.

    severity="ERROR" -> correctness violation (pair with a blocking AssetCheckSpec
    so it fails the run and raises the alarm). severity="WARN" -> soft anomaly,
    logged as a warning but non-blocking. Returns ``passed`` for convenience.
    """
    md = dict(metadata or {})
    ctx.report_asset_check(
        check_name=name, passed=passed, severity=severity, metadata=md
    )
    detail = " ".join(f"{k}={v}" for k, v in md.items())
    tail = f" {detail}" if detail else ""
    if passed:
        ctx.log.info(f"CHECK {name} PASSED{tail}")
    elif severity == "WARN":
        ctx.log.warning(f"CHECK {name} WARN (anomaly){tail}")
    else:
        ctx.log.error(f"CHECK {name} FAILED{tail}")
    return passed
