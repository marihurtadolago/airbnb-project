"""Run monitoring: task timings, run history and the alert rules.

    @monitored_task           logs the start, end and duration of a task, and the
                              full error if it fails
    monitored_run(snapshot)   wraps one whole run: gives it a run_id, sends a
                              CRITICAL alert if it fails and appends a summary
                              line to logs/runs.jsonl
    check_rejections(counts)  WARNING when a dataset rejects too many rows
    check_freshness(date)     WARNING when Inside Airbnb has published nothing new
                              for a long time
    airflow_failure_callback  the same CRITICAL alert, for the Airflow DAG

Design and alert rules: docs/logging_alerting.md
"""

import functools
import json
import time
import uuid
from contextlib import contextmanager
from datetime import date, datetime, timezone

from pipeline import utils
from pipeline.alerts import send_alert

log = utils.get_logger("monitoring")

RUNS_FILE = utils.LOGS_DIR / "runs.jsonl"
MAX_REJECTED_SHARE = 0.01  # 1 % of the rows of one dataset
STALE_AFTER_DAYS = 120     # Inside Airbnb usually publishes Madrid every three months

_run = {"tasks": {}}  # summary of the current run


def _record_task(name: str, status: str, start: float) -> float:
    seconds = round(time.perf_counter() - start, 1)
    _run["tasks"][name] = {"status": status, "seconds": seconds}
    if status == "failed":
        _run["failed_task"] = name
    elif _run.get("failed_task") == name:  # it succeeded on a retry
        del _run["failed_task"]
    return seconds


def monitored_task(func):
    """Decorator for pipeline tasks. Failures are logged and re-raised, never hidden.

    It does not alert by itself: a task may still be retried, so the alert is sent
    once the whole run has failed (monitored_run or the Airflow callback).
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        name = func.__name__
        log.info("%s: started", name)
        start = time.perf_counter()
        try:
            result = func(*args, **kwargs)
        except Exception:
            log.exception("%s: failed after %.1f s", name, _record_task(name, "failed", start))
            raise
        log.info("%s: finished in %.1f s", name, _record_task(name, "success", start))
        return result
    return wrapper


@contextmanager
def monitored_run(snapshot_date: str):
    """Track one pipeline run from start to end.

    Usage:
        with monitored_run("2026-09-17"):
            download_files(...)
            ...
    """
    run_id = f"{datetime.now(timezone.utc):%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:6]}"
    utils.set_run_id(run_id)
    _run.clear()
    _run.update(run_id=run_id, snapshot_date=snapshot_date,
                started_at=datetime.now(timezone.utc).isoformat(timespec="seconds"), tasks={})
    log.info("run started for snapshot %s", snapshot_date)
    start = time.perf_counter()

    try:
        yield _run
        _run["status"] = "success"
    except Exception as exc:
        _run.update(status="failed", error=f"{type(exc).__name__}: {exc}")
        send_alert("CRITICAL", f"run failed at {_run.get('failed_task', 'unknown task')}",
                   f"snapshot {snapshot_date}, run {run_id}\n{_run['error']}")
        raise
    finally:
        _run.setdefault("status", "interrupted")
        _run["seconds"] = round(time.perf_counter() - start, 1)
        RUNS_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(RUNS_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(_run, default=str) + "\n")
        log.info("run finished: %s in %.1f s", _run["status"], _run["seconds"])
        utils.set_run_id("-")


def check_rejections(counts: dict) -> dict:
    """Warn when a dataset rejects more than MAX_REJECTED_SHARE of its rows.

    `counts` is the result of integration.transform_clean. Rejected rows do not
    stop the run (they are kept in data/rejected/), but a sudden increase usually
    means the source format has changed.
    """
    shares = {name: c["rows_rejected"] / c["rows_read"]
              for name, c in counts.items() if c["rows_read"]}
    too_many = {name: share for name, share in shares.items() if share > MAX_REJECTED_SHARE}
    if too_many:
        send_alert("WARNING", "high share of rejected rows",
                   "\n".join(f"{name}: {share:.1%} rejected (limit {MAX_REJECTED_SHARE:.0%})"
                             for name, share in too_many.items()))
    return too_many


def check_freshness(latest_snapshot: str, today: date = None) -> int:
    """Warn when the latest snapshot is older than STALE_AFTER_DAYS.

    The daily run would otherwise keep saying "no new snapshot" forever without
    anyone noticing that the source has stopped publishing or changed its URLs.
    The warning is repeated once a week, not every day. Returns the age in days.
    """
    today = today or datetime.now(timezone.utc).date()
    age = (today - date.fromisoformat(latest_snapshot)).days
    if age >= STALE_AFTER_DAYS and (age - STALE_AFTER_DAYS) % 7 == 0:
        send_alert("WARNING", "no new snapshot",
                   f"the latest Madrid snapshot ({latest_snapshot}) is {age} days old")
    return age


def airflow_failure_callback(context: dict) -> None:
    """on_failure_callback for the DAG. Airflow calls it only after the last retry."""
    task = context["task_instance"].task_id
    send_alert("CRITICAL", f"run failed at {task}",
               f"run {context.get('run_id')}\n{context.get('exception')}")
