# Logging and alerting design

## 1. Goal

The pipeline runs by itself once a day, usually when nobody is watching. We need two things from it:

- **Logs**, so that after any run we can see what happened, how long each step took, how many rows moved and, if something broke, exactly where and why.
- **Alerts**, so that the team finds out about a problem the same day, without having to open the logs to check.

Most days the run finds no new snapshot and finishes in under a minute. That is normal and should stay quiet. An alert should mean that someone has to do something.

## 2. What we log

Every module uses the same logger (`get_logger` in `pipeline/utils.py`). Each line looks like this:

```text
2026-11-25 06:00:14 | INFO     | 20261125T060012-81ba31 | integration | listings: 23109 rows read, 23109 valid, 0 rejected
```

| Field | Why it is there |
|---|---|
| Time (UTC) | Same time zone as the schedule, so log times match the 06:00 UTC run |
| Level | Lets us filter the important lines quickly |
| Run id | Groups every line of one run. `grep <run_id> logs/*.log` shows a complete run, even if it ran twice in one day |
| Module | Tells us which part of the pipeline wrote the line |

How we use the levels:

| Level | Used for | Example |
|---|---|---|
| INFO | Normal progress: task start and end, durations, row counts, files downloaded | `transform_clean: finished in 8.1 s` |
| WARNING | Something unusual that does not stop the run | rejected rows found, Azure or webhook not configured |
| ERROR | A task failed. The full traceback is included | `validate_curated: failed after 0.4 s` |
| CRITICAL | The whole run failed and an alert was sent | `run failed at download_files` |

The `@monitored_task` decorator (`pipeline/monitoring.py`) is applied to every task function. It writes the start, the end and the duration of each task, and the full error if it fails, so the task code does not need to repeat these lines. It never hides an error: the exception is logged and raised again, so Airflow or the runner still sees the failure and can retry.

Logs never contain personal data. The pipeline only writes counts, file names, snapshot dates and rejection reasons, never the content of a row. Secrets are never logged either: when a webhook call fails, we log only the error type, because the error message would include the webhook URL.

## 3. Where logs are stored

| Output | Location | Content |
|---|---|---|
| Daily log file | `logs/pipeline_YYYY-MM-DD.log` | Every log line of that day, readable by people |
| Run history | `logs/runs.jsonl` | One JSON line per run: run id, snapshot, status, duration of each task, the failed task and the error |
| Evidence reports | `evidence/<snapshot>_integration_report.json` | Row counts and validation results of a processed snapshot (kept in Git) |

The run history is a separate file, so it can be read by a script or loaded into DuckDB to answer questions such as "how long does `transform_clean` usually take?". We keep it as a file, not as a warehouse table, on purpose: in production the warehouse is only uploaded after a successful run (see the orchestration plan, section 8). A failed run would never reach a table, and failed runs are exactly the ones we want to keep.

Example line:

```json
{"run_id": "20261125T060012-81ba31", "snapshot_date": "2026-09-17", "status": "failed",
 "failed_task": "validate_curated", "error": "ValueError: Curated validation failed ...",
 "tasks": {"transform_clean": {"status": "success", "seconds": 8.1}, "...": "..."}, "seconds": 9.6}
```

**In Airflow**, each task already has its own log page, and Airflow keeps the run history in its own database. Our log lines show up there as well, because they also go to the console.

**In production** (scheduled Azure container job), the container is deleted after each run, so local files would be lost. The console output is collected automatically by Azure Log Analytics, where it can be searched by run id. The log file and `runs.jsonl` are also uploaded to Blob Storage under `logs/` at the end of the run, next to `raw/` and `curated/`. This upload will be added together with `pipeline/cloud.py`.

**Retention (planned, not implemented yet):** 30 days for daily log files, both local and in Log Analytics. The code does not delete old logs yet; this will be added with the production setup. That is enough to look into any problem and short enough that old logs do not pile up. `runs.jsonl` is small (one line per day) and is kept until the project is graded, like the rest of the evidence.

## 4. Alert rules

| # | Alert | Severity | When it fires | What to do |
|---|---|---|---|---|
| 1 | Run failed | CRITICAL | Any task fails after its retries, or hits the 60-minute timeout | Open the log for that run id, fix the cause, run again. The snapshot is not marked as processed, so the next daily run also retries it |
| 2 | Validation gate failed | CRITICAL | `validate_raw` or `validate_curated` finds a problem (this is a case of rule 1, and the message shows which check failed) | Check the source data. Usually Inside Airbnb changed a column or a file |
| 3 | Too many rejected rows | WARNING | More than 1% of the rows of a dataset are rejected in `transform_clean` | Look at `data/rejected/<snapshot>/` and the reasons in the evidence report. The run continues |
| 4 | Source looks stale | WARNING | The latest snapshot is more than 120 days old. Repeated once a week, not every day | Check the Inside Airbnb page: they may have stopped publishing Madrid or changed their URLs |
| 5 | Run did not happen (planned) | CRITICAL | The scheduled job did not start or crashed before Python ran | Check the Azure job or the Airflow scheduler |

Why these thresholds:

- **1% rejected rows:** snapshot 2026-09-17 rejected 0 rows out of about ten million, so a clean source is the normal case. 1% of the calendar is about 84,000 rows, which is far beyond random noise and almost always means the source format has changed.
- **120 days:** Inside Airbnb publishes Madrid roughly every three months (about 90 days). Thirty days of margin avoids false alarms when a release is a little late.

Rules 1 to 4 are implemented in `pipeline/monitoring.py` (rule 4 is not called yet, see section 8). Rule 5 is planned. Rule 5 cannot be detected by the pipeline itself, because the pipeline is not running. It is configured outside the code: in production, as an Azure Monitor alert rule on failed or missing executions of the container job; locally, the Airflow UI shows a missing or failed run.

### Avoiding alert spam

- A task that fails and then succeeds on a retry does **not** alert. Network glitches are normal, and retries exist for that. The alert is sent once, when the whole run has failed.
- A day with no new snapshot does **not** alert. It is the normal case.
- The stale-source warning is sent once a week, so it does not become noise the team learns to ignore.

## 5. Alert channel

Alerts are sent to the `#pipeline-alerts` channel of the team's Slack workspace through an **incoming webhook** (a Discord webhook would also work). Microsoft Teams webhooks expect a different message format, so Teams is not supported by the current code. We chose a webhook over email because:

- it is a single HTTP request with the `requests` library we already use, with no mail server and no password to store;
- the whole team sees the alert in the same place, and can discuss it there;
- changing the channel only means changing one URL.

The URL is a secret, so it lives in the `.env` file as `ALERT_WEBHOOK_URL` and is passed to the production job as a secret, like the Azure connection string. If it is empty, alerts are only written to the log. This way anyone in the team can run the pipeline locally without setting anything up.

Sending an alert can never break the pipeline: if the chat service is down, `send_alert` logs that the alert could not be delivered and returns. The original error still stops the run and stays in the log.

Example messages:

```text
[CRITICAL] Madrid Airbnb pipeline: run failed at download_files
snapshot 2026-12-20, run 20261221T060004-3f9a1c
HTTPError: 503 Server Error: Service Unavailable for url: https://data.insideairbnb.com/...

[WARNING] Madrid Airbnb pipeline: high share of rejected rows
listings: 5.0% rejected (limit 1%)
```

### Access for team members

There are two different things a team member may need: seeing the alerts, and sending them from their own computer.

**Seeing the alerts** needs no setup in the repository:

1. Accept the invitation to the **Madrid Airbnb pipeline** Slack workspace (any admin can send it with *Invite teammates*).
2. Join the `#pipeline-alerts` channel.
3. In the channel settings, set notifications to **All new messages**, so a CRITICAL alert shows up on your phone or desktop.

**Sending alerts from your own computer** (for example, when you run the pipeline or the forced-failure test) needs the webhook URL in your local `.env` file. There are two ways to get it:

- **Recommended:** ask a workspace admin to add you as a collaborator of the Slack app (*api.slack.com/apps* > Madrid Airbnb pipeline > *Collaborators*). You can then open *Incoming Webhooks* in the same page and copy the URL yourself.
- Ask a teammate who has it to send it to you in a **private** Slack message.

Then add it to `.env` and check it with `python -m pipeline.alerts --test`.

Rules for the URL, because anyone who has it can post in our channel:

- never commit it, never paste it in a channel, an issue, a pull request or a screenshot;
- it only lives in each member's `.env` file and, in production, in the job's secrets.

**If the URL leaks**, go to *api.slack.com/apps* > Madrid Airbnb pipeline > *Incoming Webhooks*, remove the old webhook and add a new one for `#pipeline-alerts`. The old URL stops working at once. Then share the new URL with the team in private and update the production secret.

## 6. How it connects to the orchestration

| Runner | Logging | Alert on failure |
|---|---|---|
| `python -m pipeline.ingestion` / `pipeline.integration` (manual) | `@monitored_task` + `monitored_run` | `monitored_run` sends the CRITICAL alert |
| `pipeline/run_daily.py` (production) | Same: the whole run goes inside `with monitored_run(snapshot):` | Same |
| Airflow DAG | `@monitored_task` (already on the task functions) + Airflow task logs | `on_failure_callback=airflow_failure_callback` in `default_args`. Airflow calls it only after the last retry |

For the DAG this is one extra line:

```python
from pipeline.monitoring import airflow_failure_callback

default_args = {
    ...
    "on_failure_callback": airflow_failure_callback,
}
```

`check_rejections` runs right after `transform_clean`, and `check_freshness` runs inside `check_new_snapshot` with the latest published date.

## 7. How to test it

```bash
# 1. Put the webhook URL of the team channel in .env, then:
python -m pipeline.alerts --test            # a test message should appear in the channel

# 2. Force a failure: this snapshot does not exist, so the download fails
python -m pipeline.ingestion 1999-01-01     # CRITICAL alert "run failed at download_files"
```

After each run, `logs/pipeline_<date>.log` shows every step with its run id, and `logs/runs.jsonl` has one new line.

## 8. Pending

- Upload of the log file and `runs.jsonl` to Blob Storage (with `pipeline/cloud.py`).
- Azure Monitor alert rule for missing runs (rule 5), created together with the scheduled job in `deploy/`.
- Calling `check_freshness` from `check_new_snapshot`, and adding `on_failure_callback` to the DAG, once those files exist.
- Automatic deletion of daily log files older than 30 days.
