# Madrid Short-Term Rental Observatory

Data engineering semester project (Universidad San Jorge, 2026-27).

The project studies where Airbnb accommodation is concentrated in Madrid and how its advertised availability and review activity vary across neighbourhoods and over time. It combines four related Inside Airbnb datasets for Madrid (snapshot 2026-09-17): listings, calendar, reviews and neighbourhoods.

## Technology

| Area | Choice |
|---|---|
| Main language | Python |
| Validation and transformation | DuckDB SQL, run from Python |
| Data format | Parquet |
| Data warehouse | DuckDB (local) |
| Orchestration | Apache Airflow, daily at 06:00 UTC (local); scheduled Azure container job in production |
| Cloud storage | Azure Blob Storage, accessed through the Python SDK |
| Dashboard | To be decided |

## Pipeline

```text
Inside Airbnb -> download (data/raw + Azure) -> validate -> transform with DuckDB
  -> curated Parquet (+ rejected records) -> DuckDB warehouse -> dashboard
```

Implemented today: download, raw validation, transformation, integration and validation of the curated data with DuckDB, with logging and failure alerts. Azure storage, automatic snapshot detection, scheduled execution and warehouse loading are still pending.

When orchestration is in place, a daily run will check whether a new Madrid snapshot exists. If it does, the pipeline will process it and record it in the `processed_snapshots` table. If not, the run will end successfully without changes.

### Results for snapshot 2026-09-17

| Dataset | Rows read | Valid | Rejected |
|---|---:|---:|---:|
| Neighbourhoods | 128 | 128 | 0 |
| Listings | 23,109 | 23,109 | 0 |
| Calendar | 8,438,629 | 8,438,629 | 0 |
| Reviews | 1,524,765 | 1,524,765 | 0 |

The integration produces a listing-level summary of 23,109 rows and passes all 12 quality checks in about 10 seconds. Full details: [evidence/2026-09-17_integration_report.json](evidence/2026-09-17_integration_report.json).

## Documentation

| Document | Content |
|---|---|
| [Data lifecycle mapping](docs/lifecycle.md) | How data moves through each lifecycle stage |
| [Architecture and cluster design](docs/architecture.md) | System components, data organisation and cluster decision |
| [Orchestration plan](docs/orchestration_plan.md) | Daily schedule, tasks, idempotency and production setup |
| [Data warehouse model](docs/data_model.md) | Star schema, design decisions and open questions (in progress) |
| [Logging and alerting](docs/logging_alerting.md) | What is logged, where, alert rules and the webhook channel |
| [Checkpoint 2 status](docs/checkpoint2_status.md) | Written status update for Checkpoint 2 |

## Project status

Checkpoint 2 (25 November 2026): architecture, ETL and monitoring.

- [x] Lifecycle mapping
- [x] Architecture and cluster design
- [x] Orchestration plan (design; the DAG and the production job are not implemented yet)
- [x] Working ingestion and integration (Azure upload pending)
- [ ] Data warehouse model (in progress: schema defined in `sql/create_tables.sql`)
- [x] Logging and alerting design (implemented for local runs; log upload to Azure, log retention and the missed-run alert are pending)

## Repository structure

```text
README.md                 project overview (this file)
requirements.txt          Python dependencies
.env.example              template for local secrets (Azure connection string, alert webhook)
config/sources.json       Inside Airbnb URLs, files and default snapshot
pipeline/
  utils.py                shared folders, configuration and logger
  monitoring.py           task timings, run history (logs/runs.jsonl) and alert rules
  alerts.py               sends alerts to the team chat through a webhook
  ingestion.py            download and validate the raw files (tasks 2 and 4)
  integration.py          clean, combine and validate the data with DuckDB (tasks 5-7)
sql/create_tables.sql     data warehouse schema (star schema)
docs/                     lifecycle, architecture, orchestration, data model, logging and alerting, Checkpoint 2 status
evidence/
  2026-09-17_integration_report.json        validation and integration results
  benchmark_star_vs_snowflake.py            star vs snowflake measurement (data model decision)
  benchmark_star_vs_snowflake_2026-09-17.json  its results
```

Planned: `pipeline/cloud.py` (Azure upload), `pipeline/checks.py`, `pipeline/tasks.py`, `pipeline/run_daily.py`, `dags/` (Airflow DAG) and `deploy/` (Azure scheduled job).

Local folders `data/`, `warehouse/` and `logs/` are created by the pipeline and excluded by `.gitignore`.

## How to run

Set up the environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Download and validate the raw files, then clean, combine and validate them (this writes the report in `evidence/`):

```bash
python -m pipeline.ingestion
python -m pipeline.integration
```

Optional: send a test alert (needs `ALERT_WEBHOOK_URL` in `.env`) and repeat the star vs snowflake measurement:

```bash
python -m pipeline.alerts --test
python evidence/benchmark_star_vs_snowflake.py
```

Each run writes its log to `logs/pipeline_<date>.log` and one summary line to `logs/runs.jsonl`.

## Data

Source data is not stored in this repository. It is downloaded from [Inside Airbnb](https://insideairbnb.com/get-the-data/). Credentials, such as the Azure connection string, are kept in a local `.env` file and are never committed.
