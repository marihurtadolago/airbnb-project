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

A daily Airflow run checks whether a new Madrid snapshot exists. If it does, the pipeline processes it and records it in the `processed_snapshots` table. If not, the run ends successfully without changes.

## Documentation

| Document | Content |
|---|---|
| [Data lifecycle mapping](docs/lifecycle.md) | How data moves through each lifecycle stage |
| [Architecture and cluster design](docs/architecture.md) | System components, data organisation and cluster decision |
| [Orchestration plan](docs/orchestration_plan.md) | Daily schedule, tasks, idempotency and production setup |
| [Data warehouse model](docs/data_model.md) | Star schema, design decisions and open questions (in progress) |

## Project status

Checkpoint 2 (25 November 2026): architecture, ETL and monitoring.

- [x] Lifecycle mapping
- [x] Architecture and cluster design
- [x] Orchestration plan
- [x] Working ingestion and integration (Azure upload pending)
- [ ] Data warehouse model (in progress: schema defined in `sql/create_tables.sql`)
- [ ] Logging and alerting design

## Repository structure

```text
README.md                 project overview (this file)
requirements.txt          Python dependencies
.env.example              template for local secrets (Azure connection string)
config/sources.json       Inside Airbnb URLs, files and default snapshot
pipeline/
  utils.py                shared folders, configuration and logger
  ingestion.py            download and validate the raw files (tasks 2 and 4)
  integration.py          clean, combine and validate the data with DuckDB (tasks 5-7)
sql/create_tables.sql     data warehouse schema (star schema)
docs/                     lifecycle, architecture, orchestration and data model
evidence/                 validation reports produced by the pipeline
```

Planned: `pipeline/cloud.py` (Azure upload), `pipeline/checks.py`, `pipeline/tasks.py`, `pipeline/run_daily.py`, `dags/` (Airflow DAG) and `deploy/` (Azure scheduled job).

Local folders `data/`, `warehouse/` and `logs/` are created by the pipeline and excluded by `.gitignore`.

## How to run

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python -m pipeline.ingestion     # download and validate the raw files
python -m pipeline.integration   # clean, combine and validate; writes evidence/
```

## Data

Source data is not stored in this repository. It is downloaded from [Inside Airbnb](https://insideairbnb.com/get-the-data/). Credentials, such as the Azure connection string, are kept in a local `.env` file and are never committed.
