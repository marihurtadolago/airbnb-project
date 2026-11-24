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
| Orchestration | Apache Airflow, daily at 06:00 UTC |
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
| [Orchestration plan](docs/orchestration_plan.md) | Daily schedule, tasks, idempotency and production options |

## Project status

Checkpoint 2 (25 November 2026): architecture, ETL and monitoring.

- [x] Lifecycle mapping
- [x] Architecture and cluster design
- [x] Orchestration plan (production option still to be decided)
- [ ] Working ingestion and integration
- [ ] Data warehouse model
- [ ] Logging and alerting design

## Repository structure

```text
config/     source URLs and settings (planned)
dags/       Airflow DAG (planned)
pipeline/   Python pipeline code (planned)
docs/       project documentation
```

Local folders `data/`, `warehouse/` and `logs/` are created by the pipeline and excluded by `.gitignore`.

## Data

Source data is not stored in this repository. It is downloaded from [Inside Airbnb](https://insideairbnb.com/get-the-data/). Credentials, such as the Azure connection string, are kept in a local `.env` file and are never committed.
