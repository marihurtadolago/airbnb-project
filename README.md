# Madrid Short-Term Rental Observatory

Data engineering semester project (Universidad San Jorge, 2026-27).

The project studies where Airbnb accommodation is concentrated in Madrid and how its advertised availability and review activity vary across neighbourhoods and over time. It uses four related Inside Airbnb datasets for Madrid: listings, calendar, reviews and neighbourhoods.

## Team

| Member | Role |
|---|---|
| Mari Carmen Hurtado | Ingestion & Storage Lead |
| Jessi Ying Ying Hu | Platform & Cloud Lead |
| Nicolás García | Orchestration & Quality Lead |
| Ricardo Buxton | Delivery & Presentation Lead |

## Technology

- Python to coordinate the pipeline, with DuckDB SQL for validation and transformation
- Parquet for cleaned datasets
- DuckDB as the local data warehouse
- Apache Airflow for daily orchestration
- Azure Blob Storage, accessed through the Python SDK, for cloud storage

## Documentation

| Document | Content | Author |
|---|---|---|
| [Data lifecycle mapping](docs/lifecycle.md) | How data moves through each lifecycle stage | Mari Carmen Hurtado |
| [Architecture and cluster design](docs/architecture.md) | System components, data organisation and cluster decision | Mari Carmen Hurtado |
| [Orchestration plan](docs/orchestration_plan.md) | Daily schedule, tasks, idempotency and production options | Nicolás García |

## Project status

Checkpoint 2 (25 November 2026): architecture, ETL and monitoring.

- [x] Lifecycle mapping
- [x] Architecture and cluster design
- [ ] Working ingestion and integration
- [ ] Data warehouse model
- [x] Orchestration plan (production option pending)
- [ ] Logging and alerting design

## Data

Source data is not stored in this repository. The files are downloaded from [Inside Airbnb](https://insideairbnb.com/get-the-data/) and kept locally under `data/`, which is excluded by `.gitignore`.
