# Architecture and cluster design

## 1. Architecture overview

Our project uses a batch processing architecture. Python downloads the Madrid Inside Airbnb datasets and coordinates every stage. Validation and transformation run as SQL queries in DuckDB, called from Python. Cleaned files are stored in Parquet format and will be loaded into a local DuckDB warehouse.

Azure Blob Storage will hold a cloud copy of the original files. SQL queries and a dashboard will use the warehouse to compare accommodation across neighbourhoods.

The first version will run on one team member's computer. Azure will provide remote storage, while processing and the warehouse will remain local.

## 2. System components

| Component | Role | Location |
|---|---|---|
| Inside Airbnb | Provides the four source datasets | External website |
| Python ingestion | Downloads the selected snapshot | Local computer |
| Raw storage | Keeps the original downloaded files | Local disk and planned Azure copy |
| Validation and transformation (DuckDB SQL run from Python) | Checks, cleans and integrates the data | Local computer |
| Parquet files | Store the cleaned datasets | Local disk |
| Rejected records | Store invalid records with rejection reasons | Restricted local folder |
| DuckDB warehouse | Stores the analytical tables | Local computer |
| Apache Airflow | Schedules and runs the pipeline once a day | Local computer; in production, a scheduled Azure container job runs the same tasks |
| Dashboard | Presents aggregate results | Delivery tool to be selected |
| GitHub | Stores code, configuration examples and documentation | Shared repository |

Azure Blob Storage will be accessed through the Python SDK. Cloud credentials and personal configuration will remain outside GitHub.

## 3. Processing flow

The pipeline runner will coordinate the processing stages.

First, it will identify the source snapshot (automatic detection is planned) and download the required files. The original files are kept unchanged in raw storage, and a copy will be uploaded to Azure Blob Storage (planned).

The validation stage will check required columns, missing values, duplicate keys, dates and numerical values. Transformation will select the required fields and convert them to suitable types.

Both stages are written as SQL queries executed by DuckDB through its Python API. DuckDB reads the compressed CSV files directly and writes Parquet, so the 8.4 million calendar rows do not have to be loaded into memory at once. Using the same engine for transformation and for the warehouse keeps the stack small.

Valid records are written to Parquet. Rejected records are stored separately with an explanation.

The warehouse loading stage will read the curated files and populate DuckDB tables. SQL queries and the dashboard will use the validated warehouse outputs.

Each stage will finish before the next dependent stage starts. The pipeline will stop if a required stage fails.

## 4. Data organisation

Files will be organised by snapshot date.

| Location | Contents |
|---|---|
| data/raw/2026-09-17/ | Original downloaded files |
| data/curated/2026-09-17/ | Cleaned Parquet files |
| data/rejected/2026-09-17/ | Rejected records and reasons |
| warehouse/airbnb.duckdb | Proposed analytical database |
| logs/ | Execution logs |
| evidence/ | Selected validation reports and evidence |

Azure Blob Storage will mirror the local structure, for example raw/2026-09-17/listings.csv.gz.

Source data, cleaned data and the warehouse will remain separate. This allows the team to repeat processing without changing the original files.

## 5. Integration rules

Listings are connected to calendar and review records through the listing identifier.

The neighbourhood reference table is matched using district and neighbourhood names. Unmatched values are rejected with a reason rather than silently discarded.

Calendar and review records remain separate in the detailed model. For combined reports, their summaries are calculated independently before being joined to listing information.

In the selected reviews file, the combination of listing_id and id is unique. Review ID alone is not used as the record key.

## 6. Cluster design decision

We will start without a distributed cluster.

The selected listings, calendar and reviews files contain approximately ten million records in total. Ingestion, validation and integration of these records run on a laptop: the integration stage takes about 10 seconds (see `evidence/2026-09-17_integration_report.json`). Warehouse loading and the Azure upload have not been measured yet.

A single-machine design is easier for our team to develop, test and explain. A cluster would introduce additional deployment and coordination work before we have evidence that it is needed.

We will measure processing time, peak memory use, disk space and query performance. We will first reduce unnecessary work by selecting the required columns, using Parquet and avoiding joins that multiply records. DuckDB supports this single-machine approach, because it processes large files in a columnar and parallel way without a separate server.

If the workload grows beyond the capacity of one machine, we will evaluate moving processing to a larger cloud machine or a distributed engine. These are future options, not components of the current implementation.

## 7. Scheduling and monitoring

Apache Airflow will start the pipeline once a day at 06:00 UTC. In production, a scheduled Azure container job will run the same tasks, so no machine has to stay on. The schedule, task order and this production decision are described in the [orchestration plan](orchestration_plan.md). It will check whether a new Madrid snapshot is available.

If no new snapshot is found, the run will record that there is no update. If new data is available, the pipeline will process it and mark the snapshot as complete in the processed_snapshots table of the warehouse only after all required stages succeed.

Overlapping executions must be prevented. In Airflow, `max_active_runs=1` does this. In production, scheduled Azure container job executions can overlap, so the runner will take a lock (a lease on a blob in Azure Blob Storage) before downloading, changing and uploading the warehouse. Loading the same snapshot again must not create duplicate warehouse records.

Logs record progress, record counts, durations and errors, and each run is summarised in `logs/runs.jsonl`. Alerts notify the team through a chat webhook when a run fails. The design is described in [logging_alerting.md](logging_alerting.md).

## 8. Failure handling and access

Temporary outputs will be kept separate from published outputs. A failed run must not replace the last successful warehouse results with incomplete data.

The raw files and Azure copy will allow the team to repeat processing if local derived files are lost. The cloud copy must be tested before it is described as a working recovery mechanism.

Only one process will write to the local warehouse at a time. Dashboard refreshes and queries will be coordinated with warehouse publication.

Reviewer names, reviewer ids and comments are never read, because they are not needed for the analysis. Public outputs will contain aggregate results.

## 9. Implementation status

Local ingestion, transformation and integration are implemented. The repository includes an integration report for snapshot 2026-09-17. The pipeline produces curated Parquet files, rejected records and a listing-level summary. Azure storage, automatic snapshot detection, scheduled execution and warehouse loading are still pending. The warehouse schema is defined in SQL.

Logging and failure alerts are implemented for the local runs. The orchestration plan has been written, but the Airflow DAG and the production job are not implemented yet. Dashboard delivery is also pending. The architecture describes the intended system and does not claim that all components are already deployed.

## Architecture diagram

Solid arrows show implemented components and connections. Dashed arrows show planned ones.

```mermaid
flowchart TD
    SOURCE["Inside Airbnb"] --> INGEST["Python ingestion"]

    subgraph LOCAL["Local computer"]
        INGEST --> RAW["Original files"]
        INGEST --> LOGS["Logs and run history"]
        RAW --> CHECKS["Quality checks"]
        CHECKS --> REPORTS["Validation reports"]
        CHECKS --> TRANSFORM["Transformation and integration (DuckDB)"]
        TRANSFORM --> PARQUET["Curated Parquet"]
        TRANSFORM --> REJECTS["Rejected records"]
        PARQUET -.-> WAREHOUSE["DuckDB warehouse"]
    end

    INGEST -.-> AZURE["Azure Blob Storage"]
    SCHEDULER["Daily scheduler"] -.-> INGEST
    WAREHOUSE -.-> DASHBOARD["Dashboard"]

    INGEST --> ALERTS["Failure alerts"]
    CHECKS --> ALERTS
    TRANSFORM --> ALERTS
    WAREHOUSE -.-> ALERTS
```