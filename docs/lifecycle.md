# Data lifecycle mapping

## Project scope

Our project uses four Inside Airbnb datasets for Madrid from the release labelled 17 September 2026.

The datasets contain listing information, calendar availability, reviews and a neighbourhood reference table. We will combine them to compare Airbnb accommodation across Madrid neighbourhoods.

Our agreed architecture uses Python to coordinate the pipeline, DuckDB SQL for validation and transformation, Parquet for cleaned files, a local DuckDB warehouse and Azure Blob Storage for a cloud copy of the original data.

## 1. Data source

The original files are available from Inside Airbnb.

https://insideairbnb.com/get-the-data/

We use listings.csv.gz, calendar.csv.gz, reviews.csv.gz and neighbourhoods.csv.

The release label identifies the selected publication. Individual records may have different collection dates.

## 2. Ingestion

Python reads the dataset URLs from config/sources.json and downloads the files.

Each download is first saved with a temporary .part extension. When it finishes successfully, the file receives its final name. Progress and errors are recorded in a log.

The four files were downloaded for a first prototype, which has since been discarded. The download step is being reimplemented. The planned daily process will check for new releases. If no new release is available, it will record that the check finished without an update.

## 3. Raw storage

Original files are stored locally in data/raw/2026-09-17/. They are not edited manually.

We will also upload a copy to Azure Blob Storage using the Python SDK. Files will be organised by city and release date.

The local files are available. The cloud storage step is planned.

Large datasets and credentials will remain outside GitHub. The repository will contain the source configuration, processing code, documentation and selected evidence.

## 4. Validation

Before transforming the data, we check file readability, required columns, missing values and duplicate keys.

The completed local checks found these record counts.

| Dataset | Records |
|---|---:|
| Listings | 23,109 |
| Calendar | 8,438,629 |
| Reviews | 1,524,765 |
| Neighbourhoods | 128 |

No missing values were found in the selected columns.

The checked keys are unique for listings, calendar and neighbourhoods. Some review IDs appear under different listings, but the combination of listing_id and id is unique in this file. We will use this combination as the review key.

These checks have not changed or deleted records. Checks of dates, numerical fields, availability values and relationships between datasets are still pending.

## 5. Transformation and integration

SQL queries executed by DuckDB from Python will select the columns needed for analysis, remove unnecessary spaces and convert values to suitable types. DuckDB reads the compressed source files directly and writes Parquet without loading whole datasets into memory.

Records that fail the agreed validation rules will be stored separately with a rejection reason. Unusual values will be investigated before deciding whether to exclude them.

Listings will be matched to the neighbourhood reference table using district and neighbourhood names. Calendar and review records will be linked to listings through listing_id.

Calendar and review summaries will be calculated separately before combining them with listing information. A direct join between both detailed datasets would multiply records and produce incorrect counts.

Reviewer names and comments will be excluded from the cleaned review data because they are not needed for the planned analysis.

## 6. Curated storage

Cleaned datasets will be saved as Parquet files in data/curated/2026-09-17/.

Rejected records will be saved in data/rejected/2026-09-17/ with the reasons for rejection.

Outputs will identify their source release. Original files will remain separate so that processing can be repeated.

## 7. Warehouse loading

The cleaned data will be loaded into a local DuckDB warehouse.

The warehouse will contain related tables for listings, neighbourhoods, dates, availability and review activity. Its detailed model and loading process will be developed as a separate team task.

Processing the same release again must not create duplicate warehouse records. An incomplete load must not replace the last successful published results.

## 8. Consumption

SQL queries and a dashboard will allow users to compare listing counts, accommodation types, advertised availability and review activity across neighbourhoods.

Outputs will show the release and time period used. Unavailable dates will not be treated as confirmed bookings, and review counts will not be treated as booking counts.

Users will receive aggregate results rather than personal reviewer information.

## 9. Retention and deletion

We propose keeping the selected source files, validation reports and derived outputs until grading is complete.

After grading, the team will review which files still need to be retained. Unnecessary personal data and temporary files will be deleted from local and cloud storage.

Rejected records will be accessible only to the team and kept only while needed to investigate data quality problems.

## Current status

The first local prototype has been discarded. Downloads, validation and transformation are being reimplemented, with DuckDB as the processing engine.

Cloud storage, further validation, transformation, integration, warehouse loading and dashboard delivery remain planned stages.

This document describes the lifecycle design. It does not claim that the complete pipeline is already implemented.

## Lifecycle diagram

Solid arrows show the core local stages being implemented for Checkpoint 2. Dashed arrows show planned stages.

```mermaid
flowchart TD
    A["Inside Airbnb"] --> B["Python download"]
    B --> C["Local raw storage"]
    C --> D["Validation"]
    D --> E["Quality reports"]

    B -.-> F["Azure Blob raw storage"]
    D -.-> G["Transformation and integration (DuckDB)"]
    G -.-> H["Curated Parquet files"]
    G -.-> I["Rejected records"]
    H -.-> J["DuckDB warehouse"]
    J -.-> K["SQL queries and dashboard"]

    C -.-> L["Retention review and deletion"]
    F -.-> L
    H -.-> L
    I -.-> L
```