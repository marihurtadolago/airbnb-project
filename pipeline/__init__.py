"""Madrid Short-Term Rental Observatory: data pipeline package.

Each module covers one part of the pipeline:
    utils        shared helpers (configuration, folder paths, logging)
    checks       task 1: detect whether a new snapshot is available
    ingestion    tasks 2-4: download, store raw files, validate raw files
    cloud        Azure Blob Storage client
    integration  tasks 5-7: transform, aggregate and validate curated data
    tasks        single import point used by the Airflow DAG and run_daily
"""
