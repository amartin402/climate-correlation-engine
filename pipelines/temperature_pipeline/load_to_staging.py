"""
pipelines/temperature_pipeline/load_to_staging.py
---------------------------------------------------
Reads the raw temperature anomaly CSV from GCS and loads it
into the BigQuery staging table.

STAGING PATTERN:
  We use WRITE_TRUNCATE (full replace) for staging. Each run
  replaces the entire staging table with the latest download.
  This keeps staging simple — it always reflects the latest
  raw file in GCS, nothing more.
"""

import os
import pandas as pd
from google.cloud import bigquery, storage

GCS_BUCKET  = os.environ["GCS_BUCKET"]
GCP_PROJECT = os.environ["GCP_PROJECT"]
GCS_OBJECT  = "raw/temperature/temperature_raw.csv"
BQ_TABLE    = f"{GCP_PROJECT}.climate_staging.stg_temperature"

SCHEMA = [
    bigquery.SchemaField("entity",                     "STRING",  mode="NULLABLE", description="Country or region name (e.g. Northern Hemisphere, World)"),
    bigquery.SchemaField("code",                       "STRING",  mode="NULLABLE", description="OWID country/region code"),
    bigquery.SchemaField("year",                       "INTEGER", mode="NULLABLE", description="Calendar year of measurement"),
    bigquery.SchemaField("global_temperature_anomaly", "FLOAT64", mode="NULLABLE", description="Deviation from baseline average temperature (°C)"),
]

EXPECTED_COLUMNS = [field.name for field in SCHEMA]


def load_temperature_to_staging() -> None:
    # Download raw CSV from GCS
    storage_client = storage.Client()
    blob = storage_client.bucket(GCS_BUCKET).blob(GCS_OBJECT)

    local_path = "/tmp/temperature_for_staging.csv"
    blob.download_to_filename(local_path)
    print(f"Downloaded gs://{GCS_BUCKET}/{GCS_OBJECT}")

    df = pd.read_csv(local_path)
    print(f"  Loaded {len(df):,} rows from GCS")

    # Validate columns match expected schema before loading
    missing = [col for col in EXPECTED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"CSV is missing expected columns: {missing}")

    # Enforce data types to match BigQuery schema
    df["entity"]                     = df["entity"].astype(str)
    df["code"]                       = df["code"].astype(str)
    df["year"]                       = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    df["global_temperature_anomaly"] = pd.to_numeric(df["global_temperature_anomaly"], errors="coerce")

    # Retain only schema columns in the correct order
    df = df[EXPECTED_COLUMNS]

    # Load into BigQuery staging table
    bq_client = bigquery.Client(project=GCP_PROJECT)

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=False,
        schema=SCHEMA,
    )

    job = bq_client.load_table_from_dataframe(df, BQ_TABLE, job_config=job_config)
    job.result()
    print(f"  Loaded {len(df):,} rows into {BQ_TABLE}")


if __name__ == "__main__":
    load_temperature_to_staging()