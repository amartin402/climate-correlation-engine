# """
# pipelines/sea_level_pipeline/load_to_staging.py
# -------------------------------------------------
# Reads the raw sea level CSV from GCS and loads it
# into the BigQuery staging table.
# """

import os
import pandas as pd
from google.cloud import bigquery, storage

GCS_BUCKET  = os.environ["GCS_BUCKET"]
GCP_PROJECT = os.environ["GCP_PROJECT"]
GCS_OBJECT  = "raw/sea_level/sea_level_raw.csv"
BQ_TABLE    = f"{GCP_PROJECT}.climate_staging.stg_sea_level"

SCHEMA = [
    bigquery.SchemaField("entity",                   "STRING",  mode="NULLABLE", description="Country or region name (e.g. World)"),
    bigquery.SchemaField("code",                     "STRING",  mode="NULLABLE", description="OWID country/region code"),
    bigquery.SchemaField("day",                      "DATE",    mode="NULLABLE", description="Date of measurement (YYYY-MM-DD)"),
    bigquery.SchemaField("church_and_white_2011",    "FLOAT64", mode="NULLABLE", description="Sea level change from Church and White (2011) dataset (mm)"),
    bigquery.SchemaField("uhslc",                    "FLOAT64", mode="NULLABLE", description="Sea level change from UHSLC dataset (mm)"),
    bigquery.SchemaField("avg_church_white_and_uhslc", "FLOAT64", mode="NULLABLE", description="Average of Church and White (2011) and UHSLC sea level measurements (mm)"),
]

EXPECTED_COLUMNS = [field.name for field in SCHEMA]


def load_sea_level_to_staging() -> None:
    # Download raw CSV from GCS
    storage_client = storage.Client()
    blob = storage_client.bucket(GCS_BUCKET).blob(GCS_OBJECT)

    local_path = "/tmp/sea_level_for_staging.csv"
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
    df["day"]                        = pd.to_datetime(df["day"], format="%Y-%m-%d").dt.date
    df["church_and_white_2011"]      = pd.to_numeric(df["church_and_white_2011"],      errors="coerce")
    df["uhslc"]                      = pd.to_numeric(df["uhslc"],                      errors="coerce")
    df["avg_church_white_and_uhslc"] = pd.to_numeric(df["avg_church_white_and_uhslc"], errors="coerce")

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
    load_sea_level_to_staging()