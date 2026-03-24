""" @bruin
name: ingest.download_temperature
type: python
@bruin"""

# """
# ingestion/download_temperature.py
# ----------------------------------
# Downloads the global temperature anomaly dataset from Our World in Data
# and uploads the raw CSV to the GCS data lake.

# Source columns (as received from Our World in Data):
#   - Entity                      : str   (e.g. "Northern Hemisphere", "World")
#   - Code                        : str   (e.g. "OWID_NH")
#   - Year                        : int   (e.g. 1850)
#   - Global temperature anomaly  : float (deviation from baseline in °C, e.g. -0.055067)
# """

import os
import requests
import pandas as pd
from google.cloud import storage

GCS_BUCKET = os.environ["GCS_BUCKET"]

TEMPERATURE_URL = (
    "https://ourworldindata.org/explorers/climate-change.csv?v=1&csvType=full"
    "&useColumnShortNames=true&Metric=Temperature+anomaly&Long-run+series=false"
)

LOCAL_FILE = "/tmp/temperature_raw.csv"
GCS_OBJECT = "raw/temperature/temperature_raw.csv"

# Exact source column names → snake_case BigQuery-friendly names
COLUMN_RENAME_MAP = {
    "Entity":                     "entity",
    "Code":                       "code",
    "Year":                       "year",
    "Global temperature anomaly": "global_temperature_anomaly",
}


def download_temperature() -> pd.DataFrame:
    """Download temperature anomaly CSV from Our World in Data and return as a DataFrame."""
    print("Downloading temperature anomaly data from Our World in Data...")
    response = requests.get(TEMPERATURE_URL, timeout=60)
    response.raise_for_status()

    with open(LOCAL_FILE, "wb") as f:
        f.write(response.content)

    df = pd.read_csv(LOCAL_FILE)
    print(f"  Downloaded {len(df):,} rows, columns: {list(df.columns)}")
    return df


def normalise_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rename source columns to snake_case, enforce data types, and
    preserve all rows. Nulls in the measurement column are valid
    at the staging layer — filtering belongs in the staging → mart transform.
    """
    # Validate that all expected source columns are present
    missing = [col for col in COLUMN_RENAME_MAP if col not in df.columns]
    if missing:
        raise ValueError(f"Unexpected source schema. Missing columns: {missing}")

    # Rename to snake_case
    df = df.rename(columns=COLUMN_RENAME_MAP)

    # Enforce data types
    df["entity"]                    = df["entity"].astype(str)
    df["code"]                      = df["code"].astype(str)
    df["year"]                      = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    df["global_temperature_anomaly"] = pd.to_numeric(df["global_temperature_anomaly"], errors="coerce")

    # Retain column order to match BigQuery staging schema
    df = df[[
        "entity",
        "code",
        "year",
        "global_temperature_anomaly",
    ]]

    print(f"  Normalised to {len(df):,} rows, columns: {list(df.columns)}")
    return df


def upload_to_gcs(local_path: str, gcs_object: str) -> None:
    """Upload a local file to the GCS data lake bucket."""
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    blob = bucket.blob(gcs_object)
    blob.upload_from_filename(local_path)
    print(f"  Uploaded gs://{GCS_BUCKET}/{gcs_object}")


if __name__ == "__main__":
    df = download_temperature()
    df = normalise_columns(df)
    df.to_csv(LOCAL_FILE, index=False)

    upload_to_gcs(LOCAL_FILE, GCS_OBJECT)
    print("Temperature ingestion complete.")