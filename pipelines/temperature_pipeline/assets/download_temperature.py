"""@bruin
name: ingest.download_temperature
type: python
@bruin"""

# pipelines/temperature_pipeline/assets/download_temperature.py
# ----------------------------------
# Downloads the global temperature anomaly dataset from Our World in Data
# and uploads the raw CSV to the GCS data lake.

# Source columns returned by the URL (useColumnShortNames=true returns
# pre-normalised snake_case names directly):
#   - entity                          : str   (e.g. "Northern Hemisphere", "World")
#   - code                            : str   (e.g. "OWID_NH")
#   - year                            : int   (e.g. 1850)
#   - near_surface_temperature_anomaly: float (deviation from baseline in °C)
#
# We rename near_surface_temperature_anomaly → global_temperature_anomaly
# to match our BigQuery staging schema column name.

import os
import requests
import pandas as pd
from google.cloud import storage

GCS_BUCKET = os.environ["GCS_BUCKET"]

# useColumnShortNames=true means the API returns snake_case column names
# directly — no need to normalise casing ourselves.
TEMPERATURE_URL = (
    "https://ourworldindata.org/explorers/climate-change.csv?v=1&csvType=full"
    "&useColumnShortNames=true&Metric=Temperature+anomaly&Long-run+series=false"
)

LOCAL_FILE = "/tmp/temperature_raw.csv"
GCS_OBJECT = "raw/temperature/temperature_raw.csv"

# Source column names (as returned by the URL) → BigQuery staging column names
# Only near_surface_temperature_anomaly needs renaming — the rest already match.
COLUMN_RENAME_MAP = {
    "near_surface_temperature_anomaly": "global_temperature_anomaly",
}

# Columns we expect to exist in the download before renaming
EXPECTED_SOURCE_COLUMNS = [
    "entity",
    "code",
    "year",
    "near_surface_temperature_anomaly",
]


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
    Validate source columns, rename to staging schema names,
    enforce data types, and preserve all rows.
    Nulls in the measurement column are valid at the staging
    layer — filtering belongs in the staging → mart transform.
    """
    # Validate that all expected source columns are present
    missing = [col for col in EXPECTED_SOURCE_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(
            f"Unexpected source schema. Missing columns: {missing}\n"
            f"Actual columns received: {list(df.columns)}"
        )

    # Rename near_surface_temperature_anomaly → global_temperature_anomaly
    df = df.rename(columns=COLUMN_RENAME_MAP)

    # Enforce data types
    df["entity"]                     = df["entity"].astype(str)
    df["code"]                       = df["code"].astype(str)
    df["year"]                       = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    df["global_temperature_anomaly"] = pd.to_numeric(df["global_temperature_anomaly"], errors="coerce")

    # Retain only the columns needed, in the correct order for BigQuery staging
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
