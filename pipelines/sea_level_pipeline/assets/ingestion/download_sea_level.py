"""@bruin
name: ingest.download_sea_level
type: python
@bruin"""

# pipelines/sea_level_pipeline/assets/download_sea_level.py
# ---------------------------------
# Downloads the global sea level rise dataset from Our World in Data
# and uploads the raw CSV to the GCS data lake.
#
# Source columns returned by the URL (useColumnShortNames=true returns
# pre-normalised snake_case names directly):
#   - entity                        : str   (e.g. "World")
#   - code                          : str   (e.g. "OWID_WRL")
#   - day                           : date  (e.g. "1880-04-15")
#   - sea_level_church_and_white_2011: float (Church & White 2011 series, mm)
#   - sea_level_uhslc               : float (UHSLC series, mm, nullable)
#   - sea_level_average             : float (average of both series, mm)
#
# We rename sea_level_average → sea_level_change to match our
# BigQuery staging schema column name, as it is the primary
# measurement column used downstream in the mart transformation.

import os
import requests
import pandas as pd
from google.cloud import storage

GCS_BUCKET = os.environ["GCS_BUCKET"]

SEA_LEVEL_URL = (
    "https://ourworldindata.org/grapher/sea-level.csv?v=1&csvType=full&useColumnShortNames=true"
)

LOCAL_FILE = "/tmp/sea_level_raw.csv"
GCS_OBJECT = "raw/sea_level/sea_level_raw.csv"

# Columns we expect to exist in the download (snake_case from API)
EXPECTED_SOURCE_COLUMNS = [
    "entity",
    "code",
    "day",
    "sea_level_church_and_white_2011",
    "sea_level_uhslc",
    "sea_level_average",
]

# Rename sea_level_average → sea_level_change to match staging schema.
# The other columns already match — no rename needed.
COLUMN_RENAME_MAP = {
    "sea_level_average": "sea_level_change",
}


def download_sea_level() -> pd.DataFrame:
    """Download sea level CSV from Our World in Data and return as a DataFrame."""
    print("Downloading sea level data from Our World in Data...")
    response = requests.get(SEA_LEVEL_URL, timeout=60)
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
    Nulls in sea_level_uhslc are valid — the UHSLC series has
    gaps. Filtering belongs in the staging → mart transform.
    """
    # Validate that all expected source columns are present
    missing = [col for col in EXPECTED_SOURCE_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(
            f"Unexpected source schema. Missing columns: {missing}\n"
            f"Actual columns received: {list(df.columns)}"
        )

    # Rename sea_level_average → sea_level_change
    df = df.rename(columns=COLUMN_RENAME_MAP)

    # Enforce data types
    df["entity"]                      = df["entity"].astype(str)
    df["code"]                        = df["code"].astype(str)
    df["day"]                         = pd.to_datetime(df["day"], format="%Y-%m-%d").dt.date
    df["sea_level_church_and_white_2011"] = pd.to_numeric(df["sea_level_church_and_white_2011"], errors="coerce")
    df["sea_level_uhslc"]             = pd.to_numeric(df["sea_level_uhslc"], errors="coerce")
    df["sea_level_change"]            = pd.to_numeric(df["sea_level_change"], errors="coerce")

    # Retain only the columns needed, in the correct order for BigQuery staging
    df = df[[
        "entity",
        "code",
        "day",
        "sea_level_church_and_white_2011",
        "sea_level_uhslc",
        "sea_level_change",
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
    df = download_sea_level()
    df = normalise_columns(df)
    df.to_csv(LOCAL_FILE, index=False)

    upload_to_gcs(LOCAL_FILE, GCS_OBJECT)
    print("Sea level ingestion complete.")
