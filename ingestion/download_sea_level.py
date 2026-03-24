# """
# ingestion/download_sea_level.py
# ---------------------------------
# Downloads the global sea level rise dataset from Our World in Data
# and uploads the raw CSV to the GCS data lake.

# Source columns (as received from Our World in Data):
#   - Entity                                    : str   (e.g. "World")
#   - Code                                      : str   (e.g. "OWID_WRL")
#   - Day                                       : date  (e.g. "1880-04-15")
#   - Church and White (2011)                   : float (sea level change in mm, nullable)
#   - UHSLC                                     : float (sea level change in mm, nullable)
#   - Average of Church and White (2011) and UHSLC : float (sea level change in mm, nullable)
# """

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

# Exact source column names → snake_case BigQuery-friendly names
COLUMN_RENAME_MAP = {
    "Entity":                                       "entity",
    "Code":                                         "code",
    "Day":                                          "day",
    "Church and White (2011)":                      "church_and_white_2011",
    "UHSLC":                                        "uhslc",
    "Average of Church and White (2011) and UHSLC": "avg_church_white_and_uhslc",
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
    Rename source columns to snake_case, enforce data types, and
    preserve all rows (including those where UHSLC is null) since
    nullable float columns are valid in the staging schema.
    """
    # Validate that all expected source columns are present
    missing = [col for col in COLUMN_RENAME_MAP if col not in df.columns]
    if missing:
        raise ValueError(f"Unexpected source schema. Missing columns: {missing}")

    # Rename to snake_case
    df = df.rename(columns=COLUMN_RENAME_MAP)

    # Enforce data types
    df["entity"]                  = df["entity"].astype(str)
    df["code"]                    = df["code"].astype(str)
    df["day"]                     = pd.to_datetime(df["day"], format="%Y-%m-%d").dt.date
    df["church_and_white_2011"]   = pd.to_numeric(df["church_and_white_2011"],  errors="coerce")
    df["uhslc"]                   = pd.to_numeric(df["uhslc"],                  errors="coerce")
    df["avg_church_white_and_uhslc"] = pd.to_numeric(df["avg_church_white_and_uhslc"], errors="coerce")

    # Retain column order to match BigQuery staging schema
    df = df[[
        "entity",
        "code",
        "day",
        "church_and_white_2011",
        "uhslc",
        "avg_church_white_and_uhslc",
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