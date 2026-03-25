#!/usr/bin/env bash
# scripts/run_pipeline.sh
# -------------------------------------------------------
# END-TO-END PIPELINE RUNNER
# -------------------------------------------------------
# This script runs the full pipeline in the correct order:
#   1. Ingest raw data → GCS (data lake)
#   2. Load GCS → BigQuery staging
#   3. Transform staging → BigQuery mart (analytics tables)
#
# PRE-REQUISITES (run once before first pipeline execution):
#   export GCS_BUCKET=your-project-id-climate-data-lake
#   export GCP_PROJECT=your-gcp-project-id
#   gcloud auth application-default login
#
# USAGE:
#   bash scripts/run_pipeline.sh
# -------------------------------------------------------
set -euo pipefail

# ---- Validate required environment variables ----
if [ -z "${GCS_BUCKET:-}" ] || [ -z "${GCP_PROJECT:-}" ]; then
  echo "ERROR: GCS_BUCKET and GCP_PROJECT environment variables must be set."
  echo "  export GCS_BUCKET=your-project-id-climate-data-lake"
  echo "  export GCP_PROJECT=your-gcp-project-id"
  exit 1
fi

VENV_PYTHON="/workspaces/climate-correlation-engine/.venv/bin/python"

echo ""
echo "================================================="
echo " Climate Correlation Engine — Pipeline Runner"
echo "================================================="
echo " GCP Project : $GCP_PROJECT"
echo " GCS Bucket  : $GCS_BUCKET"
echo "================================================="
echo ""

# ---- STEP 1: Ingest raw data to GCS ----
echo ">>> STEP 1: Ingesting raw data to GCS..."
$VENV_PYTHON pipelines/temperature_pipeline/assets/ingestion/download_temperature.py
$VENV_PYTHON pipelines/sea_level_pipeline/assets/ingestion/download_sea_level.py
echo ">>> Step 1 complete."
echo ""

# ---- STEP 2: Load GCS → BigQuery staging ----
echo ">>> STEP 2: Loading raw data into BigQuery staging..."
$VENV_PYTHON pipelines/temperature_pipeline/assets/staging/load_to_staging.py
$VENV_PYTHON pipelines/sea_level_pipeline/assets/staging/load_to_staging.py
echo ">>> Step 2 complete."
echo ""

# ---- STEP 3: Transform staging → BigQuery mart ----
echo ">>> STEP 3: Running SQL transformations into mart..."
bq query \
  --project_id="$GCP_PROJECT" \
  --use_legacy_sql=false \
  --format=none \
  < pipelines/temperature_pipeline/assets/bigquery/temperature_model.sql

bq query \
  --project_id="$GCP_PROJECT" \
  --use_legacy_sql=false \
  --format=none \
  < pipelines/sea_level_pipeline/assets/bigquery/sea_level_model.sql

echo ">>> Step 3 complete."
echo ""
echo "================================================="
echo " Pipeline complete."
echo " Connect Looker Studio to:"
echo "   Dataset : ${GCP_PROJECT}.climate_mart"
echo "   Tables  : fact_temperature, fact_sea_level"
echo "================================================="
