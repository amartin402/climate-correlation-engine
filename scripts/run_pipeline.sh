#!/usr/bin/env bash
# scripts/run_pipeline.sh
# -------------------------------------------------------
# END-TO-END PIPELINE RUNNER
# -------------------------------------------------------
# Runs both pipelines end-to-end via Bruin, which handles
# asset dependency ordering, GCP auth via ADC, and logging.
#
# PRE-REQUISITES (run once before first pipeline execution):
#   export GCS_BUCKET=your-project-id-climate-data-lake
#   export GCP_PROJECT=your-gcp-project-id
#   gcloud auth application-default login
#   gcloud auth application-default set-quota-project $GCP_PROJECT
#   gcloud config set project $GCP_PROJECT
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

echo ""
echo "================================================="
echo " Climate Correlation Engine — Pipeline Runner"
echo "================================================="
echo " GCP Project : $GCP_PROJECT"
echo " GCS Bucket  : $GCS_BUCKET"
echo "================================================="
echo ""

# ---- TEMPERATURE PIPELINE ----
# Bruin runs all 3 assets in dependency order:
#   ingest.download_temperature
#       ↓
#   staging.load_temperature
#       ↓
#   mart.fact_temperature
echo ">>> Running temperature pipeline via Bruin..."
bruin run pipelines/temperature_pipeline/pipeline.yml
echo ">>> Temperature pipeline complete."
echo ""

# ---- SEA LEVEL PIPELINE ----
# Bruin runs all 3 assets in dependency order:
#   ingest.download_sea_level
#       ↓
#   staging.load_sea_level
#       ↓
#   mart.fact_sea_level
echo ">>> Running sea level pipeline via Bruin..."
bruin run pipelines/sea_level_pipeline/pipeline.yml
echo ">>> Sea level pipeline complete."
echo ""

echo "================================================="
echo " Pipeline complete."
echo " Connect Looker Studio to:"
echo "   Dataset : ${GCP_PROJECT}.climate_mart"
echo "   Tables  : fact_temperature, fact_sea_level"
echo "================================================="