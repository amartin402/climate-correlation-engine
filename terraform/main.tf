# -------------------------------------------------------
# DATA LAKE — Google Cloud Storage
# -------------------------------------------------------
# A single bucket holds all raw CSV files uploaded by
# the ingestion scripts. The bucket name must be globally
# unique across all GCP projects, so we append the
# project ID to guarantee uniqueness.
# -------------------------------------------------------
resource "google_storage_bucket" "data_lake" {
  name          = "${var.project_id}-climate-data-lake"
  location      = var.region
  force_destroy = true   # allows terraform destroy to empty the bucket

  # Versioning keeps previous copies of files — useful for debugging
  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      age = 90  # delete raw files older than 90 days to control costs
    }
    action {
      type = "Delete"
    }
  }
}

# -------------------------------------------------------
# DATA WAREHOUSE — BigQuery Dataset (staging)
# -------------------------------------------------------
# Staging is where raw data lands straight from GCS before
# any transformation. Think of it as an exact mirror of
# what is in the data lake but queryable with SQL.
# -------------------------------------------------------
resource "google_bigquery_dataset" "staging" {
  dataset_id  = "climate_staging"
  description = "Raw climate data loaded directly from GCS"
  location    = var.bq_location
}

# Staging table: temperature anomalies (raw, unprocessed)
resource "google_bigquery_table" "stg_temperature" {
  dataset_id          = google_bigquery_dataset.staging.dataset_id
  table_id            = "stg_temperature"
  deletion_protection = false

  schema = jsonencode([
    { name = "entity",                    type = "STRING",  mode = "NULLABLE", description = "Country or region name (e.g. Northern Hemisphere, World)" },
    { name = "code",                      type = "STRING",  mode = "NULLABLE", description = "OWID country/region code" },
    { name = "year",                      type = "INTEGER", mode = "NULLABLE", description = "Calendar year of measurement" },
    { name = "global_temperature_anomaly", type = "FLOAT64", mode = "NULLABLE", description = "Deviation from baseline average temperature (°C)" }
  ])
}

# Staging table: sea level rise (raw, unprocessed)
resource "google_bigquery_table" "stg_sea_level" {
  dataset_id          = google_bigquery_dataset.staging.dataset_id
  table_id            = "stg_sea_level"
  deletion_protection = false

  schema = jsonencode([
    { name = "entity",                                type = "STRING",  mode = "NULLABLE", description = "Country or region name (e.g. World)" },
    { name = "code",                                  type = "STRING",  mode = "NULLABLE", description = "OWID country/region code" },
    { name = "day",                                   type = "DATE",    mode = "NULLABLE", description = "Date of measurement (YYYY-MM-DD)" },
    { name = "church_and_white_2011",                 type = "FLOAT64", mode = "NULLABLE", description = "Sea level change from Church and White (2011) dataset (mm)" },
    { name = "uhslc",                                 type = "FLOAT64", mode = "NULLABLE", description = "Sea level change from UHSLC dataset (mm)" },
    { name = "avg_church_white_and_uhslc",            type = "FLOAT64", mode = "NULLABLE", description = "Average of Church and White (2011) and UHSLC sea level measurements (mm)" }
  ])
}

# -------------------------------------------------------
# DATA WAREHOUSE — BigQuery Dataset (marts / analytics)
# -------------------------------------------------------
# The mart is where transformed, analytics-ready tables
# live. Looker Studio connects here, not to staging.
#
# PARTITIONING: We partition by year using an INTEGER
# RANGE partition. This means BigQuery only scans the
# partitions (year ranges) relevant to a query — e.g. a
# dashboard filtering to 1993–2020 skips all other data.
# This reduces cost and speeds up queries.
#
# CLUSTERING: Within each partition we cluster by entity.
# When a query filters WHERE entity = 'World', BigQuery
# reads only the clustered blocks for that entity instead
# of the full partition. Clustering is free and automatic.
# -------------------------------------------------------
resource "google_bigquery_dataset" "mart" {
  dataset_id  = "climate_mart"
  description = "Transformed, analytics-ready climate data for dashboards"
  location    = var.bq_location
}

resource "google_bigquery_table" "fact_temperature" {
  dataset_id          = google_bigquery_dataset.mart.dataset_id
  table_id            = "fact_temperature"
  deletion_protection = false

  # INTEGER RANGE partitioning on year (1850–2100, every 10 years)
  range_partitioning {
    field = "year"
    range {
      start    = 1850
      end      = 2100
      interval = 10
    }
  }

  # Cluster by entity so entity-level filters are fast
  clustering = ["entity"]

  schema = jsonencode([
    { name = "year",                type = "INTEGER", description = "Calendar year" },
    { name = "entity",              type = "STRING",  description = "Country or region name" },
    { name = "temperature_anomaly", type = "FLOAT",   description = "Mean temperature anomaly for the year (°C)" },
    { name = "anomaly_5yr_avg",     type = "FLOAT",   description = "5-year rolling average anomaly (°C)" }
  ])
}

resource "google_bigquery_table" "fact_sea_level" {
  dataset_id          = google_bigquery_dataset.mart.dataset_id
  table_id            = "fact_sea_level"
  deletion_protection = false

  range_partitioning {
    field = "year"
    range {
      start    = 1850
      end      = 2100
      interval = 10
    }
  }

  clustering = ["entity"]

  schema = jsonencode([
    { name = "year",             type = "INTEGER", description = "Calendar year" },
    { name = "entity",           type = "STRING",  description = "Country or region name" },
    { name = "sea_level_change", type = "FLOAT",   description = "Cumulative sea level change from baseline (mm)" },
    { name = "yoy_change_mm",    type = "FLOAT",   description = "Year-over-year sea level change (mm)" }
  ])
}
