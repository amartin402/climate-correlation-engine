output "data_lake_bucket" {
  description = "GCS bucket name for the raw data lake"
  value       = google_storage_bucket.data_lake.name
}

output "staging_dataset" {
  description = "BigQuery staging dataset ID"
  value       = google_bigquery_dataset.staging.dataset_id
}

output "mart_dataset" {
  description = "BigQuery mart (analytics) dataset ID"
  value       = google_bigquery_dataset.mart.dataset_id
}
