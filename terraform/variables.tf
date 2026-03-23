variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region for all resources"
  type        = string
  default     = "africa-south1"
}

variable "bq_location" {
  description = "BigQuery dataset location"
  type        = string
  default     = "africa-south1"
}
