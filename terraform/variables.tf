variable "project_id" {
  description = "Google Cloud Project ID where the Open Lakehouse Raw layer will be provisioned."
  type        = string
}

variable "region" {
  description = "Unified Google Cloud region for GCS, BigLake Iceberg, BigQuery Pipelines, and Colab Enterprise."
  type        = string
  default     = "us-central1"
}

variable "dataset_id" {
  description = "BigQuery dataset ID for the Raw BigLake Iceberg layer."
  type        = string
  default     = "retail_raw"
}

variable "connection_id" {
  description = "BigQuery Cloud Resource Connection ID used by BigLake Apache Iceberg tables."
  type        = string
  default     = "retail_iceberg_conn"
}

variable "bucket_suffix" {
  description = "Suffix appended to project_id to form the Lakehouse GCS bucket name."
  type        = string
  default     = "retail-lakehouse"
}
