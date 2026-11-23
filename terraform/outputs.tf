output "project_id" {
  description = "Provisioned Google Cloud Project ID."
  value       = var.project_id
}

output "region" {
  description = "Unified Lakehouse region."
  value       = var.region
}

output "lakehouse_bucket" {
  description = "GCS bucket storing Iceberg data and metadata."
  value       = google_storage_bucket.lakehouse.url
}

output "iceberg_connection" {
  description = "Fully qualified BigQuery Cloud Resource Connection for BigLake Iceberg."
  value       = google_bigquery_connection.iceberg_conn.name
}

output "raw_dataset_id" {
  description = "BigQuery Raw dataset ID."
  value       = google_bigquery_dataset.retail_raw.dataset_id
}

output "tables_provisioned_count" {
  description = "Total number of BigLake Apache Iceberg tables created and loaded."
  value       = length(google_bigquery_table.raw_iceberg_tables)
}
