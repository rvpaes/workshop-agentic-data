locals {
  manifest = jsondecode(file("${path.module}/../raw_staging_parquet/iceberg_tables_manifest.json"))

  required_apis = toset([
    "bigquery.googleapis.com",
    "biglake.googleapis.com",
    "bigqueryconnection.googleapis.com",
    "dataform.googleapis.com",
    "dataplex.googleapis.com",
    "datalineage.googleapis.com",
    "dataproc.googleapis.com",
    "aiplatform.googleapis.com",
    "notebooks.googleapis.com",
    "cloudaicompanion.googleapis.com",
    "geminidataanalytics.googleapis.com",
    "storage.googleapis.com",
  ])

  common_labels = {
    datacloud = "antigravity"
    layer     = "raw"
    format    = "iceberg"
  }
}

# 1. Enable required Google Cloud APIs
resource "google_project_service" "apis" {
  for_each           = local.required_apis
  project            = var.project_id
  service            = each.value
  disable_on_destroy = false
}

# 2. Create Lakehouse GCS Bucket (object versioning MUST be disabled for BigLake Iceberg)
resource "google_storage_bucket" "lakehouse" {
  name                        = "${var.project_id}-${var.bucket_suffix}"
  project                     = var.project_id
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = true
  labels                      = local.common_labels

  versioning {
    enabled = false
  }

  depends_on = [google_project_service.apis]
}

# 3. Upload the 19 Iceberg-ready Parquet staging files
resource "google_storage_bucket_object" "staging_parquet" {
  for_each = local.manifest.tables
  name     = "staging/${each.key}.parquet"
  bucket   = google_storage_bucket.lakehouse.name
  source   = "${path.module}/../${each.value.parquet_file}"
}

# 4. Create Cloud Resource Connection for BigLake Apache Iceberg
resource "google_bigquery_connection" "iceberg_conn" {
  project       = var.project_id
  connection_id = var.connection_id
  location      = var.region
  friendly_name = "Retail Open Lakehouse Iceberg Connection"
  description   = "Cloud Resource Connection for BigLake Managed Apache Iceberg tables."

  cloud_resource {}

  depends_on = [google_project_service.apis]
}

# 5. Grant Storage Admin on Lakehouse bucket to the BigLake Connection Service Account
resource "google_storage_bucket_iam_member" "iceberg_conn_storage_admin" {
  bucket = google_storage_bucket.lakehouse.name
  role   = "roles/storage.admin"
  member = "serviceAccount:${google_bigquery_connection.iceberg_conn.cloud_resource[0].service_account_id}"
}

# Wait for IAM propagation before creating BigLake Iceberg tables
resource "time_sleep" "wait_for_iam_propagation" {
  create_duration = "45s"
  depends_on      = [google_storage_bucket_iam_member.iceberg_conn_storage_admin]
}

# 6. Create BigQuery Raw Dataset with business description
resource "google_bigquery_dataset" "retail_raw" {
  project                    = var.project_id
  dataset_id                 = var.dataset_id
  location                   = var.region
  description                = local.manifest.dataset_description
  delete_contents_on_destroy = true
  labels                     = local.common_labels

  depends_on = [google_project_service.apis]
}

# 7. Create all 19 BigLake Managed Apache Iceberg Tables (Partitioned, Clustered & Cataloged)
resource "google_bigquery_table" "raw_iceberg_tables" {
  for_each            = local.manifest.tables
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.retail_raw.dataset_id
  table_id            = each.key
  description         = each.value.description
  deletion_protection = false
  labels              = local.common_labels

  schema = jsonencode(each.value.columns)

  dynamic "time_partitioning" {
    for_each = each.value.partition_by != null ? [each.value.partition_by] : []
    content {
      type  = "DAY"
      field = time_partitioning.value
    }
  }

  clustering = each.value.cluster_by

  biglake_configuration {
    connection_id = google_bigquery_connection.iceberg_conn.name
    storage_uri   = "gs://${google_storage_bucket.lakehouse.name}/iceberg/raw/${each.key}"
    file_format   = "PARQUET"
    table_format  = "ICEBERG"
  }

  depends_on = [time_sleep.wait_for_iam_propagation]
}

# 8. Load Parquet data into Iceberg tables and export open Iceberg metadata snapshots
resource "google_bigquery_job" "load_and_export_iceberg" {
  for_each = local.manifest.tables
  project  = var.project_id
  location = var.region
  job_id   = "load_raw_${each.key}_${substr(filemd5("${path.module}/../${each.value.parquet_file}"), 0, 10)}"
  labels   = local.common_labels

  query {
    use_legacy_sql = false
    query          = <<-EOT
      TRUNCATE TABLE `${var.project_id}.${var.dataset_id}.${each.key}`;
      LOAD DATA INTO `${var.project_id}.${var.dataset_id}.${each.key}`
      FROM FILES (
        format = 'PARQUET',
        uris = ['gs://${google_storage_bucket.lakehouse.name}/staging/${each.key}.parquet']
      );
      EXPORT TABLE METADATA FROM `${var.project_id}.${var.dataset_id}.${each.key}`;
    EOT
  }

  depends_on = [
    google_bigquery_table.raw_iceberg_tables,
    google_storage_bucket_object.staging_parquet,
  ]
}
