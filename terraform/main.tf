terraform {
  backend "gcs" {
    bucket = "argus-terraform-state"
    prefix = "terraform/state"
  }

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# ──────────────────────────────────────────────
# Pub/Sub
# ──────────────────────────────────────────────

# Java producer sends messages to this topic
resource "google_pubsub_topic" "transactions" {
  name    = "transactions-topic"
  project = var.project_id

  message_retention_duration = "604800s" # 7 days
}

# Dataflow job reads messages from this subscription
resource "google_pubsub_subscription" "transactions_subscription" {
  name    = "transactions-sub"
  project = var.project_id
  topic   = google_pubsub_topic.transactions.id

  # 60 seconds to process a message before it is redelivered
  ack_deadline_seconds = 60

  # redeliver messages that have not been acknowledged within 7 days
  message_retention_duration = "604800s"
  retain_acked_messages      = false

  # Ensure that messages are delivered exactly once to the Dataflow job
  enable_exactly_once_delivery = true

  expiration_policy {
    ttl = "" # don't expire
  }
}

# ──────────────────────────────────────────────
# BigQuery
# ──────────────────────────────────────────────
resource "google_bigquery_dataset" "argus" {
  dataset_id  = "argus_dataset"
  project     = var.project_id
  location    = var.region
  description = "Argus financial transactions monitoring dataset"

  # 90 day partition (money saving measure)
  default_partition_expiration_ms = 7776000000
}

#                   -- BRONZE TABLE --                    #
resource "google_bigquery_table" "bronze" {
  dataset_id          = google_bigquery_dataset.argus.dataset_id
  table_id            = "bronze_raw_transactions"
  project             = var.project_id
  deletion_protection = true

  time_partitioning {
    type  = "DAY"
    field = "pubsub_publish_time"
  }

  clustering = ["ingestion_id"]

  schema = file("${path.module}/schemas/bronze.json")
}

#                  -- SILVER TABLE --                    #
resource "google_bigquery_table" "silver" {
  dataset_id          = google_bigquery_dataset.argus.dataset_id
  table_id            = "silver_enriched_transactions"
  project             = var.project_id
  deletion_protection = true

  time_partitioning {
    type  = "DAY"
    field = "event_timestamp"
  }

  clustering = ["card_id", "risk_zone"]

  schema = file("${path.module}/schemas/silver.json")
}

#                   -- GOLD TABLE --                    #
resource "google_bigquery_table" "gold" {
  dataset_id          = google_bigquery_dataset.argus.dataset_id
  table_id            = "gold_daily_fraud_summary"
  project             = var.project_id
  deletion_protection = true

  time_partitioning {
    type  = "DAY"
    field = "summary_date"
  }

  schema = file("${path.module}/schemas/gold.json")
}

# ──────────────────────────────────────────────
# DLQ + Dataflow Staging
# ──────────────────────────────────────────────

resource "google_storage_bucket" "dlq" {
  name          = "${var.project_id}-argus-dlq"
  project       = var.project_id
  location      = var.region
  storage_class = "STANDARD"
  force_destroy = true # delete all objects when destroying the bucket

}

resource "google_storage_bucket" "dataflow_staging" {
  name          = "${var.project_id}-argus-dataflow"
  project       = var.project_id
  location      = var.region
  storage_class = "STANDARD"
  force_destroy = true # delete all objects when destroying the bucket
}

# ──────────────────────────────────────────────
# IAM — Service Account for Dataflow
# ──────────────────────────────────────────────

resource "google_service_account" "dataflow_sa" {
  account_id   = "argus-dataflow-sa"
  display_name = "Argus Dataflow Service Account"
  project      = var.project_id
}

resource "google_project_iam_member" "dataflow_worker" {
  project = var.project_id
  role    = "roles/dataflow.worker"
  member  = "serviceAccount:${google_service_account.dataflow_sa.email}"
}

resource "google_project_iam_member" "bq_admin" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.dataflow_sa.email}"
}

resource "google_project_iam_member" "pubsub_subscriber" {
  project = var.project_id
  role    = "roles/pubsub.subscriber"
  member  = "serviceAccount:${google_service_account.dataflow_sa.email}"
}

resource "google_project_iam_member" "storage_admin" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.dataflow_sa.email}"
}