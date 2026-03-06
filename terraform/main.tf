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
    name = "transactions-topic"
    project = var.project_id

    message_retention_duration = "604800s" # 7 days
}

# Dataflow job reads messages from this subscription
resource "google_pubsub_subscription" "transactions_subscription" {
    name = "transactions-sub"
    project = var.project_id
    topic = google_pubsub_topic.transactions.id

    # 60 seconds to process a message before it is redelivered
    ack_deadline_seconds = 60

    # redeliver messages that have not been acknowledged within 7 days
    message_retention_duration = "604800s"
    retain_acked_messages = false

    # Ensure that messages are delivered exactly once to the Dataflow job
    enable_exactly_once_delivery = true

    expiration_policy {
        ttl = "" # don't expire
    }
}