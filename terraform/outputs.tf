output "pubsub_topic" {
  value       = google_pubsub_topic.transactions.id
  description = "The ID of the Pub/Sub topic for transactions"
}

output "pubsub_subscription" {
  value       = google_pubsub_subscription.transactions_subscription.id
  description = "The ID of the Pub/Sub subscription for Dataflow"
}

output "bigquery_dataset" {
  value       = google_bigquery_dataset.argus.dataset_id
  description = "The ID of the BigQuery dataset"
}

output "dataflow_service_account_email" {
  value       = google_service_account.dataflow_sa.email
  description = "The email of the service account used by Dataflow"
}

output "dlq_bucket" {
  value       = google_storage_bucket.dlq.name
  description = "The name of the Dead Letter Queue bucket"
}

output "dataflow_staging_bucket" {
  value       = google_storage_bucket.dataflow_staging.name
  description = "The name of the Dataflow staging bucket"
}