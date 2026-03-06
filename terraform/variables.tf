variable "project_id" {
  description = "GCP Project id"
  type        = string
}

variable "region" {
  description = "GCP region for Dataflow job"
  type        = string
  default     = "northamerica-northeast2"
}