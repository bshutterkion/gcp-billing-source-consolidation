variable "demo2_project_id" {
  description = "GCP project ID for demo2-service"
  type        = string
  default     = "demo2-service"
}

variable "demo3_project_id" {
  description = "GCP project ID for demo3-service"
  type        = string
  default     = "demo3-service"
}

variable "region" {
  description = "GCP region for compute instances"
  type        = string
  default     = "us-east1"
}

variable "zone" {
  description = "GCP zone for compute instances"
  type        = string
  default     = "us-east1-b"
}
