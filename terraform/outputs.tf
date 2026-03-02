output "demo2_instance_name" {
  description = "Name of the compute instance in demo2-service"
  value       = google_compute_instance.demo2_billing.name
}

output "demo3_instance_name" {
  description = "Name of the compute instance in demo3-service"
  value       = google_compute_instance.demo3_billing.name
}

output "estimated_hourly_cost" {
  description = "Estimated hourly cost per instance (n2-standard-8 in us-east1)"
  value       = "$0.388/hr per instance ($0.776/hr total)"
}

output "destroy_reminder" {
  description = "Remember to destroy after 12 hours to limit spend"
  value       = "Run 'terraform destroy' after ~12 hours. Estimated total spend: ~$9.32 across both projects."
}
