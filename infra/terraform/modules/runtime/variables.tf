variable "folder_id" { type = string }
variable "name_prefix" { type = string }
variable "service_account_ids" { type = map(string) }
variable "images" { type = map(string) }
variable "image_source_shas" {
  type = map(string)

  validation {
    condition = (
      length(setsubtract(toset(keys(var.image_source_shas)), local.required_images)) == 0 &&
      length(setsubtract(local.required_images, toset(keys(var.image_source_shas)))) == 0 &&
      alltrue([for sha in values(var.image_source_shas) : can(regex("^[0-9a-f]{40}$", sha))])
    )
    error_message = "image_source_shas must contain one full lowercase commit SHA for every runtime image slot."
  }
}
variable "registry_cleaner_service_account_id" { type = string }
variable "ydb_connection_string" { type = string }
variable "artifact_bucket_name" { type = string }
variable "dispatch_queue_url" { type = string }
variable "dispatch_queue_arn" { type = string }
variable "dispatch_dlq_arn" { type = string }
variable "delivery_queue_url" { type = string }
variable "delivery_queue_arn" { type = string }
variable "delivery_dlq_arn" { type = string }
variable "scheduler_wake_queue_url" { type = string }
variable "scheduler_wake_queue_arn" { type = string }
variable "scheduler_wake_dlq_arn" { type = string }
variable "telegram_secret_id" { type = string }
variable "telegram_secret_version_id" { type = string }
variable "scheduler_ymq_secret_id" { type = string }
variable "scheduler_ymq_secret_version_id" { type = string }
variable "log_group_id" { type = string }
variable "labels" { type = map(string) }
variable "control_memory_mb" { type = number }
variable "worker_memory_mb" { type = number }
variable "worker_timeout" { type = string }
variable "reconciler_cron" { type = string }
variable "telegram_sender_cron" { type = string }

locals {
  required_images = toset(["control-blue", "control-green", "reconciler", "telegram-sender", "worker-runtime"])
}
