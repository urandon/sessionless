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
variable "log_group_id" {
  description = "Explicit existing native log-group destination; set exactly one native destination."
  type        = string
  default     = null
  validation {
    condition     = var.log_group_id == null || can(regex("^[a-z0-9]{20}$", var.log_group_id))
    error_message = "log_group_id must be null or one exact 20-character native log-group ID."
  }
  validation {
    condition     = (var.log_group_id != null) != (var.log_folder_id != null)
    error_message = "Set exactly one of log_group_id or log_folder_id; no implicit native logging destination is allowed."
  }
}
variable "log_folder_id" {
  description = "Explicit native folder-default log destination; not proof of Monium delivery or retention."
  type        = string
  default     = null
  validation {
    condition     = var.log_folder_id == null || can(regex("^[a-z0-9]{20}$", var.log_folder_id))
    error_message = "log_folder_id must be null or one exact 20-character folder ID."
  }
  validation {
    condition     = var.log_folder_id == null || var.log_folder_id == var.folder_id
    error_message = "log_folder_id must be the same explicit deployment folder_id."
  }
}
variable "labels" { type = map(string) }
variable "control_memory_mb" { type = number }
variable "worker_memory_mb" { type = number }
variable "worker_timeout" { type = string }
variable "reconciler_cron" { type = string }
variable "telegram_sender_cron" { type = string }

locals {
  required_images = toset(["control-blue", "control-green", "reconciler", "telegram-sender", "worker-runtime"])
}
