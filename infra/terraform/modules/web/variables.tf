variable "folder_id" { type = string }
variable "name_prefix" { type = string }
variable "base_domain" { type = string }
variable "dns_zone_id" { type = string }
variable "service_account_id" { type = string }
variable "gateway_service_account_id" { type = string }
variable "registry_cleaner_service_account_id" { type = string }
variable "source_sha" {
  type = string
  validation {
    condition     = can(regex("^[0-9a-f]{40}$", var.source_sha))
    error_message = "source_sha must be a full lowercase commit SHA."
  }
}
variable "image_ref" {
  description = "Immutable AMD64 Web BFF image reference from the publication manifest."
  type        = string
  validation {
    condition     = can(regex("^cr\\.yandex/[^/]+/web-bff@sha256:[0-9a-f]{64}$", var.image_ref))
    error_message = "image_ref must be an immutable cr.yandex web-bff digest reference."
  }
}
variable "ydb_connection_string" { type = string }
variable "artifact_bucket_name" { type = string }
variable "scheduler_wake_queue_url" { type = string }
variable "web_secret_id" { type = string }
variable "web_secret_version_id" { type = string }
variable "scheduler_ymq_secret_id" { type = string }
variable "scheduler_ymq_secret_version_id" { type = string }
variable "telegram_oidc_client_id" {
  description = "Non-secret numeric Telegram OIDC client identifier."
  type        = string
  validation {
    condition     = can(regex("^[0-9]+$", var.telegram_oidc_client_id))
    error_message = "telegram_oidc_client_id must contain only decimal digits."
  }
}
variable "allowed_mcp_servers" {
  type    = list(string)
  default = []
}
variable "max_upload_bytes" {
  type    = number
  default = 33554432
  validation {
    condition     = var.max_upload_bytes > 0 && var.max_upload_bytes <= 67108864
    error_message = "max_upload_bytes must be positive and no greater than 64 MiB."
  }
}
variable "memory_mb" {
  type    = number
  default = 256
  validation {
    condition     = var.memory_mb >= 128 && var.memory_mb <= 512 && var.memory_mb % 128 == 0
    error_message = "memory_mb must be a 128 MiB multiple between 128 and 512."
  }
}
variable "concurrency" {
  type    = number
  default = 4
  validation {
    condition     = var.concurrency >= 1 && var.concurrency <= 8
    error_message = "concurrency must remain between 1 and 8 for the dev cost envelope."
  }
}
variable "execution_timeout" {
  type    = string
  default = "30s"
  validation {
    condition     = contains(["10s", "15s", "20s", "30s"], var.execution_timeout)
    error_message = "execution_timeout must remain at or below 30 seconds."
  }
}
variable "log_group_id" { type = string }
variable "deletion_protection" { type = bool }
variable "labels" {
  type    = map(string)
  default = {}
}
