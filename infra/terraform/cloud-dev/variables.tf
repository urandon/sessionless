variable "cloud_id" {
  description = "Existing Yandex Cloud ID that will contain the isolated dev folder."
  type        = string
  validation {
    condition     = length(trimspace(var.cloud_id)) > 0
    error_message = "cloud_id must not be empty."
  }
}

variable "folder_name" {
  type    = string
  default = "sessionless-dev"
}
variable "name_prefix" {
  type    = string
  default = "sessionless-dev"
}
variable "base_domain" {
  description = "Public subdomain delegated to this environment's Yandex Cloud DNS zone."
  type        = string

  validation {
    condition     = length(trimspace(trimsuffix(var.base_domain, "."))) > 0
    error_message = "base_domain must not be empty."
  }
}
variable "artifact_bucket_name" { type = string }
variable "telegram_secret_version_id" {
  description = "Non-secret Lockbox version ID loaded by scripts/cloud-secret-load.sh outside Terraform."
  type        = string
}
variable "control_blue_image_tag" { type = string }
variable "control_green_image_tag" { type = string }
variable "runtime_image_tag" { type = string }
variable "stable_slot" {
  type    = string
  default = "blue"
}
variable "canary_weight" {
  type    = number
  default = 0
}
variable "ydb_ru_limit" {
  type    = number
  default = 50
}
variable "ydb_storage_gib" {
  type    = number
  default = 10
}
variable "artifact_bucket_max_size_bytes" {
  type    = number
  default = 10737418240
}
variable "artifact_retention_days" {
  type    = number
  default = 30
}
variable "queue_visibility_timeout_seconds" {
  type    = number
  default = 900
}
variable "queue_retention_seconds" {
  type    = number
  default = 345600
}
variable "queue_max_receive_count" {
  type    = number
  default = 5
}
variable "log_retention" {
  type    = string
  default = "168h"
}
variable "deletion_protection" {
  description = "Protect state-bearing cloud resources. Disable only in a separately reviewed apply before destroy."
  type        = bool
  default     = true
}
variable "control_memory_mb" {
  type    = number
  default = 256
}
variable "worker_memory_mb" {
  type    = number
  default = 1024
}
variable "worker_timeout" {
  type    = string
  default = "900s"
}
variable "reconciler_cron" {
  type    = string
  default = "* * * * ? *"
}
variable "telegram_sender_cron" {
  type    = string
  default = "* * * * ? *"
}
variable "labels" {
  type    = map(string)
  default = {}
}

variable "billing_account_id" {
  description = "Billing account whose external budget gate must cover the dev folder."
  type        = string
}
variable "budget_id" {
  description = "Existing Billing Budget ID verified by scripts/cloud-preflight.sh; budgets are not supported by the provider."
  type        = string
}
