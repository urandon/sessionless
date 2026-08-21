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
variable "web_bff_secret_version_id" {
  description = "Non-secret Lockbox version ID loaded by scripts/cloud-web-secret-load.sh outside Terraform."
  type        = string
}
variable "telegram_oidc_client_id" {
  description = "Non-secret numeric Telegram OIDC client identifier registered for the Web hostname."
  type        = string
  validation {
    condition     = can(regex("^[0-9]+$", var.telegram_oidc_client_id))
    error_message = "telegram_oidc_client_id must contain only decimal digits."
  }
}
variable "web_image_ref" {
  description = "Required immutable web-bff image reference generated from a publication manifest."
  type        = string
  validation {
    condition     = can(regex("^cr\\.yandex/[^/]+/web-bff@sha256:[0-9a-f]{64}$", var.web_image_ref))
    error_message = "web_image_ref must be an immutable cr.yandex web-bff digest reference."
  }
}
variable "web_allowed_mcp_servers" {
  type    = list(string)
  default = []
}
variable "web_max_upload_bytes" {
  type    = number
  default = 33554432
}
variable "web_memory_mb" {
  type    = number
  default = 256
}
variable "web_concurrency" {
  type    = number
  default = 4
}
variable "web_execution_timeout" {
  type    = string
  default = "30s"
}
variable "control_blue_image_tag" {
  type = string
  validation {
    condition     = can(regex("^[0-9a-f]{40}$", var.control_blue_image_tag))
    error_message = "control_blue_image_tag must be a full lowercase commit SHA."
  }
}
variable "control_green_image_tag" {
  type = string
  validation {
    condition     = can(regex("^[0-9a-f]{40}$", var.control_green_image_tag))
    error_message = "control_green_image_tag must be a full lowercase commit SHA."
  }
}
variable "runtime_image_tag" {
  type = string
  validation {
    condition     = can(regex("^[0-9a-f]{40}$", var.runtime_image_tag))
    error_message = "runtime_image_tag must be a full lowercase commit SHA."
  }
}
variable "control_blue_image_ref" {
  description = "Optional immutable control image reference generated from a publication manifest."
  type        = string
  default     = null
  nullable    = true

  validation {
    condition     = var.control_blue_image_ref == null || can(regex("^cr\\.yandex/.+@sha256:[0-9a-f]{64}$", var.control_blue_image_ref))
    error_message = "control_blue_image_ref must be an immutable cr.yandex digest reference."
  }
}
variable "control_green_image_ref" {
  description = "Optional immutable control image reference generated from a publication manifest."
  type        = string
  default     = null
  nullable    = true

  validation {
    condition     = var.control_green_image_ref == null || can(regex("^cr\\.yandex/.+@sha256:[0-9a-f]{64}$", var.control_green_image_ref))
    error_message = "control_green_image_ref must be an immutable cr.yandex digest reference."
  }
}
variable "runtime_image_refs" {
  description = "Optional immutable reconciler, sender, and worker digest references generated from a publication manifest."
  type        = map(string)
  default     = {}

  validation {
    condition = alltrue([
      for name, ref in var.runtime_image_refs :
      contains(["reconciler", "telegram-sender", "worker-runtime"], name) &&
      can(regex("^cr\\.yandex/.+@sha256:[0-9a-f]{64}$", ref))
    ])
    error_message = "runtime_image_refs accepts only immutable reconciler, telegram-sender, and worker-runtime digest references."
  }
}
variable "github_oidc_audience" {
  description = "Audience used for the GitHub-to-Yandex workload identity exchange."
  type        = string
  default     = "https://github.com/urandon"
}
variable "github_oidc_subject" {
  description = "Exact subject printed by the safe GitHub OIDC claim-inspection workflow."
  type        = string
}
variable "github_release_oidc_subject" {
  description = "Exact subject printed from the protected GitHub release environment."
  type        = string

  validation {
    condition     = var.github_release_oidc_subject == "repo:urandon/sessionless:environment:release"
    error_message = "github_release_oidc_subject must be the exact urandon/sessionless release-environment subject."
  }
}
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
variable "artifact_cold_transition_days" {
  description = "Age of immutable tenant payloads before STANDARD-to-COLD transition."
  type        = number
  default     = 30

  validation {
    condition     = var.artifact_cold_transition_days >= 1
    error_message = "artifact_cold_transition_days must be at least one day."
  }
}
variable "artifact_ice_transition_days" {
  description = "Age of immutable tenant payloads before COLD-to-ICE transition; ICE has a 12-month minimum billable duration."
  type        = number
  default     = 365

  validation {
    condition     = var.artifact_ice_transition_days >= 365 && var.artifact_ice_transition_days > var.artifact_cold_transition_days
    error_message = "artifact_ice_transition_days must be at least 365 and later than the COLD transition."
  }
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
  default = "0 */6 * * ? *"
}
variable "telegram_sender_cron" {
  type    = string
  default = "0 */6 * * ? *"
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
