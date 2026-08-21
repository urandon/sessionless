variable "cloud_id" {
  description = "Yandex Cloud identifier containing the management folder."
  type        = string

  validation {
    condition     = length(trimspace(var.cloud_id)) > 0
    error_message = "cloud_id must not be empty."
  }
}

variable "management_folder_id" {
  description = "Existing folder that owns non-destroyable Terraform bootstrap resources."
  type        = string

  validation {
    condition     = length(trimspace(var.management_folder_id)) > 0
    error_message = "management_folder_id must not be empty."
  }
}

variable "zone" {
  description = "Default provider zone; bootstrap resources themselves are regional/global."
  type        = string
  default     = "ru-central1-a"
}

variable "state_bucket_name" {
  description = "Globally unique private Object Storage bucket for Terraform state."
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$", var.state_bucket_name))
    error_message = "state_bucket_name must be a valid 3-63 character bucket name."
  }
}

variable "state_bucket_max_size_bytes" {
  description = "Hard state-bucket size ceiling."
  type        = number
  default     = 1073741824

  validation {
    condition     = var.state_bucket_max_size_bytes >= 104857600
    error_message = "state_bucket_max_size_bytes must be at least 100 MiB."
  }
}

variable "state_noncurrent_retention_days" {
  description = "Retention for noncurrent state object versions."
  type        = number
  default     = 90

  validation {
    condition     = var.state_noncurrent_retention_days >= 30
    error_message = "Retain noncurrent state for at least 30 days."
  }
}

variable "lock_database_name" {
  description = "Dedicated YDB Serverless database name for deployment leases."
  type        = string
  default     = "sessionless-terraform-locks"
}

variable "lock_database_ru_limit" {
  description = "Maximum on-demand request units per second for the lock database."
  type        = number
  default     = 10

  validation {
    condition     = var.lock_database_ru_limit >= 1 && var.lock_database_ru_limit <= 100
    error_message = "lock_database_ru_limit must be between 1 and 100 RU/s."
  }
}

variable "lock_database_storage_gib" {
  description = "Storage ceiling in GiB for the lock database."
  type        = number
  default     = 1

  validation {
    condition     = var.lock_database_storage_gib >= 1 && var.lock_database_storage_gib <= 10
    error_message = "lock_database_storage_gib must be between 1 and 10 GiB."
  }
}

variable "registry_cleaner_service_account_id" {
  description = "Optional cloud-dev registry cleaner identity allowed to use only the deployment-lock database."
  type        = string
  default     = ""

  validation {
    condition = (
      var.registry_cleaner_service_account_id == "" ||
      can(regex("^[a-z0-9]{20}$", var.registry_cleaner_service_account_id))
    )
    error_message = "registry_cleaner_service_account_id must be empty or a 20-character service account ID."
  }
}

variable "labels" {
  description = "Additional non-secret labels for bootstrap resources."
  type        = map(string)
  default     = {}
}
