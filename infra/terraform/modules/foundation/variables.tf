variable "cloud_id" { type = string }
variable "folder_name" { type = string }
variable "name_prefix" { type = string }
variable "base_domain" {
  description = "Delegated public DNS zone owned by this environment."
  type        = string

  validation {
    condition     = length(trimspace(trimsuffix(var.base_domain, "."))) > 0
    error_message = "base_domain must not be empty."
  }
}
variable "artifact_bucket_name" { type = string }
variable "artifact_bucket_max_size_bytes" { type = number }
variable "artifact_retention_days" { type = number }
variable "artifact_cold_transition_days" { type = number }
variable "artifact_ice_transition_days" { type = number }
variable "ydb_ru_limit" { type = number }
variable "ydb_storage_gib" { type = number }
variable "queue_visibility_timeout_seconds" { type = number }
variable "queue_retention_seconds" { type = number }
variable "queue_max_receive_count" { type = number }
variable "log_retention" { type = string }
variable "deletion_protection" { type = bool }
variable "github_oidc_audience" {
  description = "Audience requested by the trusted GitHub Actions workload."
  type        = string
}
variable "github_oidc_subject" {
  description = "Exact GitHub Actions OIDC subject observed from the trusted mirrored-main workflow."
  type        = string

  validation {
    condition     = startswith(var.github_oidc_subject, "repo:") && endswith(var.github_oidc_subject, ":ref:refs/heads/main")
    error_message = "github_oidc_subject must be the exact repository subject for refs/heads/main."
  }
}
variable "labels" {
  type    = map(string)
  default = {}
}
