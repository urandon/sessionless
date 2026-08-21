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
variable "webui_origin" {
  description = "Exact public HTTPS origin allowed to use browser object capabilities; paths, wildcards, queries, and fragments are forbidden."
  type        = string

  validation {
    condition = (
      var.webui_origin == trimspace(var.webui_origin) &&
      can(regex("^https://[A-Za-z0-9]([A-Za-z0-9.-]*[A-Za-z0-9])?(:[0-9]{1,5})?$", var.webui_origin)) &&
      !strcontains(var.webui_origin, "*")
    )
    error_message = "webui_origin must be one exact HTTPS origin without credentials, path, query, fragment, whitespace, or wildcards."
  }
}
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
variable "github_release_oidc_subject" {
  description = "Exact GitHub Actions OIDC subject for the protected release environment."
  type        = string

  validation {
    condition     = startswith(var.github_release_oidc_subject, "repo:") && endswith(var.github_release_oidc_subject, ":environment:release")
    error_message = "github_release_oidc_subject must be the exact repository subject for the release environment."
  }
}
variable "labels" {
  type    = map(string)
  default = {}
}
