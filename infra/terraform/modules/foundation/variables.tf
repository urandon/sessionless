variable "cloud_id" { type = string }
variable "folder_name" { type = string }
variable "name_prefix" { type = string }
variable "artifact_bucket_name" { type = string }
variable "artifact_bucket_max_size_bytes" { type = number }
variable "artifact_retention_days" { type = number }
variable "ydb_ru_limit" { type = number }
variable "ydb_storage_gib" { type = number }
variable "queue_visibility_timeout_seconds" { type = number }
variable "queue_retention_seconds" { type = number }
variable "queue_max_receive_count" { type = number }
variable "log_retention" { type = string }
variable "deletion_protection" { type = bool }
variable "labels" {
  type    = map(string)
  default = {}
}
