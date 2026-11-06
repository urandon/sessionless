variable "folder_id" { type = string }
variable "name_prefix" { type = string }
variable "base_domain" { type = string }
variable "dns_zone_id" { type = string }
variable "gateway_service_account_id" { type = string }
variable "control_container_ids" { type = map(string) }
variable "stable_slot" {
  type = string
  validation {
    condition     = contains(["blue", "green"], var.stable_slot)
    error_message = "stable_slot must be blue or green."
  }
}
variable "canary_weight" {
  type = number
  validation {
    condition     = var.canary_weight >= 0 && var.canary_weight <= 100
    error_message = "canary_weight must be between 0 and 100."
  }
}
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
variable "deletion_protection" { type = bool }
variable "labels" { type = map(string) }
