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
variable "log_group_id" { type = string }
variable "deletion_protection" { type = bool }
variable "labels" { type = map(string) }
