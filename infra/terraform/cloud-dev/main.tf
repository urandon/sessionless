locals {
  labels = merge(var.labels, {
    application = "sessionless"
    environment = "dev"
    managed-by  = "terraform"
  })
}

module "foundation" {
  source = "../modules/foundation"

  cloud_id                         = var.cloud_id
  folder_name                      = var.folder_name
  name_prefix                      = var.name_prefix
  artifact_bucket_name             = var.artifact_bucket_name
  artifact_bucket_max_size_bytes   = var.artifact_bucket_max_size_bytes
  artifact_retention_days          = var.artifact_retention_days
  ydb_ru_limit                     = var.ydb_ru_limit
  ydb_storage_gib                  = var.ydb_storage_gib
  queue_visibility_timeout_seconds = var.queue_visibility_timeout_seconds
  queue_retention_seconds          = var.queue_retention_seconds
  queue_max_receive_count          = var.queue_max_receive_count
  log_retention                    = var.log_retention
  deletion_protection              = var.deletion_protection
  labels                           = local.labels
}

module "runtime" {
  source = "../modules/runtime"

  folder_id           = module.foundation.folder_id
  name_prefix         = var.name_prefix
  service_account_ids = module.foundation.service_account_ids
  images = {
    control-blue    = "cr.yandex/${module.foundation.repository_names["control-api"]}:${var.control_blue_image_tag}"
    control-green   = "cr.yandex/${module.foundation.repository_names["control-api"]}:${var.control_green_image_tag}"
    reconciler      = "cr.yandex/${module.foundation.repository_names["reconciler"]}:${var.runtime_image_tag}"
    telegram-sender = "cr.yandex/${module.foundation.repository_names["telegram-sender"]}:${var.runtime_image_tag}"
    worker-runtime  = "cr.yandex/${module.foundation.repository_names["worker-runtime"]}:${var.runtime_image_tag}"
  }
  ydb_connection_string      = module.foundation.ydb_connection_string
  artifact_bucket_name       = module.foundation.artifact_bucket_name
  dispatch_queue_url         = module.foundation.dispatch_queue_url
  dispatch_queue_arn         = module.foundation.dispatch_queue_arn
  dispatch_dlq_arn           = module.foundation.dispatch_dlq_arn
  delivery_dlq_arn           = module.foundation.delivery_dlq_arn
  telegram_secret_id         = module.foundation.telegram_secret_id
  telegram_secret_version_id = var.telegram_secret_version_id
  log_group_id               = module.foundation.log_group_id
  labels                     = local.labels
  control_memory_mb          = var.control_memory_mb
  worker_memory_mb           = var.worker_memory_mb
  worker_timeout             = var.worker_timeout
  reconciler_cron            = var.reconciler_cron
  telegram_sender_cron       = var.telegram_sender_cron
}

module "edge" {
  source = "../modules/edge"

  folder_id                  = module.foundation.folder_id
  name_prefix                = var.name_prefix
  base_domain                = var.base_domain
  dns_zone_id                = var.dns_zone_id
  gateway_service_account_id = module.foundation.service_account_ids["gateway"]
  control_container_ids      = module.runtime.control_container_ids
  stable_slot                = var.stable_slot
  canary_weight              = var.canary_weight
  log_group_id               = module.foundation.log_group_id
  deletion_protection        = var.deletion_protection
  labels                     = local.labels
}

resource "terraform_data" "external_guardrails" {
  input = {
    billing_account_id = var.billing_account_id
    budget_id          = var.budget_id
    folder_id          = module.foundation.folder_id
  }
  lifecycle {
    precondition {
      condition     = length(trimspace(var.billing_account_id)) > 0 && length(trimspace(var.budget_id)) > 0
      error_message = "A folder-scoped Billing Budget must be created and verified before the environment can be applied."
    }
  }
}
