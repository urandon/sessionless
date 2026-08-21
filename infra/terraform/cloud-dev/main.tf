locals {
  web_origin = "https://web.dev.${trimsuffix(var.base_domain, ".")}"
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
  base_domain                      = var.base_domain
  artifact_bucket_name             = var.artifact_bucket_name
  webui_origin                     = local.web_origin
  artifact_bucket_max_size_bytes   = var.artifact_bucket_max_size_bytes
  artifact_retention_days          = var.artifact_retention_days
  artifact_cold_transition_days    = var.artifact_cold_transition_days
  artifact_ice_transition_days     = var.artifact_ice_transition_days
  ydb_ru_limit                     = var.ydb_ru_limit
  ydb_storage_gib                  = var.ydb_storage_gib
  queue_visibility_timeout_seconds = var.queue_visibility_timeout_seconds
  queue_retention_seconds          = var.queue_retention_seconds
  queue_max_receive_count          = var.queue_max_receive_count
  log_retention                    = var.log_retention
  deletion_protection              = var.deletion_protection
  github_oidc_audience             = var.github_oidc_audience
  github_oidc_subject              = var.github_oidc_subject
  github_release_oidc_subject      = var.github_release_oidc_subject
  labels                           = local.labels
}

module "runtime" {
  source = "../modules/runtime"

  folder_id                           = module.foundation.folder_id
  name_prefix                         = var.name_prefix
  service_account_ids                 = module.foundation.service_account_ids
  registry_cleaner_service_account_id = module.foundation.registry_cleaner_service_account_id
  images = {
    control-blue    = coalesce(var.control_blue_image_ref, "cr.yandex/${module.foundation.repository_names["control-api"]}:${var.control_blue_image_tag}")
    control-green   = coalesce(var.control_green_image_ref, "cr.yandex/${module.foundation.repository_names["control-api"]}:${var.control_green_image_tag}")
    reconciler      = lookup(var.runtime_image_refs, "reconciler", "cr.yandex/${module.foundation.repository_names["reconciler"]}:${var.runtime_image_tag}")
    telegram-sender = lookup(var.runtime_image_refs, "telegram-sender", "cr.yandex/${module.foundation.repository_names["telegram-sender"]}:${var.runtime_image_tag}")
    worker-runtime  = lookup(var.runtime_image_refs, "worker-runtime", "cr.yandex/${module.foundation.repository_names["worker-runtime"]}:${var.runtime_image_tag}")
  }
  image_source_shas = {
    control-blue    = var.control_blue_image_tag
    control-green   = var.control_green_image_tag
    reconciler      = var.runtime_image_tag
    telegram-sender = var.runtime_image_tag
    worker-runtime  = var.runtime_image_tag
  }
  ydb_connection_string           = module.foundation.ydb_connection_string
  artifact_bucket_name            = module.foundation.artifact_bucket_name
  dispatch_queue_url              = module.foundation.dispatch_queue_url
  dispatch_queue_arn              = module.foundation.dispatch_queue_arn
  dispatch_dlq_arn                = module.foundation.dispatch_dlq_arn
  delivery_queue_url              = module.foundation.delivery_queue_url
  delivery_queue_arn              = module.foundation.delivery_queue_arn
  delivery_dlq_arn                = module.foundation.delivery_dlq_arn
  scheduler_wake_queue_url        = module.foundation.scheduler_wake_queue_url
  scheduler_wake_queue_arn        = module.foundation.scheduler_wake_queue_arn
  scheduler_wake_dlq_arn          = module.foundation.scheduler_wake_dlq_arn
  telegram_secret_id              = module.foundation.telegram_secret_id
  telegram_secret_version_id      = var.telegram_secret_version_id
  scheduler_ymq_secret_id         = module.foundation.scheduler_ymq_secret_id
  scheduler_ymq_secret_version_id = module.foundation.scheduler_ymq_secret_version_id
  log_group_id                    = module.foundation.log_group_id
  labels                          = local.labels
  control_memory_mb               = var.control_memory_mb
  worker_memory_mb                = var.worker_memory_mb
  worker_timeout                  = var.worker_timeout
  reconciler_cron                 = var.reconciler_cron
  telegram_sender_cron            = var.telegram_sender_cron

  depends_on = [module.foundation]
}

module "edge" {
  source = "../modules/edge"

  folder_id                  = module.foundation.folder_id
  name_prefix                = var.name_prefix
  base_domain                = var.base_domain
  dns_zone_id                = module.foundation.dns_zone_id
  gateway_service_account_id = module.foundation.service_account_ids["gateway"]
  control_container_ids      = module.runtime.control_container_ids
  stable_slot                = var.stable_slot
  canary_weight              = var.canary_weight
  log_group_id               = module.foundation.log_group_id
  deletion_protection        = var.deletion_protection
  labels                     = local.labels
}

module "web" {
  source = "../modules/web"

  folder_id                           = module.foundation.folder_id
  name_prefix                         = var.name_prefix
  base_domain                         = var.base_domain
  dns_zone_id                         = module.foundation.dns_zone_id
  service_account_id                  = module.foundation.service_account_ids["web-bff"]
  gateway_service_account_id          = module.foundation.service_account_ids["web-gateway"]
  registry_cleaner_service_account_id = module.foundation.registry_cleaner_service_account_id
  source_sha                          = var.runtime_image_tag
  image_ref                           = var.web_image_ref
  ydb_connection_string               = module.foundation.ydb_connection_string
  artifact_bucket_name                = module.foundation.artifact_bucket_name
  scheduler_wake_queue_url            = module.foundation.scheduler_wake_queue_url
  web_secret_id                       = module.foundation.web_bff_secret_id
  web_secret_version_id               = var.web_bff_secret_version_id
  scheduler_ymq_secret_id             = module.foundation.scheduler_ymq_secret_id
  scheduler_ymq_secret_version_id     = module.foundation.scheduler_ymq_secret_version_id
  telegram_oidc_client_id             = var.telegram_oidc_client_id
  allowed_mcp_servers                 = var.web_allowed_mcp_servers
  max_upload_bytes                    = var.web_max_upload_bytes
  memory_mb                           = var.web_memory_mb
  concurrency                         = var.web_concurrency
  execution_timeout                   = var.web_execution_timeout
  log_group_id                        = module.foundation.log_group_id
  deletion_protection                 = var.deletion_protection
  labels                              = local.labels

  depends_on = [module.foundation]
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
