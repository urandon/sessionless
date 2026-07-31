locals {
  labels = merge(var.labels, {
    application = "sessionless"
    environment = "dev"
    managed-by  = "terraform"
  })
  account_names = toset(["deploy", "api", "scheduler", "worker", "telegram-sender", "trigger", "gateway"])
  account_roles = {
    api             = ["logging.writer"]
    scheduler       = ["ymq.writer", "logging.writer"]
    worker          = ["logging.writer"]
    telegram-sender = ["logging.writer"]
    trigger         = ["ymq.reader", "ymq.writer", "logging.writer"]
    gateway         = ["logging.writer"]
    deploy = [
      "iam.serviceAccounts.user", "container-registry.admin", "serverless.containers.admin",
      "api-gateway.admin", "ydb.admin", "storage.admin", "ymq.admin", "lockbox.admin",
      "kms.admin", "logging.admin", "dns.admin", "certificate-manager.admin",
    ]
  }
  role_grants = flatten([
    for account, roles in local.account_roles : [
      for role in roles : { key = "${account}:${role}", account = account, role = role }
    ]
  ])
}

resource "yandex_resourcemanager_folder" "environment" {
  cloud_id    = var.cloud_id
  name        = var.folder_name
  description = "Isolated Sessionless cloud development environment"
  labels      = local.labels
}

resource "yandex_iam_service_account" "runtime" {
  for_each    = local.account_names
  folder_id   = yandex_resourcemanager_folder.environment.id
  name        = "${var.name_prefix}-${each.key}"
  description = "Sessionless dev ${each.key} identity"
  labels      = local.labels
}

resource "yandex_resourcemanager_folder_iam_member" "runtime" {
  for_each  = { for grant in local.role_grants : grant.key => grant }
  folder_id = yandex_resourcemanager_folder.environment.id
  role      = each.value.role
  member    = "serviceAccount:${yandex_iam_service_account.runtime[each.value.account].id}"
}

resource "yandex_ydb_database_serverless" "application" {
  folder_id           = yandex_resourcemanager_folder.environment.id
  name                = "${var.name_prefix}-state"
  description         = "Authoritative Sessionless dev operational state"
  deletion_protection = var.deletion_protection
  labels              = local.labels

  serverless_database {
    enable_throttling_rcu_limit = true
    provisioned_rcu_limit       = 0
    throttling_rcu_limit        = var.ydb_ru_limit
    storage_size_limit          = var.ydb_storage_gib
  }

}

resource "yandex_ydb_database_iam_binding" "runtime_editor" {
  database_id = yandex_ydb_database_serverless.application.id
  role        = "ydb.editor"
  members = [for name in ["api", "scheduler", "worker", "telegram-sender"] :
    "serviceAccount:${yandex_iam_service_account.runtime[name].id}"
  ]
}

resource "yandex_storage_bucket" "artifacts" {
  folder_id               = yandex_resourcemanager_folder.environment.id
  bucket                  = var.artifact_bucket_name
  max_size                = var.artifact_bucket_max_size_bytes
  force_destroy           = false
  disabled_statickey_auth = true
  tags                    = local.labels

  anonymous_access_flags {
    read        = false
    list        = false
    config_read = false
  }
  versioning { enabled = true }
  lifecycle_rule {
    id      = "expire-noncurrent-artifacts"
    enabled = true
    noncurrent_version_expiration { days = var.artifact_retention_days }
    abort_incomplete_multipart_upload_days = 1
  }
}

resource "yandex_storage_bucket_iam_binding" "runtime_editor" {
  bucket = yandex_storage_bucket.artifacts.bucket
  role   = "storage.editor"
  members = [for name in ["api", "worker"] :
    "serviceAccount:${yandex_iam_service_account.runtime[name].id}"
  ]
}

resource "yandex_storage_bucket_iam_binding" "sender_viewer" {
  bucket  = yandex_storage_bucket.artifacts.bucket
  role    = "storage.viewer"
  members = ["serviceAccount:${yandex_iam_service_account.runtime["telegram-sender"].id}"]
}

resource "yandex_message_queue" "dispatch_dlq" {
  name                      = "${var.name_prefix}-dispatch-dlq"
  message_retention_seconds = var.queue_retention_seconds
  tags                      = local.labels
}

resource "yandex_message_queue" "dispatch" {
  name                       = "${var.name_prefix}-dispatch"
  message_retention_seconds  = var.queue_retention_seconds
  visibility_timeout_seconds = var.queue_visibility_timeout_seconds
  receive_wait_time_seconds  = 20
  redrive_policy = jsonencode({
    deadLetterTargetArn = yandex_message_queue.dispatch_dlq.arn
    maxReceiveCount     = var.queue_max_receive_count
  })
  tags = local.labels
}

resource "yandex_message_queue" "delivery_dlq" {
  name                      = "${var.name_prefix}-delivery-dlq"
  message_retention_seconds = var.queue_retention_seconds
  tags                      = local.labels
}

resource "yandex_message_queue" "delivery" {
  name                       = "${var.name_prefix}-delivery"
  message_retention_seconds  = var.queue_retention_seconds
  visibility_timeout_seconds = 120
  receive_wait_time_seconds  = 20
  redrive_policy = jsonencode({
    deadLetterTargetArn = yandex_message_queue.delivery_dlq.arn
    maxReceiveCount     = var.queue_max_receive_count
  })
  tags = local.labels
}

resource "yandex_container_registry" "application" {
  folder_id = yandex_resourcemanager_folder.environment.id
  name      = "${var.name_prefix}-runtime"
  labels    = local.labels
}

resource "yandex_container_repository" "runtime" {
  for_each = toset(["control-api", "reconciler", "telegram-sender", "worker-runtime"])
  name     = "${yandex_container_registry.application.id}/${each.key}"
}

resource "yandex_container_repository_lifecycle_policy" "runtime" {
  for_each      = yandex_container_repository.runtime
  name          = "${var.name_prefix}-${each.key}-retention"
  repository_id = each.value.id
  status        = "active"
  rule {
    description   = "Retain the ten newest images and expire older images after 30 days"
    retained_top  = 10
    expire_period = "720h"
  }
}

resource "yandex_kms_symmetric_key" "secrets" {
  folder_id           = yandex_resourcemanager_folder.environment.id
  name                = "${var.name_prefix}-secrets"
  description         = "Envelope encryption key for Sessionless dev secrets"
  rotation_period     = "8760h"
  deletion_protection = var.deletion_protection
  labels              = local.labels
}

resource "yandex_lockbox_secret" "telegram" {
  folder_id           = yandex_resourcemanager_folder.environment.id
  name                = "${var.name_prefix}-telegram"
  description         = "Metadata only; payload versions are loaded outside Terraform"
  kms_key_id          = yandex_kms_symmetric_key.secrets.id
  deletion_protection = var.deletion_protection
  labels              = local.labels
}

resource "yandex_lockbox_secret_iam_member" "telegram" {
  for_each  = toset(["api", "telegram-sender"])
  secret_id = yandex_lockbox_secret.telegram.id
  role      = "lockbox.payloadViewer"
  member    = "serviceAccount:${yandex_iam_service_account.runtime[each.key].id}"
}

resource "yandex_logging_group" "application" {
  folder_id        = yandex_resourcemanager_folder.environment.id
  name             = "${var.name_prefix}-runtime"
  description      = "Structured runtime, trigger, and gateway logs"
  retention_period = var.log_retention
  labels           = local.labels
}
