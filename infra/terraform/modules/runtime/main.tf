locals {
  common_environment = {
    YDB_CONNECTION_STRING = var.ydb_connection_string
    S3_ENDPOINT           = "https://storage.yandexcloud.net"
    S3_REGION             = "ru-central1"
    S3_BUCKET             = var.artifact_bucket_name
    S3_FORCE_PATH_STYLE   = "true"
  }
  trigger_member = "serviceAccount:${var.service_account_ids["trigger"]}"
  gateway_member = "serviceAccount:${var.service_account_ids["gateway"]}"
}

resource "yandex_serverless_container" "control" {
  for_each           = toset(["blue", "green"])
  folder_id          = var.folder_id
  name               = "${var.name_prefix}-control-${each.key}"
  description        = "Private Sessionless control API ${each.key} slot"
  memory             = var.control_memory_mb
  cores              = 1
  core_fraction      = 100
  concurrency        = 4
  execution_timeout  = "30s"
  service_account_id = var.service_account_ids["api"]
  labels             = var.labels

  runtime { type = "http" }
  image {
    url = var.images["control-${each.key}"]
    environment = merge(local.common_environment, {
      DEFAULT_COMPUTE_PROVIDER = "deterministic"
      TELEGRAM_SOURCE_ID       = "bot-primary"
    })
  }
  metadata_options {
    aws_v1_http_endpoint = 1
    gce_http_endpoint    = 1
  }
  secrets {
    id                   = var.telegram_secret_id
    version_id           = var.telegram_secret_version_id
    key                  = "bot-token"
    environment_variable = "TELEGRAM_BOT_TOKEN"
  }
  secrets {
    id                   = var.telegram_secret_id
    version_id           = var.telegram_secret_version_id
    key                  = "webhook-secret"
    environment_variable = "TELEGRAM_WEBHOOK_SECRET"
  }
  secrets {
    id                   = var.telegram_secret_id
    version_id           = var.telegram_secret_version_id
    key                  = "identity-hmac-key"
    environment_variable = "TELEGRAM_IDENTITY_HMAC_KEY"
  }
  log_options {
    log_group_id = var.log_group_id
    min_level    = "INFO"
  }
}

resource "yandex_serverless_container" "reconciler" {
  folder_id          = var.folder_id
  name               = "${var.name_prefix}-reconciler"
  description        = "Timer-driven bounded scheduler and recovery pass"
  memory             = 256
  cores              = 1
  core_fraction      = 100
  concurrency        = 1
  execution_timeout  = "60s"
  service_account_id = var.service_account_ids["scheduler"]
  labels             = var.labels
  runtime { type = "http" }
  image {
    url = var.images["reconciler"]
    environment = {
      SERVERLESS_TRIGGER_HTTP = "true"
      YDB_CONNECTION_STRING   = var.ydb_connection_string
      QUEUE_ENDPOINT          = "https://message-queue.api.cloud.yandex.net"
      QUEUE_REGION            = "ru-central1"
      DISPATCH_QUEUE_URL      = var.dispatch_queue_url
    }
  }
  metadata_options {
    aws_v1_http_endpoint = 1
    gce_http_endpoint    = 1
  }
  log_options {
    log_group_id = var.log_group_id
    min_level    = "INFO"
  }
}

resource "yandex_serverless_container" "worker" {
  folder_id          = var.folder_id
  name               = "${var.name_prefix}-worker"
  description        = "YMQ-triggered isolated harness-neutral worker"
  memory             = var.worker_memory_mb
  cores              = 1
  core_fraction      = 100
  concurrency        = 1
  execution_timeout  = var.worker_timeout
  service_account_id = var.service_account_ids["worker"]
  labels             = var.labels
  runtime { type = "http" }
  image {
    url = var.images["worker-runtime"]
    environment = merge(local.common_environment, {
      SERVERLESS_TRIGGER_HTTP   = "true"
      WORKER_ID                 = "serverless-worker"
      WORKER_LEASE_TTL          = "2m"
      WORKER_MAX_DELIVERY_COUNT = "5"
    })
  }
  metadata_options {
    aws_v1_http_endpoint = 1
    gce_http_endpoint    = 1
  }
  mounts {
    mount_point_path = "/tmp/sessionless-worker"
    mode             = "rw"
    ephemeral_disk { size_gb = 1 }
  }
  log_options {
    log_group_id = var.log_group_id
    min_level    = "INFO"
  }
}

resource "yandex_serverless_container" "telegram_sender" {
  folder_id          = var.folder_id
  name               = "${var.name_prefix}-telegram-sender"
  description        = "Timer-driven bounded Telegram delivery pass"
  memory             = 256
  cores              = 1
  core_fraction      = 100
  concurrency        = 1
  execution_timeout  = "60s"
  service_account_id = var.service_account_ids["telegram-sender"]
  labels             = var.labels
  runtime { type = "http" }
  image {
    url = var.images["telegram-sender"]
    environment = merge(local.common_environment, {
      SERVERLESS_TRIGGER_HTTP = "true"
      TELEGRAM_API_BASE_URL   = "https://api.telegram.org"
    })
  }
  metadata_options {
    aws_v1_http_endpoint = 1
    gce_http_endpoint    = 1
  }
  secrets {
    id                   = var.telegram_secret_id
    version_id           = var.telegram_secret_version_id
    key                  = "bot-token"
    environment_variable = "TELEGRAM_BOT_TOKEN"
  }
  log_options {
    log_group_id = var.log_group_id
    min_level    = "INFO"
  }
}

resource "yandex_serverless_container_iam_binding" "control_invoker" {
  for_each     = yandex_serverless_container.control
  container_id = each.value.id
  role         = "serverless.containers.invoker"
  members      = [local.gateway_member]
}

resource "yandex_serverless_container_iam_binding" "trigger_invoker" {
  for_each = {
    reconciler      = yandex_serverless_container.reconciler.id
    worker          = yandex_serverless_container.worker.id
    telegram-sender = yandex_serverless_container.telegram_sender.id
  }
  container_id = each.value
  role         = "serverless.containers.invoker"
  members      = [local.trigger_member]
}

resource "yandex_function_trigger" "worker" {
  folder_id   = var.folder_id
  name        = "${var.name_prefix}-dispatch"
  description = "Deliver one dispatch envelope to the worker per invocation"
  labels      = var.labels
  container {
    id                 = yandex_serverless_container.worker.id
    path               = "/invoke"
    service_account_id = var.service_account_ids["trigger"]
  }
  message_queue {
    queue_id           = var.dispatch_queue_arn
    service_account_id = var.service_account_ids["trigger"]
    batch_cutoff       = "1"
    batch_size         = "1"
    visibility_timeout = "900"
  }
  depends_on = [yandex_serverless_container_iam_binding.trigger_invoker]
}

resource "yandex_function_trigger" "reconciler" {
  folder_id   = var.folder_id
  name        = "${var.name_prefix}-reconciler"
  description = "Run a bounded scheduler/recovery pass"
  labels      = var.labels
  container {
    id                 = yandex_serverless_container.reconciler.id
    path               = "/invoke"
    service_account_id = var.service_account_ids["trigger"]
    retry_attempts     = "3"
    retry_interval     = "10"
  }
  timer {
    cron_expression = var.reconciler_cron
    payload         = "{\"source\":\"timer\"}"
  }
  dlq {
    queue_id           = var.dispatch_dlq_arn
    service_account_id = var.service_account_ids["trigger"]
  }
  depends_on = [yandex_serverless_container_iam_binding.trigger_invoker]
}

resource "yandex_function_trigger" "telegram_sender" {
  folder_id   = var.folder_id
  name        = "${var.name_prefix}-telegram-sender"
  description = "Run a bounded Telegram delivery pass"
  labels      = var.labels
  container {
    id                 = yandex_serverless_container.telegram_sender.id
    path               = "/invoke"
    service_account_id = var.service_account_ids["trigger"]
    retry_attempts     = "3"
    retry_interval     = "10"
  }
  timer {
    cron_expression = var.telegram_sender_cron
    payload         = "{\"source\":\"timer\"}"
  }
  dlq {
    queue_id           = var.delivery_dlq_arn
    service_account_id = var.service_account_ids["trigger"]
  }
  depends_on = [yandex_serverless_container_iam_binding.trigger_invoker]
}
