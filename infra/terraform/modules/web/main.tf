locals {
  fqdn                  = "web.dev.${trimsuffix(var.base_domain, ".")}"
  origin                = "https://${local.fqdn}"
  object_storage_origin = "https://${var.artifact_bucket_name}.storage.yandexcloud.net"
  gateway_integration = {
    type               = "serverless_containers"
    container_id       = yandex_serverless_container.web.id
    service_account_id = var.gateway_service_account_id
  }
  ok_response        = { "200" = { description = "Web BFF response" } }
  redirect_response  = { "303" = { description = "Authentication redirect" } }
  session_parameter  = { name = "session_id", in = "path", required = true, schema = { type = "string" } }
  upload_parameter   = { name = "upload_id", in = "path", required = true, schema = { type = "string" } }
  run_parameter      = { name = "run_id", in = "path", required = true, schema = { type = "string" } }
  sequence_parameter = { name = "sequence", in = "path", required = true, schema = { type = "integer" } }
  index_parameter    = { name = "index", in = "path", required = true, schema = { type = "integer" } }
  manifest_parameter = { name = "manifest_id", in = "path", required = true, schema = { type = "string" } }
  api_paths = {
    "/healthz"                  = { get = { operationId = "webHealth", responses = local.ok_response, "x-yc-apigateway-integration" = local.gateway_integration } }
    "/readyz"                   = { get = { operationId = "webReady", responses = local.ok_response, "x-yc-apigateway-integration" = local.gateway_integration } }
    "/version"                  = { get = { operationId = "webVersion", responses = local.ok_response, "x-yc-apigateway-integration" = local.gateway_integration } }
    "/auth/telegram/start"      = { get = { operationId = "webOIDCStart", responses = local.redirect_response, "x-yc-apigateway-integration" = local.gateway_integration } }
    "/auth/telegram/callback"   = { get = { operationId = "webOIDCCallback", responses = local.redirect_response, "x-yc-apigateway-integration" = local.gateway_integration } }
    "/auth/logout"              = { post = { operationId = "webLogout", responses = local.ok_response, "x-yc-apigateway-integration" = local.gateway_integration } }
    "/api/web/v1/me"            = { get = { operationId = "webMe", responses = local.ok_response, "x-yc-apigateway-integration" = local.gateway_integration } }
    "/api/web/v1/tenants"       = { get = { operationId = "webTenants", responses = local.ok_response, "x-yc-apigateway-integration" = local.gateway_integration } }
    "/api/web/v1/active-tenant" = { post = { operationId = "webActiveTenant", responses = local.ok_response, "x-yc-apigateway-integration" = local.gateway_integration } }
    "/api/web/v1/sessions" = {
      get  = { operationId = "webSessionsList", responses = local.ok_response, "x-yc-apigateway-integration" = local.gateway_integration }
      post = { operationId = "webSessionsCreate", responses = local.ok_response, "x-yc-apigateway-integration" = local.gateway_integration }
    }
    "/api/web/v1/sessions/{session_id}"                                                                  = { parameters = [local.session_parameter], get = { operationId = "webSession", responses = local.ok_response, "x-yc-apigateway-integration" = local.gateway_integration } }
    "/api/web/v1/sessions/{session_id}/events"                                                           = { parameters = [local.session_parameter], get = { operationId = "webSessionEvents", responses = local.ok_response, "x-yc-apigateway-integration" = local.gateway_integration } }
    "/api/web/v1/sessions/{session_id}/runs"                                                             = { parameters = [local.session_parameter], get = { operationId = "webSessionRuns", responses = local.ok_response, "x-yc-apigateway-integration" = local.gateway_integration } }
    "/api/web/v1/sessions/{session_id}/compute"                                                          = { parameters = [local.session_parameter], get = { operationId = "webSessionCompute", responses = local.ok_response, "x-yc-apigateway-integration" = local.gateway_integration } }
    "/api/web/v1/sessions/{session_id}/archive"                                                          = { parameters = [local.session_parameter], post = { operationId = "webSessionArchive", responses = local.ok_response, "x-yc-apigateway-integration" = local.gateway_integration } }
    "/api/web/v1/sessions/{session_id}/messages"                                                         = { parameters = [local.session_parameter], post = { operationId = "webSessionMessage", responses = local.ok_response, "x-yc-apigateway-integration" = local.gateway_integration } }
    "/api/web/v1/uploads"                                                                                = { post = { operationId = "webUploadCreate", responses = local.ok_response, "x-yc-apigateway-integration" = local.gateway_integration } }
    "/api/web/v1/uploads/{upload_id}/commit"                                                             = { parameters = [local.upload_parameter], post = { operationId = "webUploadCommit", responses = local.ok_response, "x-yc-apigateway-integration" = local.gateway_integration } }
    "/api/web/v1/runs/{run_id}"                                                                          = { parameters = [local.run_parameter], get = { operationId = "webRun", responses = local.ok_response, "x-yc-apigateway-integration" = local.gateway_integration } }
    "/api/web/v1/sessions/{session_id}/events/{sequence}/attachments/{index}"                            = { parameters = [local.session_parameter, local.sequence_parameter, local.index_parameter], get = { operationId = "webAttachment", responses = local.ok_response, "x-yc-apigateway-integration" = local.gateway_integration } }
    "/api/web/v1/sessions/{session_id}/runs/{run_id}/artifact-manifests/{manifest_id}/artifacts/{index}" = { parameters = [local.session_parameter, local.run_parameter, local.manifest_parameter, local.index_parameter], get = { operationId = "webArtifact", responses = local.ok_response, "x-yc-apigateway-integration" = local.gateway_integration } }
  }
  document_paths = {
    "/" = {
      get  = { operationId = "webRoot", responses = local.ok_response, "x-yc-apigateway-integration" = local.gateway_integration }
      head = { operationId = "webRootHead", responses = local.ok_response, "x-yc-apigateway-integration" = local.gateway_integration }
    }
    "/login" = {
      get  = { operationId = "webLogin", responses = local.ok_response, "x-yc-apigateway-integration" = local.gateway_integration }
      head = { operationId = "webLoginHead", responses = local.ok_response, "x-yc-apigateway-integration" = local.gateway_integration }
    }
    "/sessions/{session_id}" = {
      parameters = [local.session_parameter]
      get        = { operationId = "webSessionDocument", responses = local.ok_response, "x-yc-apigateway-integration" = local.gateway_integration }
      head       = { operationId = "webSessionDocumentHead", responses = local.ok_response, "x-yc-apigateway-integration" = local.gateway_integration }
    }
    "/_app/{asset+}" = {
      parameters = [{ name = "asset", in = "path", required = true, schema = { type = "string" } }]
      get        = { operationId = "webAsset", responses = local.ok_response, "x-yc-apigateway-integration" = local.gateway_integration }
      head       = { operationId = "webAssetHead", responses = local.ok_response, "x-yc-apigateway-integration" = local.gateway_integration }
    }
  }
}

resource "yandex_serverless_container" "web" {
  folder_id          = var.folder_id
  name               = "${var.name_prefix}-web-bff"
  description        = "Private same-origin Sessionless Web UI and BFF"
  memory             = var.memory_mb
  cores              = 1
  core_fraction      = 100
  concurrency        = var.concurrency
  execution_timeout  = var.execution_timeout
  service_account_id = var.service_account_id
  labels = merge(var.labels, {
    component     = "web-bff"
    slot          = "singleton"
    source-commit = var.source_sha
  })

  runtime { type = "http" }
  provision_policy { min_instances = 0 }
  image {
    url = var.image_ref
    environment = {
      SESSIONLESS_ENVIRONMENT     = "cloud-dev"
      WEB_BASE_URL                = local.origin
      WEB_OBJECT_STORAGE_ORIGIN   = local.object_storage_origin
      WEB_MAX_UPLOAD_BYTES        = tostring(var.max_upload_bytes)
      WEB_ALLOWED_MCP_SERVERS     = join(",", var.allowed_mcp_servers)
      TELEGRAM_OIDC_CLIENT_ID     = var.telegram_oidc_client_id
      YDB_CONNECTION_STRING       = var.ydb_connection_string
      YDB_METADATA_CREDENTIALS    = "1"
      S3_ENDPOINT                 = "https://storage.yandexcloud.net"
      S3_REGION                   = "ru-central1"
      S3_BUCKET                   = var.artifact_bucket_name
      S3_FORCE_PATH_STYLE         = "true"
      S3_IAM_METADATA_CREDENTIALS = "true"
      QUEUE_ENDPOINT              = "https://message-queue.api.cloud.yandex.net"
      QUEUE_REGION                = "ru-central1"
      SCHEDULER_WAKE_QUEUE_URL    = var.scheduler_wake_queue_url
      DEPLOYMENT_IMAGE            = var.image_ref
    }
  }
  metadata_options {
    aws_v1_http_endpoint = 1
    gce_http_endpoint    = 1
  }
  secrets {
    id                   = var.web_secret_id
    version_id           = var.web_secret_version_id
    key                  = "oidc-client-secret"
    environment_variable = "TELEGRAM_OIDC_CLIENT_SECRET"
  }
  secrets {
    id                   = var.web_secret_id
    version_id           = var.web_secret_version_id
    key                  = "session-cursor-hmac-key"
    environment_variable = "SESSION_API_CURSOR_HMAC_KEY"
  }
  secrets {
    id                   = var.web_secret_id
    version_id           = var.web_secret_version_id
    key                  = "session-id-hmac-key"
    environment_variable = "SESSION_API_ID_HMAC_KEY"
  }
  secrets {
    id                   = var.scheduler_ymq_secret_id
    version_id           = var.scheduler_ymq_secret_version_id
    key                  = "access-key"
    environment_variable = "OUTBOX_QUEUE_ACCESS_KEY_ID"
  }
  secrets {
    id                   = var.scheduler_ymq_secret_id
    version_id           = var.scheduler_ymq_secret_version_id
    key                  = "secret-key"
    environment_variable = "OUTBOX_QUEUE_SECRET_ACCESS_KEY"
  }
  log_options {
    log_group_id = var.log_group_id
    min_level    = "INFO"
  }
}

resource "yandex_serverless_container_iam_binding" "gateway_invoker" {
  container_id = yandex_serverless_container.web.id
  role         = "serverless.containers.invoker"
  members      = ["serviceAccount:${var.gateway_service_account_id}"]
}

resource "yandex_serverless_container_iam_binding" "registry_cleaner_auditor" {
  container_id = yandex_serverless_container.web.id
  role         = "serverless-containers.auditor"
  members      = ["serviceAccount:${var.registry_cleaner_service_account_id}"]
}

resource "yandex_cm_certificate" "web" {
  folder_id           = var.folder_id
  name                = "${var.name_prefix}-web"
  description         = "Managed certificate for ${local.fqdn}"
  domains             = [local.fqdn]
  deletion_protection = var.deletion_protection
  labels              = var.labels
  managed {
    challenge_type  = "DNS_CNAME"
    challenge_count = 1
  }
}

resource "yandex_dns_recordset" "certificate_challenge" {
  zone_id = var.dns_zone_id
  name    = yandex_cm_certificate.web.challenges[0].dns_name
  type    = yandex_cm_certificate.web.challenges[0].dns_type
  ttl     = 60
  data    = [yandex_cm_certificate.web.challenges[0].dns_value]
}

resource "yandex_api_gateway" "web" {
  folder_id         = var.folder_id
  name              = "${var.name_prefix}-web"
  description       = "Dedicated allowlisted gateway for the Sessionless Web UI"
  execution_timeout = "30"
  labels            = var.labels
  custom_domains {
    fqdn           = local.fqdn
    certificate_id = yandex_cm_certificate.web.id
  }
  log_options {
    log_group_id = var.log_group_id
    min_level    = "INFO"
  }
  spec = yamlencode({
    openapi = "3.0.0"
    info = {
      title   = "Sessionless dev Web UI"
      version = "1.0.0"
    }
    paths = merge(local.document_paths, local.api_paths)
  })
  depends_on = [
    yandex_dns_recordset.certificate_challenge,
    yandex_serverless_container_iam_binding.gateway_invoker,
  ]
}

resource "yandex_dns_recordset" "web" {
  zone_id = var.dns_zone_id
  name    = "${local.fqdn}."
  type    = "CNAME"
  ttl     = 60
  data    = ["${yandex_api_gateway.web.domain}."]
}
