locals {
  fqdn           = "dev-api.${trimsuffix(var.base_domain, ".")}"
  stable_id      = var.control_container_ids[var.stable_slot]
  candidate_slot = var.stable_slot == "blue" ? "green" : "blue"
  candidate_id   = var.control_container_ids[local.candidate_slot]
}

resource "yandex_cm_certificate" "api" {
  folder_id           = var.folder_id
  name                = "${var.name_prefix}-api"
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
  name    = yandex_cm_certificate.api.challenges[0].dns_name
  type    = yandex_cm_certificate.api.challenges[0].dns_type
  ttl     = 60
  data    = [yandex_cm_certificate.api.challenges[0].dns_value]
}

resource "yandex_api_gateway" "api" {
  folder_id         = var.folder_id
  name              = "${var.name_prefix}-api"
  description       = "Stable edge and weighted canary router for Sessionless dev"
  execution_timeout = "30"
  labels            = var.labels
  variables = {
    control_container_id = local.stable_id
  }
  canary {
    weight = var.canary_weight
    variables = {
      control_container_id = local.candidate_id
    }
  }
  custom_domains {
    fqdn           = local.fqdn
    certificate_id = yandex_cm_certificate.api.id
  }
  log_options {
    log_group_id = var.log_group_id
    min_level    = "INFO"
  }
  spec = <<-YAML
    openapi: "3.0.0"
    info:
      title: Sessionless dev API
      version: "1.0.0"
    x-yc-apigateway:
      variables:
        control_container_id:
          default: "${local.stable_id}"
          enum:
            - "${var.control_container_ids["blue"]}"
            - "${var.control_container_ids["green"]}"
    paths:
      /healthz:
        get:
          operationId: health
          responses:
            '200': { description: Healthy }
          x-yc-apigateway-integration:
            type: serverless_containers
            container_id: $${var.control_container_id}
            service_account_id: "${var.gateway_service_account_id}"
      /readyz:
        get:
          operationId: ready
          responses:
            '200': { description: Ready }
          x-yc-apigateway-integration:
            type: serverless_containers
            container_id: $${var.control_container_id}
            service_account_id: "${var.gateway_service_account_id}"
      /version:
        get:
          operationId: version
          responses:
            '200': { description: Runtime version }
          x-yc-apigateway-integration:
            type: serverless_containers
            container_id: $${var.control_container_id}
            service_account_id: "${var.gateway_service_account_id}"
      /telegram/webhook:
        post:
          operationId: telegramWebhook
          responses:
            '200': { description: Update accepted }
          x-yc-apigateway-integration:
            type: serverless_containers
            container_id: $${var.control_container_id}
            service_account_id: "${var.gateway_service_account_id}"
  YAML

  depends_on = [yandex_dns_recordset.certificate_challenge]
}

resource "yandex_dns_recordset" "api" {
  zone_id = var.dns_zone_id
  name    = "${local.fqdn}."
  type    = "CNAME"
  ttl     = 60
  data    = ["${yandex_api_gateway.api.domain}."]
}

# Telegram currently times out before reaching the API Gateway public edge from
# its webhook network, even though the same endpoint is healthy from independent
# IPv4 clients. Yandex Workflows exposes an officially documented Telegram
# webhook-compatible execution URL and acknowledges execution start immediately.
# The asynchronous workflow forwards the unchanged update to the normal gateway
# route and injects the trusted internal webhook header from Lockbox.
resource "yandex_serverless_workflow" "telegram_ingress" {
  folder_id          = var.folder_id
  name               = "${var.name_prefix}-telegram-ingress"
  description        = "Asynchronous Telegram acknowledgement and API Gateway forwarding bridge"
  service_account_id = var.gateway_service_account_id
  is_public          = true
  labels             = var.labels
  specification = {
    spec_yaml = <<-YAWL
      yawl: "0.1"
      start: forward_update
      steps:
        forward_update:
          httpCall:
            url: https://${local.fqdn}/telegram/webhook
            method: POST
            timeout: 10s
            retryPolicy:
              errorList:
                - HTTP_CALL_408
                - HTTP_CALL_425
                - HTTP_CALL_429
                - HTTP_CALL_500
                - HTTP_CALL_502
                - HTTP_CALL_503
                - HTTP_CALL_504
                - HTTP_CALL_520
                - HTTP_CALL_521
                - HTTP_CALL_522
                - HTTP_CALL_523
                - HTTP_CALL_524
              initialDelay: 1s
              backoffRate: 2.0
              retryCount: 5
              maxDelay: 30s
            headers:
              Content-Type: application/json
              X-Telegram-Bot-Api-Secret-Token: '\(lockboxPayload("${var.telegram_secret_id}"; "webhook-secret"))'
            body: '\(.input)'
    YAWL
  }
  log_options = {
    log_group_id = var.log_group_id
    min_level    = "INFO"
  }

  depends_on = [yandex_api_gateway.api]
}
