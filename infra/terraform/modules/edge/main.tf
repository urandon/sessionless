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
          x-yc-apigateway-integration: &control
            type: serverless_containers
            container_id: $${apigw.control_container_id}
            service_account_id: "${var.gateway_service_account_id}"
      /readyz:
        get:
          operationId: ready
          responses:
            '200': { description: Ready }
          x-yc-apigateway-integration: *control
      /version:
        get:
          operationId: version
          responses:
            '200': { description: Runtime version }
          x-yc-apigateway-integration: *control
      /telegram/webhook:
        post:
          operationId: telegramWebhook
          responses:
            '200': { description: Update accepted }
          x-yc-apigateway-integration: *control
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
