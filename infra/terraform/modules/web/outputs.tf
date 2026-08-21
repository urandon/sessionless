output "fqdn" { value = local.fqdn }
output "url" { value = local.origin }
output "gateway_id" { value = yandex_api_gateway.web.id }
output "certificate_id" { value = yandex_cm_certificate.web.id }
output "container_id" { value = yandex_serverless_container.web.id }
output "container_url" { value = yandex_serverless_container.web.url }
output "container_revision_id" { value = yandex_serverless_container.web.revision_id }
output "image_ref" { value = var.image_ref }
output "prepared_instances" { value = yandex_serverless_container.web.provision_policy[0].min_instances }
output "concurrency" { value = yandex_serverless_container.web.concurrency }
output "registry_gc_container_inventory" {
  value = {
    web-bff = {
      container_id = yandex_serverless_container.web.id
      revision_id  = yandex_serverless_container.web.revision_id
      component    = "web-bff"
      repository   = "web-bff"
      slot         = "singleton"
      source_sha   = var.source_sha
      image_ref    = var.image_ref
    }
  }
}
