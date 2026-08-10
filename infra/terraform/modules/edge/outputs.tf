output "api_fqdn" { value = local.fqdn }
output "api_url" { value = "https://${local.fqdn}" }
output "gateway_id" { value = yandex_api_gateway.api.id }
output "stable_slot" { value = var.stable_slot }
output "candidate_slot" { value = local.candidate_slot }
output "canary_weight" { value = var.canary_weight }
output "telegram_workflow_execution_url" {
  value     = yandex_serverless_workflow.telegram_ingress.execution_url
  sensitive = true
}
