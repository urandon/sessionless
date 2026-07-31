output "control_container_ids" { value = { for slot, container in yandex_serverless_container.control : slot => container.id } }
output "control_container_urls" { value = { for slot, container in yandex_serverless_container.control : slot => container.url } }
output "worker_container_id" { value = yandex_serverless_container.worker.id }
output "reconciler_container_id" { value = yandex_serverless_container.reconciler.id }
output "telegram_sender_container_id" { value = yandex_serverless_container.telegram_sender.id }
