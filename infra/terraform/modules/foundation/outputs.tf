output "folder_id" { value = yandex_resourcemanager_folder.environment.id }
output "service_account_ids" { value = { for name, account in yandex_iam_service_account.runtime : name => account.id } }
output "ydb_connection_string" { value = yandex_ydb_database_serverless.application.ydb_full_endpoint }
output "artifact_bucket_name" { value = yandex_storage_bucket.artifacts.bucket }
output "dispatch_queue_url" { value = yandex_message_queue.dispatch.id }
output "dispatch_queue_arn" { value = yandex_message_queue.dispatch.arn }
output "dispatch_dlq_arn" { value = yandex_message_queue.dispatch_dlq.arn }
output "delivery_queue_url" { value = yandex_message_queue.delivery.id }
output "delivery_queue_arn" { value = yandex_message_queue.delivery.arn }
output "delivery_dlq_arn" { value = yandex_message_queue.delivery_dlq.arn }
output "registry_id" { value = yandex_container_registry.application.id }
output "repository_names" { value = { for name, repository in yandex_container_repository.runtime : name => repository.name } }
output "telegram_secret_id" { value = yandex_lockbox_secret.telegram.id }
output "log_group_id" { value = yandex_logging_group.application.id }
