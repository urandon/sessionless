output "state_bucket_name" {
  description = "Terraform state bucket name used in backend configuration."
  value       = yandex_storage_bucket.terraform_state.bucket
}

output "lock_database_id" {
  description = "Dedicated deployment-lock YDB database identifier."
  value       = yandex_ydb_database_serverless.terraform_locks.id
}

output "lock_database_endpoint" {
  description = "Full SDK endpoint consumed by the deployment lock wrapper."
  value       = yandex_ydb_database_serverless.terraform_locks.ydb_full_endpoint
}

output "lock_table_path" {
  description = "YDB table path used for environment deployment leases."
  value       = yandex_ydb_table.terraform_locks.path
}
