locals {
  labels = merge(var.labels, {
    application = "sessionless"
    component   = "terraform-bootstrap"
    managed-by  = "terraform"
  })
}

resource "yandex_storage_bucket" "terraform_state" {
  bucket    = var.state_bucket_name
  folder_id = var.management_folder_id
  max_size  = var.state_bucket_max_size_bytes
  tags      = local.labels

  anonymous_access_flags {
    read        = false
    list        = false
    config_read = false
  }

  versioning {
    enabled = true
  }

  lifecycle_rule {
    id      = "expire-noncurrent-state"
    enabled = true

    noncurrent_version_expiration {
      days = var.state_noncurrent_retention_days
    }
  }

  lifecycle {
    prevent_destroy = true
  }
}

resource "yandex_ydb_database_serverless" "terraform_locks" {
  name                = var.lock_database_name
  folder_id           = var.management_folder_id
  deletion_protection = true
  labels              = local.labels

  serverless_database {
    enable_throttling_rcu_limit = true
    provisioned_rcu_limit       = 0
    throttling_rcu_limit        = var.lock_database_ru_limit
    storage_size_limit          = var.lock_database_storage_gib
  }

  lifecycle {
    prevent_destroy = true
  }
}

resource "yandex_ydb_table" "terraform_locks" {
  path              = "terraform_locks"
  connection_string = yandex_ydb_database_serverless.terraform_locks.ydb_full_endpoint

  column {
    name     = "environment"
    type     = "Utf8"
    not_null = true
  }

  column {
    name     = "owner_id"
    type     = "Utf8"
    not_null = true
  }

  column {
    name     = "fence_token"
    type     = "Uint64"
    not_null = true
  }

  column {
    name     = "acquired_at"
    type     = "Timestamp"
    not_null = true
  }

  column {
    name     = "expires_at"
    type     = "Timestamp"
    not_null = true
  }

  primary_key = ["environment"]

  ttl {
    column_name     = "expires_at"
    expire_interval = "PT0S"
  }

  lifecycle {
    prevent_destroy = true
  }
}
