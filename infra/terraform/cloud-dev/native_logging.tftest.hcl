mock_provider "yandex" {}

variables {
  runtime_log_group_id        = "e2300000000000000000"
  web_login_providers         = "telegram"
  cloud_id                    = "cloud-test"
  base_domain                 = "sessionless.example.com"
  artifact_bucket_name        = "sessionless-test-artifacts"
  telegram_secret_version_id  = "telegram-secret-version"
  web_bff_secret_version_id   = "web-secret-version"
  telegram_oidc_client_id     = "123456789"
  control_blue_image_tag      = "0000000000000000000000000000000000000000"
  control_green_image_tag     = "0000000000000000000000000000000000000000"
  runtime_image_tag           = "0000000000000000000000000000000000000000"
  web_image_ref               = "cr.yandex/crptestregistry/web-bff@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  github_oidc_subject         = "repo:immutable-test-subject:ref:refs/heads/main"
  github_release_oidc_subject = "repo:urandon/sessionless:environment:release"
  billing_account_id          = "billing-test"
  budget_id                   = "budget-test"
}

run "explicit_native_destination" {
  command = plan
  assert {
    condition     = output.runtime_log_group_id == "e2300000000000000000"
    error_message = "The configured native destination must pass through without replacement or lookup."
  }
}

run "explicit_folder_native_destination" {
  command = plan
  variables {
    runtime_log_group_id  = null
    runtime_log_folder_id = "b1g00000000000000000"
  }
  override_resource {
    target          = module.foundation.yandex_resourcemanager_folder.environment
    override_during = plan
    values          = { id = "b1g00000000000000000" }
  }
  assert {
    condition     = output.runtime_log_folder_id == "b1g00000000000000000" && output.runtime_log_group_id == null
    error_message = "Only the explicitly selected deployment folder may be wired as the native destination."
  }
}

run "reject_missing_native_destination" {
  command = plan
  variables { runtime_log_group_id = null }
  expect_failures = [var.runtime_log_group_id]
}

run "reject_ambiguous_native_destination" {
  command = plan
  variables { runtime_log_folder_id = "b1g00000000000000000" }
  expect_failures = [var.runtime_log_group_id]
}

run "reject_empty_folder_native_destination" {
  command = plan
  variables {
    runtime_log_group_id  = null
    runtime_log_folder_id = ""
  }
  expect_failures = [var.runtime_log_folder_id]
}

run "reject_padded_folder_native_destination" {
  command = plan
  variables {
    runtime_log_group_id  = null
    runtime_log_folder_id = " b1g00000000000000000"
  }
  expect_failures = [var.runtime_log_folder_id]
}

run "reject_folder_native_destination_url" {
  command = plan
  variables {
    runtime_log_group_id  = null
    runtime_log_folder_id = "https://example.invalid/logs"
  }
  expect_failures = [var.runtime_log_folder_id]
}

run "reject_project_as_folder_native_destination" {
  command = plan
  variables {
    runtime_log_group_id  = null
    runtime_log_folder_id = "folder__synthetic"
  }
  expect_failures = [var.runtime_log_folder_id]
}

run "reject_placeholder_folder_native_destination" {
  command = plan
  variables {
    runtime_log_group_id  = null
    runtime_log_folder_id = "replace-deployment-folder-id"
  }
  expect_failures = [var.runtime_log_folder_id]
}

run "reject_cross_folder_native_destination" {
  command = plan
  module { source = "../modules/web" }
  variables {
    folder_id                           = "b1g00000000000000000"
    name_prefix                         = "sessionless-test"
    base_domain                         = "sessionless.example.com"
    dns_zone_id                         = "dns-test"
    service_account_id                  = "web-test"
    gateway_service_account_id          = "gateway-test"
    registry_cleaner_service_account_id = "cleaner-test"
    source_sha                          = "0000000000000000000000000000000000000000"
    image_ref                           = "cr.yandex/crptestregistry/web-bff@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    ydb_connection_string               = "grpc://example.invalid/local"
    artifact_bucket_name                = "test-artifacts"
    scheduler_wake_queue_url            = "https://example.invalid/queue"
    web_secret_id                       = "web-secret-test"
    web_secret_version_id               = "web-version-test"
    scheduler_ymq_secret_id             = "scheduler-secret-test"
    scheduler_ymq_secret_version_id     = "scheduler-version-test"
    log_group_id                        = null
    log_folder_id                       = "b1g11111111111111111"
    deletion_protection                 = true
    login_providers                     = "yandex"
    yandex_login_client_id              = "yandex-client-test"
  }
  expect_failures = [var.log_folder_id]
}

run "reject_empty_native_destination" {
  command = plan
  variables { runtime_log_group_id = "" }
  expect_failures = [var.runtime_log_group_id]
}

run "reject_padded_native_destination" {
  command = plan
  variables { runtime_log_group_id = " e2300000000000000000" }
  expect_failures = [var.runtime_log_group_id]
}

run "reject_native_destination_url" {
  command = plan
  variables { runtime_log_group_id = "https://example.invalid/logs" }
  expect_failures = [var.runtime_log_group_id]
}

run "reject_project_as_native_destination" {
  command = plan
  variables { runtime_log_group_id = "folder__synthetic" }
  expect_failures = [var.runtime_log_group_id]
}

run "reject_placeholder_native_destination" {
  command = plan
  variables { runtime_log_group_id = "replace-existing-log-group-id" }
  expect_failures = [var.runtime_log_group_id]
}
