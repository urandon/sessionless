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
