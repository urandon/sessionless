mock_provider "yandex" {
  mock_resource "yandex_iam_service_account" {
    defaults = { id = "service-account-test" }
  }
  mock_resource "yandex_iam_service_account_static_access_key" {
    defaults = { output_to_lockbox_version_id = "scheduler-version-test" }
  }
}

run "yandex_login_web_module" {
  command = plan
  module { source = "../modules/web" }
  # Make only this computed ID known during the MOCK plan, so assertions read
  # the real configured yamlencode(spec), not a replacement spec or local map.
  # https://developer.hashicorp.com/terraform/language/tests/mocking
  override_resource {
    target          = yandex_serverless_container.web
    override_during = plan
    values          = { id = "web-container-test" }
  }
  variables {
    folder_id                           = "folder-test"
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
    log_group_id                        = "logs-test"
    deletion_protection                 = true
    login_providers                     = "yandex"
    yandex_login_client_id              = "yandex-client-test"
  }
  assert {
    condition = (
      yandex_serverless_container.web.image[0].environment["WEB_LOGIN_PROVIDERS"] == "yandex" &&
      yandex_serverless_container.web.image[0].environment["YANDEX_LOGIN_CLIENT_ID"] == "yandex-client-test" &&
      !contains(keys(yandex_serverless_container.web.image[0].environment), "TELEGRAM_OIDC_CLIENT_ID") &&
      length([
        for secret in yandex_serverless_container.web.secrets : secret
        if secret.key == "yandex-login-client-secret" && secret.environment_variable == "YANDEX_LOGIN_CLIENT_SECRET"
      ]) == 1 &&
      alltrue([
        for secret in yandex_serverless_container.web.secrets :
        secret.version_id == (contains(["access-key", "secret-key"], secret.key) ? "scheduler-version-test" : "web-version-test")
      ])
    )
    error_message = "Yandex selection must not inject Telegram configuration and every Lockbox secret must use its pinned payload version."
  }
  assert {
    condition = (
      contains(keys(local.api_paths), "/auth/login/methods") &&
      contains(keys(local.api_paths["/auth/login/yandex/start"]), "post") &&
      contains(keys(local.api_paths["/auth/login/telegram/start"]), "post") &&
      !contains(keys(local.api_paths), "/auth/login/start") &&
      !contains(keys(local.api_paths), "/auth/telegram/start") &&
      contains(keys(local.api_paths), "/auth/login/callback") &&
      !contains(keys(local.api_paths), "/{proxy+}")
    )
    error_message = "The generic login routes must use the explicit gateway allowlist."
  }
  assert {
    condition = toset([
      for path in keys(yamldecode(yandex_api_gateway.web.spec).paths) : path
      if startswith(path, "/account/") || startswith(path, "/api/web/v1/account/")
      ]) == toset([
      "/account/login-methods",
      "/api/web/v1/account/login-methods",
      "/api/web/v1/account/identity-operations",
      "/api/web/v1/account/identity-operations/{operation_id}",
      "/api/web/v1/account/identity-operations/{operation_id}/continue",
      "/api/web/v1/account/identity-operations/{operation_id}/confirm",
      "/api/web/v1/account/identity-operations/{operation_id}/cancel",
    ])
    error_message = "The actual gateway spec must expose exactly the seven closed account paths."
  }
  assert {
    condition = alltrue([
      for path, methods in {
        "/account/login-methods"                                          = ["get", "head"]
        "/api/web/v1/account/login-methods"                               = ["get"]
        "/api/web/v1/account/identity-operations"                         = ["post"]
        "/api/web/v1/account/identity-operations/{operation_id}"          = ["get"]
        "/api/web/v1/account/identity-operations/{operation_id}/continue" = ["post"]
        "/api/web/v1/account/identity-operations/{operation_id}/confirm"  = ["post"]
        "/api/web/v1/account/identity-operations/{operation_id}/cancel"   = ["post"]
        } : try(
        toset([for key in keys(yamldecode(yandex_api_gateway.web.spec).paths[path]) : key if key != "parameters"]) == toset(methods) &&
        alltrue([
          for method in methods :
          yamldecode(yandex_api_gateway.web.spec).paths[path][method]["x-yc-apigateway-integration"] == {
            type               = "serverless_containers"
            container_id       = "web-container-test"
            service_account_id = "gateway-test"
          }
        ]) &&
        (
          strcontains(path, "{operation_id}") ?
          yamldecode(yandex_api_gateway.web.spec).paths[path].parameters == [{
            name   = "operation_id", in = "path", required = true
            schema = { type = "string", maxLength = 160, pattern = "^[A-Za-z0-9][A-Za-z0-9._:-]*$" }
          }] : !contains(keys(yamldecode(yandex_api_gateway.web.spec).paths[path]), "parameters")
        ), false
      )
    ])
    error_message = "Every account method and exact bounded path parameter must target the same private Web container through its existing gateway identity."
  }
}

run "reject_missing_yandex_login_client" {
  command = plan
  module { source = "../modules/web" }
  variables {
    folder_id                           = "folder-test"
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
    log_group_id                        = "logs-test"
    deletion_protection                 = true
    login_providers                     = "yandex"
  }
  expect_failures = [var.yandex_login_client_id]
}

run "reject_unknown_login_provider" {
  command = plan
  module { source = "../modules/web" }
  variables {
    folder_id                           = "folder-test"
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
    log_group_id                        = "logs-test"
    deletion_protection                 = true
    login_providers                     = "unknown"
  }
  expect_failures = [var.login_providers]
}

run "web_deployment_plan" {
  command = plan

  variables {
    web_login_providers         = "telegram"
    cloud_id                    = "cloud-test"
    base_domain                 = "sessionless.triborg.dev"
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

  assert {
    condition     = module.web.fqdn == "web.dev.sessionless.triborg.dev"
    error_message = "the Web hostname must be derived once from base_domain"
  }

  assert {
    condition = (
      toset(keys(output.registry_gc_inventory.repositories)) ==
      toset(["control-api", "web-bff", "reconciler", "telegram-sender", "worker-runtime"])
    )
    error_message = "registry GC inventory must contain every managed repository"
  }

  assert {
    condition = (
      toset(keys(output.registry_gc_inventory.containers)) ==
      toset(["control-blue", "control-green", "web-bff", "reconciler", "telegram-sender", "worker-runtime"])
    )
    error_message = "registry GC inventory must contain every managed container slot"
  }

  assert {
    condition = (
      output.registry_gc_inventory.lock_environment == "cloud-dev" &&
      output.registry_gc_inventory.stable_slot == "blue" &&
      output.registry_gc_inventory.candidate_slot == "green" &&
      alltrue([
        for container in values(output.registry_gc_inventory.containers) :
        contains(keys(container), "revision_id") &&
        can(regex("^[0-9a-f]{40}$", container.source_sha))
      ])
    )
    error_message = "registry GC inventory must identify the shared lock, rollout slots, revisions, and source commits"
  }

  assert {
    condition = alltrue([
      for status in values(output.registry_gc_inventory.lifecycle_policy_status) : status == "disabled"
    ])
    error_message = "native registry lifecycle must remain disabled"
  }

  assert {
    condition     = module.web.image_ref == var.web_image_ref
    error_message = "the Web deployment must preserve the immutable image reference"
  }

  assert {
    condition     = output.github_release_oidc_subject == "repo:urandon/sessionless:environment:release"
    error_message = "release workload identity must preserve the exact protected-environment subject"
  }

  assert {
    condition     = module.web.prepared_instances == 0
    error_message = "the Web BFF must scale to zero"
  }

  assert {
    condition     = module.web.concurrency >= 1 && module.web.concurrency <= 8
    error_message = "the Web BFF concurrency must remain inside the dev cost envelope"
  }
}

run "reject_foreign_release_subject" {
  command = plan

  variables {
    web_login_providers         = "telegram"
    cloud_id                    = "cloud-test"
    base_domain                 = "sessionless.triborg.dev"
    artifact_bucket_name        = "sessionless-test-artifacts"
    telegram_secret_version_id  = "telegram-secret-version"
    web_bff_secret_version_id   = "web-secret-version"
    telegram_oidc_client_id     = "123456789"
    control_blue_image_tag      = "0000000000000000000000000000000000000000"
    control_green_image_tag     = "0000000000000000000000000000000000000000"
    runtime_image_tag           = "0000000000000000000000000000000000000000"
    web_image_ref               = "cr.yandex/crptestregistry/web-bff@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    github_oidc_subject         = "repo:immutable-test-subject:ref:refs/heads/main"
    github_release_oidc_subject = "repo:attacker/project:environment:release"
    billing_account_id          = "billing-test"
    budget_id                   = "budget-test"
  }

  expect_failures = [var.github_release_oidc_subject]
}


# Foundation must remain bootstrappable before out-of-band Web payload loading.
run "foundation_bootstrap_without_web_payload" {
  command = plan
  plan_options { target = [module.foundation] }
  variables {
    cloud_id                    = "cloud-test"
    base_domain                 = "sessionless.triborg.dev"
    artifact_bucket_name        = "sessionless-test-artifacts"
    telegram_secret_version_id  = ""
    web_bff_secret_version_id   = ""
    web_login_providers         = "yandex"
    yandex_login_client_id      = "yandex-client-test"
    control_blue_image_tag      = "0000000000000000000000000000000000000000"
    control_green_image_tag     = "0000000000000000000000000000000000000000"
    runtime_image_tag           = "0000000000000000000000000000000000000000"
    web_image_ref               = "cr.yandex/crptestregistry/web-bff@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    github_oidc_subject         = "repo:immutable-test-subject:ref:refs/heads/main"
    github_release_oidc_subject = "repo:urandon/sessionless:environment:release"
    billing_account_id          = "billing-test"
    budget_id                   = "budget-test"
  }
}
# Exercise Terraform's real target expansion, not a source-text approximation.
# The policy runner inspects this plan's machine-readable resource changes.
run "isolated_web_rollout" {
  command = plan
  plan_options { target = [module.web] }
  variables {
    cloud_id                    = "cloud-test"
    base_domain                 = "sessionless.triborg.dev"
    artifact_bucket_name        = "sessionless-test-artifacts"
    telegram_secret_version_id  = ""
    web_bff_secret_version_id   = "web-version-test"
    web_login_providers         = "yandex"
    yandex_login_client_id      = "yandex-client-test"
    control_blue_image_tag      = "0000000000000000000000000000000000000000"
    control_green_image_tag     = "0000000000000000000000000000000000000000"
    runtime_image_tag           = "0000000000000000000000000000000000000000"
    web_image_ref               = "cr.yandex/crptestregistry/web-bff@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    github_oidc_subject         = "repo:immutable-test-subject:ref:refs/heads/main"
    github_release_oidc_subject = "repo:urandon/sessionless:environment:release"
    billing_account_id          = "billing-test"
    budget_id                   = "budget-test"
  }
}

run "reject_empty_web_version" {
  command = plan
  module { source = "../modules/web" }
  variables {
    folder_id                           = "folder-test"
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
    web_secret_version_id               = ""
    scheduler_ymq_secret_id             = "scheduler-secret-test"
    scheduler_ymq_secret_version_id     = "scheduler-version-test"
    log_group_id                        = "logs-test"
    deletion_protection                 = true
    login_providers                     = "yandex"
    yandex_login_client_id              = "yandex-client-test"
  }
  expect_failures = [var.web_secret_version_id]
}

run "reject_whitespace_web_version" {
  command = plan
  module { source = "../modules/web" }
  variables {
    folder_id                           = "folder-test"
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
    web_secret_version_id               = "   "
    scheduler_ymq_secret_id             = "scheduler-secret-test"
    scheduler_ymq_secret_version_id     = "scheduler-version-test"
    log_group_id                        = "logs-test"
    deletion_protection                 = true
    login_providers                     = "yandex"
    yandex_login_client_id              = "yandex-client-test"
  }
  expect_failures = [var.web_secret_version_id]
}

run "reject_empty_scheduler_version" {
  command = plan
  module { source = "../modules/web" }
  variables {
    folder_id                           = "folder-test"
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
    scheduler_ymq_secret_version_id     = ""
    log_group_id                        = "logs-test"
    deletion_protection                 = true
    login_providers                     = "yandex"
    yandex_login_client_id              = "yandex-client-test"
  }
  expect_failures = [var.scheduler_ymq_secret_version_id]
}

run "reject_whitespace_scheduler_version" {
  command = plan
  module { source = "../modules/web" }
  variables {
    folder_id                           = "folder-test"
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
    scheduler_ymq_secret_version_id     = " \t "
    log_group_id                        = "logs-test"
    deletion_protection                 = true
    login_providers                     = "yandex"
    yandex_login_client_id              = "yandex-client-test"
  }
  expect_failures = [var.scheduler_ymq_secret_version_id]
}

run "both_login_web_module" {
  command = plan
  module { source = "../modules/web" }
  variables {
    folder_id                           = "folder-test"
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
    log_group_id                        = "logs-test"
    deletion_protection                 = true
    login_providers                     = "yandex,telegram"
    telegram_oidc_client_id             = "123456789"
    yandex_login_client_id              = "yandex-client-test"
  }
  assert {
    condition = (
      yandex_serverless_container.web.image[0].environment["WEB_LOGIN_PROVIDERS"] == "yandex,telegram" &&
      yandex_serverless_container.web.image[0].environment["YANDEX_LOGIN_CLIENT_ID"] == "yandex-client-test" &&
      yandex_serverless_container.web.image[0].environment["TELEGRAM_OIDC_CLIENT_ID"] == "123456789" &&
      length([
        for secret in yandex_serverless_container.web.secrets : secret
        if secret.key == "yandex-login-client-secret" && secret.environment_variable == "YANDEX_LOGIN_CLIENT_SECRET"
      ]) == 1 &&
      length([for secret in yandex_serverless_container.web.secrets : secret if secret.key == "telegram-oidc-client-secret" && secret.environment_variable == "TELEGRAM_OIDC_CLIENT_SECRET"]) == 1 &&
      !contains([for secret in yandex_serverless_container.web.secrets : secret.key], "oidc-client-secret") &&
      alltrue([
        for secret in yandex_serverless_container.web.secrets :
        secret.version_id == (contains(["access-key", "secret-key"], secret.key) ? "scheduler-version-test" : "web-version-test")
      ])
    )
    error_message = "Both providers must retain independent pinned configuration and secrets and every Lockbox secret must use its pinned payload version."
  }
  assert {
    condition = (
      contains(keys(local.api_paths), "/auth/login/methods") &&
      contains(keys(local.api_paths["/auth/login/yandex/start"]), "post") &&
      contains(keys(local.api_paths["/auth/login/telegram/start"]), "post") &&
      !contains(keys(local.api_paths), "/auth/login/start") &&
      !contains(keys(local.api_paths), "/auth/telegram/start") &&
      contains(keys(local.api_paths), "/auth/login/callback") &&
      !contains(keys(local.api_paths), "/{proxy+}")
    )
    error_message = "The generic login routes must use the explicit gateway allowlist."
  }
}


run "reject_both_missing_telegram_client" {
  command = plan
  module { source = "../modules/web" }
  variables {
    folder_id                           = "folder-test"
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
    log_group_id                        = "logs-test"
    deletion_protection                 = true
    login_providers                     = "yandex,telegram"
    yandex_login_client_id              = "yandex-client-test"
  }
  expect_failures = [var.telegram_oidc_client_id]
}
