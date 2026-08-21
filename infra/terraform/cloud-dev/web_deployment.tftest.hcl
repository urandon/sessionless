mock_provider "yandex" {}

run "web_deployment_plan" {
  command = plan

  variables {
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
    github_release_oidc_subject = "repo:immutable-test-subject:environment:release"
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
    condition     = output.github_release_oidc_subject == "repo:immutable-test-subject:environment:release"
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
