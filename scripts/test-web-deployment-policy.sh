#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
web_module="$repo_root/infra/terraform/modules/web/main.tf"
web_variables="$repo_root/infra/terraform/modules/web/variables.tf"
cloud_root="$repo_root/infra/terraform/cloud-dev/main.tf"
foundation="$repo_root/infra/terraform/modules/foundation/main.tf"
terraform_wrapper="$repo_root/scripts/cloud-terraform.sh"

require_literal() {
  file=$1
  literal=$2
  grep -Fq "$literal" "$file" || {
    printf 'missing Web deployment policy in %s: %s\n' "$file" "$literal" >&2
    exit 1
  }
}

require_regex() {
  file=$1
  pattern=$2
  grep -Eq "$pattern" "$file" || {
    printf 'missing Web deployment policy in %s: %s\n' "$file" "$pattern" >&2
    exit 1
  }
}

require_literal "$web_module" 'provision_policy { min_instances = 0 }'
require_literal "$web_module" 'members      = ["serviceAccount:${var.gateway_service_account_id}"]'
require_literal "$web_module" 'container_id       = yandex_serverless_container.web.id'
require_literal "$web_module" 'WEB_BASE_URL                = local.origin'
require_literal "$web_module" 'SESSIONLESS_ENVIRONMENT     = "cloud-dev"'
require_literal "$web_module" 'key                  = "oidc-client-secret"'
require_literal "$web_module" 'key                  = "session-cursor-hmac-key"'
require_literal "$web_module" 'key                  = "session-id-hmac-key"'
require_literal "$web_module" 'WEB_LOGIN_PROVIDER          = var.login_provider'
require_literal "$web_module" 'YANDEX_LOGIN_CLIENT_ID = var.yandex_login_client_id'
require_literal "$web_module" 'environment_variable = local.login_secret_environment'
require_literal "$web_module" 'var.login_provider == "yandex" ? "YANDEX_LOGIN_CLIENT_SECRET" : "TELEGRAM_OIDC_CLIENT_SECRET"'
require_literal "$web_module" '"/auth/login/start"'
require_literal "$web_module" '"/auth/login/callback"'
require_literal "$web_module" '"/auth/telegram/callback"'
require_literal "$web_variables" 'contains(["telegram", "yandex"], var.login_provider)'
require_literal "$web_variables" 'var.login_provider != "yandex" ||'
require_regex "$cloud_root" 'login_provider[[:space:]]*=[[:space:]]*var\.web_login_provider'
require_regex "$cloud_root" 'yandex_login_client_id[[:space:]]*=[[:space:]]*var\.yandex_login_client_id'
require_literal "$web_variables" '^cr\\.yandex/[^/]+/web-bff@sha256:[0-9a-f]{64}$'
require_literal "$web_variables" 'var.concurrency >= 1 && var.concurrency <= 8'
require_regex "$cloud_root" 'service_account_id[[:space:]]*=[[:space:]]*module\.foundation\.web_ready_service_account_id'
require_regex "$cloud_root" 'gateway_service_account_id[[:space:]]*=[[:space:]]*module\.foundation\.service_account_ids\["web-gateway"\]'
require_literal "$foundation" '"control-api", "web-bff", "reconciler", "telegram-sender", "worker-runtime"'
require_literal "$foundation" 'for name in ["api", "web-bff", "scheduler", "worker", "telegram-sender"]'
require_literal "$terraform_wrapper" 'CLOUD_DEV_IMAGE_TFVARS is required for every non-foundation plan'

if [ "${1:-}" = "--plan-json" ] && [ "$#" -eq 2 ]; then
  # Inspect Terraform's actual mocked target expansion. Source checks alone
  # cannot prove that a module dependency is absent from the execution graph.
  jq -e -s '
    [.[] | select(.type == "test_run" and .test_run.run == "foundation_bootstrap_without_web_payload" and .test_run.status == "pass")] as $bootstrap |
    [.[] | select(.type == "test_plan" and .["@testrun"] == "isolated_web_rollout")] as $plans |
    ($bootstrap | length) == 1 and
    ($plans | length) == 1 and
    ($plans[0].test_plan.resource_changes | map(.address)) as $addresses |
    all([
      "module.web.yandex_serverless_container.web",
      "module.foundation.yandex_container_registry_iam_binding.runtime_puller",
      "module.foundation.yandex_ydb_database_iam_binding.runtime_editor",
      "module.foundation.yandex_storage_bucket_iam_binding.runtime_editor",
      "module.foundation.yandex_lockbox_secret_iam_member.web_bff",
      "module.foundation.yandex_lockbox_secret_iam_member.scheduler_ymq[\"web-bff\"]",
      "module.foundation.yandex_kms_symmetric_key_iam_member.runtime_secret_decrypter[\"web-bff\"]",
      "module.foundation.yandex_resourcemanager_folder_iam_member.runtime[\"web-bff:logging.writer\"]",
      "module.foundation.yandex_resourcemanager_folder_iam_member.runtime[\"web-gateway:logging.writer\"]"
    ][]; . as $required | $addresses | index($required) != null) and
    all($addresses[];
      (startswith("module.runtime.") or startswith("module.edge.") or
       contains("yandex_iam_workload_identity_oidc_federation.") or
       contains("yandex_iam_workload_identity_federated_credential.") or
       contains("yandex_container_repository_") or
       contains("yandex_container_repository.runtime") or
       contains("yandex_iam_service_account_static_access_key.queue_provisioner")) | not)
  ' "$2" >/dev/null || {
    printf '%s\n' 'targeted Web plan must retain every runtime permission and exclude publication/GC and unrelated runtime graphs' >&2
    exit 1
  }
elif [ "$#" -ne 0 ]; then
  printf '%s\n' 'usage: test-web-deployment-policy.sh [--plan-json terraform-test.jsonl]' >&2
  exit 1
fi

if grep -Fq 'allUsers' "$web_module"; then
  printf '%s\n' 'Web container must not have an anonymous invoker binding' >&2
  exit 1
fi
if grep -Fq '{proxy+}' "$web_module"; then
  printf '%s\n' 'Web gateway must use the explicit route allowlist, not a catch-all proxy' >&2
  exit 1
fi
if grep -R -E 'variable "(telegram_oidc_client_secret|yandex_login_client_secret|session_api_cursor_hmac_key|session_api_id_hmac_key)"' \
  "$repo_root/infra/terraform" >/dev/null; then
  printf '%s\n' 'secret payloads must not be Terraform variables' >&2
  exit 1
fi

printf '%s\n' 'Web deployment policy invariants passed'
