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
require_literal "$web_variables" '^cr\\.yandex/[^/]+/web-bff@sha256:[0-9a-f]{64}$'
require_literal "$web_variables" 'var.concurrency >= 1 && var.concurrency <= 8'
require_regex "$cloud_root" 'service_account_id[[:space:]]*=[[:space:]]*module\.foundation\.service_account_ids\["web-bff"\]'
require_regex "$cloud_root" 'gateway_service_account_id[[:space:]]*=[[:space:]]*module\.foundation\.service_account_ids\["web-gateway"\]'
require_literal "$foundation" '"control-api", "web-bff", "reconciler", "telegram-sender", "worker-runtime"'
require_literal "$foundation" 'for name in ["api", "web-bff", "scheduler", "worker", "telegram-sender"]'
require_literal "$terraform_wrapper" 'CLOUD_DEV_IMAGE_TFVARS is required for every non-foundation plan'

if grep -Fq 'allUsers' "$web_module"; then
  printf '%s\n' 'Web container must not have an anonymous invoker binding' >&2
  exit 1
fi
if grep -Fq '{proxy+}' "$web_module"; then
  printf '%s\n' 'Web gateway must use the explicit route allowlist, not a catch-all proxy' >&2
  exit 1
fi
if grep -R -E 'variable "(telegram_oidc_client_secret|session_api_cursor_hmac_key|session_api_id_hmac_key)"' \
  "$repo_root/infra/terraform" >/dev/null; then
  printf '%s\n' 'secret payloads must not be Terraform variables' >&2
  exit 1
fi

printf '%s\n' 'Web deployment policy invariants passed'
