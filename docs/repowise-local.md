# Optional local RepoWise workflow

RepoWise is an **opt-in developer/research tool** for issue #65. It is not a
Sessionless runtime component, a supported production dependency, or a
prerequisite for contributors. A checkout without Python or RepoWise must keep
the same `make ci`, `make test`, `make build`, `make tools`, `make dev-up`,
image, release, and cloud behavior.

The supported experiment uses RepoWise `0.45.0` on the pinned Darwin arm64
CPython 3.12 toolchain (upstream requires Python 3.11 or newer), a repository-local
environment and state, no provider credentials, and stdio MCP only. Python is
intentionally confined to this developer experiment;
Sessionless production orchestration remains Go/serverless and deploys as
monolithic Go binaries.

## Safety boundary

The wrapper `scripts/repowise-local.sh` owns the complete experiment boundary:

- the virtual environment, synthetic home, XDG directories, temporary files,
  package cache, logs, measurements, and control files live below
  `.local/repowise/`;
- RepoWise's generated index/wiki/database state lives below `.repowise/`;
- both paths are ignored by Git;
- child processes receive an explicit environment allowlist instead of the host
  environment;
- provider keys, subscription credentials, cloud credentials, proxy variables,
  SSH/Git credentials, and host agent configuration are not forwarded;
- telemetry, editor setup, credential saving, generated agent instructions,
  Git hooks, provider prose, workspace seeding, and cost tracking are disabled;
- post-install commands are network-denied and may write only below the two
  ignored experiment roots;
- the wrapper never writes global Codex, Claude, MCP, editor, Python, Git, or
  shell configuration.

Do not run `repowise` directly for this experiment. Upstream defaults can check
PyPI, send telemetry, download UI assets, install editor integration, load
`.repowise/.env`, or write user-level state. The wrapper exists to make those
behaviors unavailable or visible.

## Install

Install is the only supported command that may access the package registry:

```sh
make repowise-install
```

It installs the pinned, hash-verified artifacts into the ignored local
environment. Review `tools/repowise/versions.env` and the platform lock before a
version change. Never replace a hash with an unpinned version, permit an
unreviewed transitive resolution, or allow a normal build target to install
RepoWise implicitly.

The first Darwin arm64 experiment is the supported baseline. Other operating
systems or architectures need their own reviewed hash-locked artifact set; do
not silently resolve a different wheel or sdist.

## Build and maintain the index

Start from a clean checkout, with neither tracked changes nor non-ignored
untracked files, so the index has an unambiguous source identity:

```sh
git status --short
git rev-parse HEAD
make repowise-index
make repowise-status
```

The initial index is keyless and credential-free. Its effective initialization
is non-interactive, no-prose, no-workspace, no-seed, no-agent-file, no-hook,
no-editor, and no-cost-tracking, with `.gitcode/` and `.local/` excluded. The
wrapper records installation provenance and the exact indexed `HEAD` in the
ignored evidence directory. The controlled evaluation additionally records
elapsed time, disk use, and available process resource measurements; those
measurements are not implied by a successful index command.

Update an existing index explicitly:

```sh
make repowise-update
make repowise-status
```

`status` must report whether the recorded indexed commit equals the current
worktree `HEAD`. Do not use an index reported as stale for merge or architecture
decisions. RepoWise state is checkout-local: a separate Git worktree needs its
own `.local/repowise/` and `.repowise/`; copying or sharing an index between
worktrees is unsupported.

The MCP process is a snapshot reader, not a live source-of-truth watcher. Any
tracked or non-ignored untracked checkout change invalidates the controlled
evaluation: stop MCP, restore or commit the change, update the index, verify
status, and restart MCP before relying on another answer.

Neither `index` nor `update` deletes an existing index. If rebuilding becomes
necessary, first use the exact-object uninstall plan and obtain explicit human
confirmation for the resulting paths.

## Doctor and local policy checks

Run the bounded local checks with:

```sh
make repowise-doctor
make repowise-policy-test
```

The wrapper's doctor mode is the upstream diagnostic command constrained by the
local environment and no-network sandbox; commit freshness is checked before it
runs. Use `evaluate` for version and size evidence and `stop` for wrapper-owned
process cleanup. Doctor does not prove the performance, RSS, or network-attempt
bounds in the evaluation record.

The policy test is intentionally manual and credential-free. It verifies that
normal Sessionless workflows do not depend on RepoWise and that the wrapper
retains its environment, network, editor/hook, state, and MCP allowlist guards.
It is not part of `make ci`: CI and developers who did not opt in should not
install Python packages merely to prove that this optional tool is absent.

## Stdio MCP

Start the local stdio server directly when the client supports a one-shot
command:

```sh
make repowise-mcp
```

For a task-local MCP registration, point the client at an absolute checkout
path and pass the `mcp` argument, for example:

```json
{
  "command": "/absolute/path/to/sessionless/scripts/repowise-local.sh",
  "args": ["mcp"]
}
```

Keep this registration in the current task/client only. Do not commit it to the
repository and do not write it to a global agent configuration. The experiment
allows only the read-oriented overview, context, change-risk, repository-health,
and dead-code tools: `get_overview`, `get_context`, `get_change_risk`,
`get_health`, and `get_dead_code`. It does not expose RepoWise plugins,
refactoring/mutation, transcript ingestion, provider prose, an HTTP/SSE server,
or the downloadable UI.

Run the deterministic discovery and five-call exact-allowlist smoke check with:

```sh
make repowise-mcp-smoke
```

The smoke check proves MCP startup and tool exposure and makes one bounded
representative call to each of the five tools. Treat every RepoWise answer as a
hypothesis. Cross-check architecture, risk,
health, and dead-code results against the exact source, `rg`, Go/Svelte tests,
Git history, and the committed Sessionless contracts before creating a finding.

## Evaluation and resource limits

Collect the local version, commit-freshness, and size artifact with:

```sh
make repowise-evaluate
```

The experiment fails its adoption gate if any supported operation attempts
network access after installation, exposes a credential, modifies a tracked or
global file, leaves a process running after stop, or crosses these initial
bounds:

| Measurement | Initial bound |
| --- | ---: |
| Local Python environment | 2 GiB |
| Repo index/state | 1 GiB |
| Cold index | 15 minutes |
| Warm update | 2 minutes |
| MCP startup | 10 seconds |
| Process RSS | 1 GiB |
| Post-install network attempts | 0 |
| Processes after stop | 0 |

Use the stdio MCP experiment described above for timings, resource sampling,
five representative calls, and source comparison. The useful-result gate
requires at least one source-verified finding that was
materially easier to obtain than the current repository tools, plus an honest
record of false positives, false negatives, stale results, and language
coverage for Go, Svelte/TypeScript, Terraform, shell, and the Cloudflare Worker.
See [the evaluation record](research/repowise-sessionless-evaluation.md).

## Stop and uninstall

The supported MCP path is stdio and normally ends with its client. The stop path
is still provided to detect and terminate only wrapper-owned processes:

```sh
make repowise-stop
```

Inspect cleanup before deleting anything:

```sh
make repowise-uninstall-plan
```

The plan must list only the exact repository-local environment/state paths and
must not follow symlinks or broaden to a repository, home, or workspace root.
If the wrapper requests confirmation, copy the displayed typed token exactly:

```sh
make repowise-uninstall
```

After stopping or uninstalling, verify:

```sh
git status --short
```

Tracked status must be unchanged. Removing `.local/repowise/` or `.repowise/`
destroys only ignored experiment state and is not recoverable unless separately
backed up; never automate that removal from `clean`, CI, or normal development.

## Troubleshooting

- **RepoWise is absent:** run `make repowise-install`; normal Sessionless work
  does not require it.
- **Unsupported Python/platform:** install a supported Python 3.11+ locally or
  add a separately reviewed artifact set. Do not loosen the hash pin.
- **Dirty worktree:** commit, stash, or use a clean dedicated worktree before
  indexing. The wrapper intentionally refuses ambiguous evidence.
- **Stale index:** run `make repowise-update` and require `status` to match the
  current exact `HEAD` before using results.
- **Network denial:** only `install` may use the package registry. An index or
  MCP network attempt is a policy failure, not a reason to disable the sandbox.
- **Provider/model request:** this experiment is no-prose and keyless. Do not
  provide credentials; record the unsupported capability instead.
- **Leftover process:** run `make repowise-stop`, inspect the wrapper-owned PID
  evidence, and do not kill unrelated Python processes.

## License and adoption boundary

RepoWise `0.45.0` is offered under AGPL-3.0-or-later or a commercial license.
This experiment does not vendor, modify, redistribute, host, or incorporate the
distribution into Sessionless. Broader shared/hosted use, a CI service, a PR
bot, or redistribution requires a separate legal and architecture review and
possibly a commercial license. No positive local result authorizes such use.

Upstream references: the [pinned source
revision](https://github.com/repowise-dev/repowise/tree/e2bb8a2e4eff3d00005a602ac65a8e4be7daa4a3),
the [0.45.0 package metadata](https://pypi.org/project/repowise/0.45.0/), and
the [license at the reviewed
revision](https://github.com/repowise-dev/repowise/blob/e2bb8a2e4eff3d00005a602ac65a8e4be7daa4a3/LICENSE).
