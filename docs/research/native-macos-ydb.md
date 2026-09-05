# Native macOS YDB 25.3.1.25 spike

Status date: **2026-09-06**. Tracking issue: **#96**.

## Decision

Do **not** add native macOS YDB as a Sessionless local-runtime option for the
pinned `25.3.1.25` release. The result is a conditional no-go, not a claim that
the source can never compile on Darwin:

- the pinned source contains a Darwin arm64 Yatool bootstrap, Darwin-specific
  `ydbd` build clauses, and Darwin arm64 toolchain resources;
- the same pinned release documents the supported Yatool build as x86_64
  Ubuntu, documents CMake server builds only for Ubuntu, and names macOS only
  for the CLI;
- upstream release automation builds the CLI and DSTool for Darwin arm64, but
  has no equivalent release or CI lane for a Darwin arm64 `ydbd`;
- the exact pinned Darwin arm64 Yatool bootstrap could not be downloaded on the
  experiment host because the legacy Yandex S3 endpoint terminated TLS before
  returning any bytes. Both the system Python/LibreSSL and an installed
  Python 3.14/OpenSSL 3 reproduced the same endpoint failure.

No YDB source, storage, synchronization, allocator, or correctness check was
patched around. No `ydbd` artifact was produced, no native process was run, and
the existing container runtime was not changed or restarted. The supported
Sessionless path remains the pinned Linux container in `compose.yaml`.

## Evidence classification

| Evidence | Classification | Consequence |
| --- | --- | --- |
| Tag `25.3.1.25` resolves to commit `8187ce049bed9e63a5ec04769da2d83dd237c08a`. | Observed | The experiment did not substitute `main` or a newer YDB release. |
| The release README says the minimum platform is x86_64 while also saying development builds are regularly exercised on current macOS and Windows. | Documented, internally broad | The general README is not sufficient evidence that a complete Apple Silicon server is supported. |
| The pinned build guide limits native Yatool use to x86_64 Ubuntu and limits CMake server builds to Ubuntu; macOS is listed for the CLI. | Documented, target-specific | There is no supported native macOS server recipe at this revision. |
| The `ydbd` target strips on Darwin and omits the x86-only Hyperscan peer outside `ARCH_X86_64`. | Observed | The graph anticipates Darwin/non-x86 compilation, but this is capability evidence, not a support or runtime-parity guarantee. |
| The bootstrap selects a dedicated `darwin-arm64` archive with object `9750552540` and MD5 `d8b7d17c095beb38007e0f55d6319cff`. | Observed | A pinned native bootstrap exists and can be retried without floating tool inputs. |
| Pinned GitHub automation builds `ydb`, not `ydbd`, with `DEFAULT-DARWIN-ARM64`. | Observed | Upstream continuously exercises a Darwin arm64 client lane, not the server acceptance boundary needed here. |
| Current YDB system requirements call macOS/Windows unsupported for server operation and direct Apple Silicon development to an emulated x86_64 container. | Documented, current | Native server adoption would be an unsupported local divergence even if a one-off link succeeded. |
| Issue [ydb-platform/ydb#12259](https://github.com/ydb-platform/ydb/issues/12259) records a file-backed local-container storage-pool failure and is closed without a visible linked fix on the issue page. | Documented, historical | It motivates storage-parity testing but does not establish native APFS support. |

Pinned primary sources:

- [release tree and supported-platform statement](https://github.com/ydb-platform/ydb/tree/8187ce049bed9e63a5ec04769da2d83dd237c08a#supported-platforms);
- [pinned build support and commands](https://github.com/ydb-platform/ydb/blob/8187ce049bed9e63a5ec04769da2d83dd237c08a/BUILD.md#L70-L100);
- [pinned Darwin arm64 bootstrap identity and cache variables](https://github.com/ydb-platform/ydb/blob/8187ce049bed9e63a5ec04769da2d83dd237c08a/ya#L35-L128);
- [pinned Darwin `ydbd` target clauses](https://github.com/ydb-platform/ydb/blob/8187ce049bed9e63a5ec04769da2d83dd237c08a/ydb/apps/ydbd/ya.make#L1-L32);
- [pinned Darwin arm64 CLI workflow](https://github.com/ydb-platform/ydb/blob/8187ce049bed9e63a5ec04769da2d83dd237c08a/.github/workflows/build_ydb_cli.yml#L23-L38);
- [current YDB server system requirements](https://ydb.tech/docs/en/devops/concepts/system-requirements);
- [current Apple Silicon container guidance](https://ydb.tech/docs/en/quickstart).

## Bounded experiment

### Host and storage snapshot

The experiment host was:

- macOS `26.6.2` (`25G83`), Darwin `25.6.0`, native `arm64`;
- Apple clang `21.0.0`, Command Line Tools at
  `/Library/Developer/CommandLineTools`;
- system Python `3.9.6` with LibreSSL `2.8.3`;
- Homebrew Python `3.14.6` with OpenSSL `3.6.3`;
- Git `2.50.1`.

Before download, `df -h` reported 313 GiB available on `/` and 1.4 TiB on
`/Volumes/hubdisk`. The enforced stop conditions were:

- stop below 250 GiB available on `/`;
- stop below 1 TiB available on `/Volumes/hubdisk`;
- stop above 250 GiB of experiment-owned hubdisk data;
- stop if any source, tool, cache, build, log, or runtime path resolves outside
  `/Volumes/hubdisk/workspace`;
- never delete an experiment path without a measured exact target and explicit
  confirmation.

All owned paths were rooted at:

```text
/Volumes/hubdisk/workspace/ai/ydb-native-macos-25.3.1.25/
  source/
  cache/
  build/
  logs/
  runtime/
```

The checkout used:

```sh
git clone --filter=blob:none --depth 1 --single-branch \
  --branch 25.3.1.25 \
  https://github.com/ydb-platform/ydb.git \
  /Volumes/hubdisk/workspace/ai/ydb-native-macos-25.3.1.25/source
```

The exact tag and commit were confirmed independently with `git describe
--tags --exact-match` and `git rev-parse HEAD`.

### Cache containment proof

The pinned bootstrap source defines `YA_CACHE_DIR` as the complete Yatool
misc/cache root and `YA_CACHE_DIR_TOOLS` as its tool root. Before invoking it,
both paths were created and resolved under the owned hubdisk root. An explicit
nonexistent hubdisk `YA_TOKEN_PATH` prevented an ambient home-directory token
from being read:

```sh
YA_CACHE_DIR=/Volumes/hubdisk/workspace/ai/ydb-native-macos-25.3.1.25/cache \
YA_CACHE_DIR_TOOLS=/Volumes/hubdisk/workspace/ai/ydb-native-macos-25.3.1.25/cache/tools \
YA_TOKEN_PATH=/Volumes/hubdisk/workspace/ai/ydb-native-macos-25.3.1.25/no-ambient-token \
./ya --version
```

The host had no pre-existing `/Users/urandon/.ya`; it remained absent. The
owned `cache/` remained zero bytes after both failed bootstrap attempts.

### Reproduced blocker

The pinned wrapper selected the expected `darwin-arm64` object but failed all
five attempts before receiving content:

```text
Downloading https://devtools-registry.s3.yandex.net/9750552540
ERROR: <urlopen error EOF occurred in violation of protocol (_ssl.c:1129)>
```

Running the same wrapper explicitly with Python 3.14/OpenSSL 3 reproduced:

```text
ERROR: <urlopen error [SSL: UNEXPECTED_EOF_WHILE_READING]
EOF occurred in violation of protocol (_ssl.c:1082)>
```

The endpoint resolved locally through a proxy fake-IP range. DNS-over-HTTPS
independently resolved its canonical `s3.yandex.net` address, but a direct
SNI/TLS attempt to that public address and an explicit attempt through the
configured localhost proxy both ended before response headers. The failure is
therefore recorded as a host/network-path blocker, not as proof that the
archive is absent.

A minimal GitHub Actions relay then fetched the exact object and verified the
upstream MD5, proving the object itself remained available. The one-day Actions
artifact was not anonymously downloadable and no broader `contents:write`
workaround was accepted. The temporary relay workflow is absent from the final
tree. Its immutable evidence is run
[`33997117393`](https://github.com/urandon/sessionless/actions/runs/33997117393),
commit `b763ac26d7705a8b77a8ec61fbec03c65dd10002`, conclusion `success`, artifact
digest `sha256:1f87b86ecd1d62af4557532b2af77fae3778c04899c9514926e75b0ef8e50d73`.

Because the verified bootstrap could not be transferred back through an
authorized channel, the required build command was not started:

```sh
./ya make ydb/apps/ydbd --build relwithdebinfo
```

This prevents false evidence: no architecture, Rosetta, linking, signing,
minimum-macOS, runtime, migration, recovery, APFS, or reproducibility claim is
made without a binary.

### Measured footprint

| Metric | Result |
| --- | ---: |
| Owned checkout and experiment root, logical | 2,417,512 KiB |
| Owned Yatool cache after failures | 0 KiB |
| Hubdisk available space after experiment | 1,543,758,980 KiB |
| System volume available space after experiment | 328,582,428 KiB |
| Coarse system free-space reading before/after | 313 GiB / 313 GiB |
| Native artifact retained | none |

The exact system-volume counter changed by about 29 MiB while unrelated host
and browser activity continued; it cannot be attributed to YDB. No Yatool data
appeared under the home directory. The hubdisk filesystem allocated about 257
MiB more blocks while the checkout reports 2.3 GiB logical size, consistent
with filesystem compression/shared storage. Both remained far from the abort
thresholds.

## Native-versus-container matrix

The existing container was deliberately not started merely to populate a
comparison after the native prerequisite failed. Doing so would add VM/image
and log pressure without answering the native feasibility question.

| Metric | Native macOS | Existing container path |
| --- | ---: | ---: |
| One-time peak build/download disk | not measured; bootstrap blocked | not re-run |
| Retained steady-state disk | no artifact | existing supported fallback unchanged |
| Cold startup to executable query | not run | not re-run |
| Warm startup | not run | not re-run |
| Idle host RSS/CPU | not run | not re-run |
| Integration-suite wall time | not run | not re-run |
| Shutdown/restart reliability | not run | not re-run |
| Host energy/thermal observations | not run | not re-run |

## Conditions for a defensible rerun

Reopen this decision only when all of the following are true:

1. The exact pinned bootstrap can be obtained through an authorized channel
   and verified against the MD5 embedded in the pinned wrapper, or upstream
   publishes a replacement with equally immutable provenance.
2. Upstream documents or continuously tests `ydbd`, not only the CLI/DSTool,
   on Darwin arm64 at the selected YDB revision.
3. Every Yatool build, tool, cache, log, and runtime root is demonstrated under
   the hubdisk before the first build download.
4. The same disk guards above are active and peak use is sampled throughout
   the build.
5. A produced binary passes `file`, Mach-O load-command, code-signature,
   Rosetta/process, startup/query, migration, restart/recovery, in-memory, and
   APFS-backed tests before any Sessionless opt-in is proposed.
6. Native and the pinned container are measured on the same host, with the
   container fallback and Linux CI left intact.

An upstream-supported macOS server recipe would justify a new implementation
task. A local patch stack, x86_64 binary under Rosetta, or a success that omits
storage/recovery semantics does not.

## Cleanup receipt

No cleanup has been performed. The exact reclaim candidate is:

```text
/Volumes/hubdisk/workspace/ai/ydb-native-macos-25.3.1.25
```

It was measured at 2,417,512 KiB logical and contains the pinned clean checkout
plus empty owned cache/build/log/runtime directories. It contains no retained
binary. Deletion still requires explicit confirmation and a fresh containment,
symlink, process-use, size, and free-space check. No broad parent directory,
home-directory cache, VM disk, container image, or unrelated checkout is part
of that candidate.
