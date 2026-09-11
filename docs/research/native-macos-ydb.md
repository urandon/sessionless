# Native macOS YDB server spike

Status date: **2026-09-11**. Tracking issue: **#96**.

> **Later result (2026-09-10):** the bounded Yatool investigation subsequently
> produced and ran a Darwin x86_64 `ydbd` under Rosetta from the same
> `25.3.1.25` source pin. Sessionless migrations plus YDB and adapter
> integration suites passed without Docker. Native Darwin arm64 remains a
> no-go, and upstream still has no supported Darwin server lane. The exact
> successful build, artifact checksums, Dockerless lifecycle, and packaging
> decision are documented in
> [Dockerless local development on macOS](../macos-dockerless-development.md).

## CMake follow-up after the issue was reopened

The first pass tested the Sessionless-pinned stable tag through YDB's documented
Yatool path. After review requested an explicit CMake attempt, the follow-up
separately reconstructed the last complete Darwin-enabled generated CMake graph
and inspected upstream's CMake history and GitHub automation.

### Revision choice and upstream support boundary

None of the concrete stable candidates inspected can serve as an immutable
native-macOS CMake server pin:

| Candidate | Exact tag SHA | Root generated CMake graph | Exact `cmakebuild` generation mapping |
| --- | --- | --- | --- |
| `24.4.4.12` | `31a1edb9e704b354454b4b76778a5400b587136a` | absent | none found |
| `25.1.2.7-rc` | `1e3953ae0d2f31c0650f50c048ebf8aaaecfd032` | absent | none found |
| `25.1.2.7` | `e0e29e98f0614e18e20f2861d69a6ee12590ad52` | absent | none found |
| `25.3.1.25` | `8187ce049bed9e63a5ec04769da2d83dd237c08a` | absent | none found |

The retained `cmakebuild` history was searched for those four exact source
SHAs. This bounded result does not claim that every YDB tag ever published was
examined. It is decisive for the proposed Sessionless pin because:

- YDB's generated `cmakebuild` branch stopped updating in July 2025 and was
  later removed from the supported project surface;
- [PR #33872](https://github.com/ydb-platform/ydb/pull/33872), merged on
  2026-02-12, explicitly removed CMake support because upstream no longer
  officially supported that build;
- [PR #48873](https://github.com/ydb-platform/ydb/pull/48873), merged on
  2026-08-04, removed remaining CMake artifacts.

The final generated `cmakebuild` commit,
`673f99a2e60268b510e1e1838b23c9cb9e5d7fa3`, maps to source commit
`45c7adf3773c433f0a200060e3ed9cd2f4f92c4d`, but it is not a valid Darwin
probe: generator commit `6a6541d7e69112a5bb2c21d2059816f95bf8b5a3`
had already removed Darwin branches from the root and leaf wrappers. Merely
restoring the root include produced only a partially connected graph with
missing generated targets.

The bounded experiment therefore uses the immediately preceding complete
Darwin-enabled generated snapshot:

| Identity | Exact value |
| --- | --- |
| Generated CMake commit | `3ba801addecf906ac4159bf3dd1037056c4441fa` |
| Source commit recorded by `ydb/ci/cmakegen.txt` | `703afff9099973c6dd6cf063ca375d84dcc895c7` |
| Generated at | `2025-07-11T06:07:52Z` |
| First generator commit without Darwin wiring | `6a6541d7e69112a5bb2c21d2059816f95bf8b5a3` |

This is suitable for a historical feasibility probe, not a production pin:
it is an untagged development snapshot, predates the stable version used by
Sessionless, and has no upstream maintenance or security-update promise.

Upstream issue
[#13756](https://github.com/ydb-platform/ydb/issues/13756) records an M2 CMake
build and startup after local patches. Current issue
[#50178](https://github.com/ydb-platform/ydb/issues/50178) and open fix
[#50181](https://github.com/ydb-platform/ydb/pull/50181) show that Darwin arm64
link compatibility still needs source-level maintenance. These reports justify
running the experiment; they do not turn the historical branch into a
supported release surface.

### What upstream CI actually covered

The historical
[`postcommit_cmakebuild.yml`](https://github.com/ydb-platform/ydb/blob/3ba801addecf906ac4159bf3dd1037056c4441fa/.github/workflows/postcommit_cmakebuild.yml)
invoked the generated CMake graph through an Ubuntu package/toolchain setup.
Its
[`prepare_vm` action](https://github.com/ydb-platform/ydb/blob/3ba801addecf906ac4159bf3dd1037056c4441fa/.github/actions/prepare_vm/action.yaml)
installs Linux packages including `libaio-dev`; there is no native macOS
`ydbd` CMake matrix. Darwin arm64 jobs in that tree build the
[`ydb` CLI](https://github.com/ydb-platform/ydb/blob/3ba801addecf906ac4159bf3dd1037056c4441fa/.github/workflows/build_ydb_cli.yml)
and
[`ydb-dstool`](https://github.com/ydb-platform/ydb/blob/3ba801addecf906ac4159bf3dd1037056c4441fa/.github/workflows/build_ydb_dstool.yml)
through Yatool. Current upstream has removed the CMake workflows along with
CMake support.

The pinned `25.3.1.25` workflows make the non-CMake boundary equally explicit.
The Darwin matrix runs on `macos-13` and builds only the CLI with:

```sh
./ya make ydb/apps/ydb -r -DUSE_SSE4=no \
  --target-platform DEFAULT-DARWIN-ARM64
```

Its x86_64 sibling changes only the target platform to
`DEFAULT-DARWIN-X86_64`. The pinned nightly workflow does build
`ydb/apps/ydbd`, but only on upstream self-hosted build-preset runners and with
no Darwin matrix or target-platform flag. It publishes that server artifact to
YDB's build storage; it is not evidence of a macOS server artifact. Thus the
CI-derived local server hypothesis is the CLI command with the target replaced
by `ydb/apps/ydbd`, not an undocumented server-specific Darwin workflow.

### Contained reproducible tool inputs

All experiment-owned persistent archives, extracted tools, generated files,
build output, logs, and runtime state are rooted at
`/Volumes/hubdisk/workspace/ai/ydb-native-macos-cmake-45c7adf`. The build was
launched with `TMPDIR` under that root, and its `CMakeCache.txt` contains no
path below `/Users/urandon`; this is a containment receipt for the owned
experiment, not a claim that unrelated host processes caused zero transient
system-volume activity. A read-only symlink audit found three virtualenv links
to the Command Line Tools Python outside the root and eleven broken generated
upstream links to former developer `.ya` locations; the latter were not build
inputs, and disk measurements did not follow symlinks. The experiment uses:

| Input | Identity |
| --- | --- |
| LLVM/Clang | official `clang+llvm-18.1.8-arm64-apple-macos11.tar.xz`, SHA-256 `4573b7f25f46d2a9c8882993f091c52f416c83271db6f5b213c93f0bd0346a10` |
| Java runtime | Temurin JRE `17.0.20.1+1`, SHA-256 `190480874ccceb358cbc840393207f77ac3e63a4c5f8129d0e23e9518b96ad05` |
| ANTLR | bundled byte-identical jars, versions 3.5.2 and 4.11.1 |
| Ragel | version 6.10, compiled locally from the exact bundled source; binary SHA-256 `f57ddadc1a2a591ebb1847592a630eb190881aef1cf12b7924fb4a20ed495e95` |
| libidn | version 1.43, compiled locally from the bundled source with a local declaration for the bundled `strverscmp` fallback; archive SHA-256 `d17a77fe8c1d0cb9b1525bcaedd05de9d6c65ed344981a73ad6f949e8a0cc0b8` |
| GNU M4 | version 1.4.18, matching `contrib/tools/m4/ya.make`; official source archive SHA-256 `f2c1e86ca0a404ff281631bdc8377638992744b175afb806e25871a24a934e07`, arm64 binary SHA-256 `15fae0896af1ed1b7c6420e9ed1a40b92d6d322d78fc93a89e4f8c1b0b8c42f1` |
| GNU Bison | version 3.7.6, matching `contrib/tools/bison/ya.make`; official source archive SHA-256 `67d68ce1e22192050525643fc0a7a22297576682bef6a5c51446903f5aeef3cf`, arm64 binary SHA-256 `b542535f931704da6765c868b3146872d8c5fc95228ed8ed0cec88ece32aae6f` |
| Generator | CMake with Ninja, `Release`, macOS SDK from Command Line Tools |

The source diff is deliberately narrow: four files and fourteen added lines.
`clang.toolchain` selects `llvm-ar` and `llvm-ranlib`;
`cmake/FindAIO.cmake` supplies an imported interface target on Apple because
the Linux `libaio` API is not present there; and the custom LLVM-bitcode
compiler in `cmake/llvm-tools.cmake` receives CMake's already resolved macOS
SDK sysroot. The last build-system change was validated by a minimal probe:
the same bundled libc++ headers reproduced the `wchar.h`/`mbstate_t` failure
without `-isysroot` and passed with the configured `MacOSX26.5.sdk`.

The fourth change renames the vendored PostgreSQL fallback implementation of
`strchrnul` to `pg_strchrnul` inside its translation unit. Current macOS SDKs
declare the platform symbol while the historical deployment target still
needs PostgreSQL's fallback, so the original static definition no longer
compiles. This mirrors PostgreSQL upstream fix
[`6da2ba1d8a031984eb016fed6741bb2ac945f19d`](https://git.postgresql.org/gitweb/?p=postgresql.git;a=commit;h=6da2ba1d8a031984eb016fed6741bb2ac945f19d),
which was made for the macOS 15.4 SDK conflict. The exact previously failing
`snprintf.c.o` target passed after that rename. No YDB storage,
synchronization, allocator, query, actor, or recovery code is changed.

The retained final graph enumerates 61,858 Ninja targets and includes a
real `ydb/apps/ydbd/ydbd` target. The first build stopped because the frozen
graph invokes `build/bin/ragel` without creating its build target; compiling
the exact bundled Ragel source supplied that missing generated-tool edge. A
second attempt reached generated and compiled YDB sources before exposing an
incomplete local libidn header staging; copying the exact bundled private
headers corrected the local prefix without changing YDB source. The first
Darwin-specific compile failure occurred at the custom LLVM-bitcode step: its
generated command omitted CMake's configured SDK sysroot. After the bounded
build-infrastructure fix, the generated command contains the exact SDK path.
That allowed the bitcode archive to link and exposed two more missing
generated-tool edges: the graph invokes exact `bin/m4/bin/m4` and
`bin/bison/bin/bison` paths but creates neither tool. The matching upstream GNU
versions were built under the experiment root and staged into those paths.
Old M4's gnulib required the narrow modern-Darwin compatibility flag for its
`noreturn` function-pointer declaration and a one-line Apple guard that avoids
constructing a writable `%n` format rejected by current macOS libc. The
resulting M4 passed version and stdin expansion smoke tests, Bison reported the
expected version, and the previously failing Pire parser target completed.
Reconfiguring also rescheduled thousands of previously completed generated and
compiled targets, so a terminal build must be followed by an immediate no-op
pass before the historical graph can be described as converged.

The configured build was started with `-j4`. A read-only
`memory_pressure -Q` snapshot reported about 50% system-wide free memory while
individual heavy compiler processes reached roughly 1.5 GiB RSS; Ninja was
stopped by exact session/PID and resumed incrementally at `-j6`, then `-j8`
after repeated samples of the same metric stayed well above the 25% stop
threshold. During a later group of large `schemeshard` translation units,
`memory_pressure -Q` fell from 55% to 37%; Ninja was stopped through its exact
session handle before that metric's 25% threshold and resumed at `-j6`. The
same translation unit then ran while `memory_pressure -Q` reported 59%. The
safe retained target command is:

```sh
env TMPDIR=/Volumes/hubdisk/workspace/ai/ydb-native-macos-cmake-45c7adf/tmp \
  PATH=/Volumes/hubdisk/workspace/ai/ydb-native-macos-cmake-45c7adf/toolchain/clang+llvm-18.1.8-arm64-apple-macos11/bin:/usr/bin:/bin:/usr/sbin:/sbin \
  /opt/homebrew/bin/ninja \
  -C /Volumes/hubdisk/workspace/ai/ydb-native-macos-cmake-45c7adf/build/last-darwin-step1 \
  -j6 -k1 ydb/apps/ydbd/all
```

The explicit `TMPDIR` preserves experiment-root containment, and `PATH`
supplies the pinned LLVM `bin` directory because the generated archive rules
call bare `llvm-ar` and `llvm-ranlib`. This command
is a reproduction receipt for the retained experiment root, not a portable
Sessionless build recipe: the historical graph omits prerequisite edges for
Ragel, libidn, M4, and Bison and does not preserve the LLVM tool path; upstream
has removed the graph rather than stabilizing those inputs.

The interrupted invocation also exposed a convergence defect: restarting
Ninja regenerated protobuf outputs and dirtied dependent objects, so the next
invocation reported 3,717 remaining edges even though more than 2,000 edges of
the preceding invocation had completed. Those early generated edges executed
quickly, but the rebuild is material evidence that an interrupted build is not
incrementally stable. A terminal build still requires a separate immediate
no-op check; the report does not infer convergence from a successful link.

### CMake terminal result and bounded decision

The historical CMake experiment did **not** produce a `ydbd` binary. Its last
contained invocation reached `[2387/3717]` with no compiler or linker error,
then was deliberately interrupted through the exact Ninja session handle. The
preceding invocation had reached `[2039/3747]` before concurrency was reduced;
as described above, restarting exposed generated-output convergence defects
and materially repeated work. The eleven retained build logs record about
9 hours 48 minutes of active Ninja wall time between the first attempt and the
bounded stop; the end-to-end investigation spanned about 16 hours 44 minutes
including tool reconstruction, upstream research, bounded fixes, and pauses.
The final log marker is:

```text
[2387/3717] Building CXX object .../kqp_opt_log_sort.cpp.o
ninja: build stopped: interrupted by user.
```

This is a bounded cost stop, not a compiler failure. A conservative watcher
reported raw free pages below its 25% threshold, while macOS
`memory_pressure -Q` remained materially healthier in surrounding samples and
reported 57% immediately after the stop. The decision does not depend on that
metric discrepancy: even a successful remaining compile and link would still
need startup, executable-query, migration, integration, crash recovery, APFS,
reproducibility, and container-comparison work, while the only usable graph is
an untagged retired snapshot with known missing dependency edges.

The run therefore establishes that a narrow four-file portability patch can
drive a large part of the real Darwin arm64 server graph, but it does not
establish a buildable, reproducible, or runnable server. There is no defensible
stable native-macOS pin and no product case for continuing this port in the MVP
critical path. The recommendation remains **no-go** for a Sessionless native
YDB option; retain the container fallback and lower further investigation to
normal priority. The experiment root is intentionally retained so a future
upstream-supported change can resume from exact evidence rather than restart
the investigation from scratch.

## Stable-tag Yatool result

The initial pass concluded that Sessionless must **not** add native macOS YDB
from the pinned `25.3.1.25` release. This stable-tag result is a conditional
no-go, not a claim that some historical source can never compile on Darwin:

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

A final attempt used the exact Darwin CLI flags from pinned CI with the target
replaced by the nightly server target:

```sh
./ya make ydb/apps/ydbd -r -DUSE_SSE4=no \
  --target-platform DEFAULT-DARWIN-ARM64
```

`TMPDIR`, `YA_CACHE_DIR`, and `YA_CACHE_DIR_TOOLS` all resolved inside the
owned hubdisk root, and `YA_TOKEN_PATH` pointed to an explicit nonexistent
owned path. The wrapper again terminated before real compilation with
`ssl.SSLEOFError: UNEXPECTED_EOF_WHILE_READING` while fetching exact object
`9750552540`. A direct `curl` probe, a no-proxy probe, and an explicit
IPv4/TLS-1.2/HTTP-1.1 probe reproduced the same pre-response TLS termination.
No compiler process or build output was created.

An x86_64/Rosetta target retry cannot bypass this blocker. On an arm64 host the
wrapper must first fetch the same `darwin-arm64` Yatool bootstrap before it can
parse `--target-platform DEFAULT-DARWIN-X86_64`; changing the target platform
does not select a different host bootstrap. Repeating it would therefore be
the same failed network operation, not an independent build hypothesis.

No YDB source, storage, synchronization, allocator, or correctness check was
patched around in that pass. It produced no `ydbd` artifact and ran no native
process. The existing container runtime was not changed or restarted.

## Evidence classification

| Evidence | Classification | Consequence |
| --- | --- | --- |
| Tag `25.3.1.25` resolves to commit `8187ce049bed9e63a5ec04769da2d83dd237c08a`. | Observed | The experiment did not substitute `main` or a newer YDB release. |
| The release README says the minimum platform is x86_64 while also saying development builds are regularly exercised on current macOS and Windows. | Documented, internally broad | The general README is not sufficient evidence that a complete Apple Silicon server is supported. |
| The pinned build guide limits native Yatool use to x86_64 Ubuntu and limits CMake server builds to Ubuntu; macOS is listed for the CLI. | Documented, target-specific | There is no supported native macOS server recipe at this revision. |
| The `ydbd` target strips on Darwin and omits the x86-only Hyperscan peer outside `ARCH_X86_64`. | Observed | The graph anticipates Darwin/non-x86 compilation, but this is capability evidence, not a support or runtime-parity guarantee. |
| The bootstrap selects a dedicated `darwin-arm64` archive with object `9750552540` and MD5 `d8b7d17c095beb38007e0f55d6319cff`. | Observed | A pinned native bootstrap exists and can be retried without floating tool inputs. |
| Pinned GitHub automation builds `ydb`, not `ydbd`, with `DEFAULT-DARWIN-ARM64`. | Observed | Upstream continuously exercises a Darwin arm64 client lane, not the server acceptance boundary needed here. |
| Current YDB system requirements call macOS/Windows unsupported for production server operation and separately call ARM unsupported; Quick Start directs Apple Silicon development to an emulated x86_64 container. | Documented, current | Native arm64 server adoption would be an unsupported local divergence even if a one-off link succeeded. |
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
authorized channel, neither the documented command nor the CI-derived command
reached the Yatool build engine. This prevents false evidence: no architecture,
Rosetta, linking, signing, minimum-macOS, runtime, migration, recovery, APFS,
or reproducibility claim is made without a binary.

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

## Dockerless-versus-container closeout measurements

Measurements below were taken on the same 24 GiB M4 host. They deliberately
record different storage modes instead of presenting them as a strict
benchmark: Dockerless used a fresh file-backed runtime, while the disposable
container used in-memory pdisks. The retained Compose-volume result is a
reliability observation, not a startup-time comparison.

| Metric | Darwin x86_64 under Rosetta | Colima/container path |
| --- | --- | --- |
| One-time build/download disk | Yatool root measured 111,420,164 KiB at cleanup; historical CMake root 15,825,148 KiB | no image pull during closeout; pinned YDB image was already present and measured 1.1 GB |
| Retained steady-state disk | 2.0 GiB artifact directory, including baseline/stripped binaries, transfer bundle, and 13 MiB provenance archive; 502 MiB native dependencies; fresh runtime data 29 MiB | Colima root 61 GiB allocated on hubdisk; Docker reported 10.83 GB images, 13.99 GB volumes, and 1.81 GB build cache across all local projects, not exclusively Sessionless |
| Fresh startup to query-backed readiness | 28.52 s after host binaries were cached; first developer run including fresh Go downloads/builds was 134.66 s | 19.50 s for a disposable, already-downloaded image with in-memory pdisks; second fresh run on the warm VM was 29.35 s |
| Warm startup | 17.74 s against the same file-backed data | retained Compose volumes did not become ready: 153.45 s to `ReasonBootBSError` / `NumUnconnectedDisks=1` |
| Process memory sample | complete stand 1,257,568 KiB RSS; `ydbd` 702,576 KiB of that total | YDB container 504.9 MiB in a clean sample; the macOS VM process was about 2,925,792 KiB RSS under integration load with an 8 GiB configured VM limit |
| Integration evidence | local integration passed in 6.84 s wall / 3.514 s Go test; an earlier full YDB integration pass took 95.033 s | full YDB integration ran 208.36 s and hit one timing-sensitive expiry assertion; the exact test then passed 5/5 in 48.17 s wall / 31.424 s Go test |
| Shutdown/restart reliability | stop completed in 2.06 s; fresh and warm startup passed; an additional stressed reused-runtime trial later hit a YDB `GENERIC_ERROR` and is excluded from timing results | disposable runs stopped and auto-removed cleanly; the retained-volume boot failure reproduced the documented local-volume recovery risk |
| Host energy/thermal | not quantified: privileged power sampling was outside the experiment | not quantified |

The result supports an opt-in Dockerless fallback for developers who already
have the pinned artifact. It does not justify replacing the Linux container
and CI path: upstream still does not support the Darwin server, the local
binary requires Rosetta, the storage modes above are not equivalent, and both
paths exposed reliability variance worth keeping visible.

## Adoption gates after the research override

The human CMake follow-up superseded the first pass's narrow “wait for an
upstream Darwin `ydbd` lane before rerunning research” condition. Research may
reproduce a historical unsupported graph, as this follow-up does. Production
or default local-runtime adoption still requires all of the following:

1. A maintained, immutable source and generated-build identity with complete
   prerequisite edges, rather than an untagged retired CMake snapshot.
2. Every build, tool, cache, log, and runtime root demonstrated under hubdisk
   before the first large download, with the same disk guards sampled through
   the run.
3. A produced binary passing `file`, Mach-O load-command, code-signature,
   Rosetta/process, startup/query, migration, restart/recovery, in-memory, and
   APFS-backed tests before any Sessionless opt-in is proposed.
4. Native and the pinned container measured on the same host, with the
   container fallback and Linux CI left intact.
5. Explicit ownership of the ongoing Darwin portability/security patch burden
   if upstream still does not support or continuously test the server target.

An upstream-supported macOS server recipe would justify a new implementation
task. A historical local patch stack, x86_64 binary under Rosetta, or a success
that omits storage/recovery semantics does not justify adoption.

## Cleanup receipt

On 2026-09-11 the exact audited intermediate roots were deleted:

```text
/Volumes/hubdisk/workspace/ai/ydb-native-macos-25.3.1.25
/Volumes/hubdisk/workspace/ai/ydb-native-macos-cmake-45c7adf
```

Immediately before deletion they measured 111,420,164 KiB and 15,825,148 KiB,
respectively (127,245,312 KiB logical total). Hubdisk available space increased
by 126,325,592 KiB, about 120.5 GiB. The sole process using either root was an
owned loopback-only provenance relay; its exact PID, command, working directory,
and listener were verified before it was terminated. Shadowrocket and its
packet-tunnel process remained running with unchanged PIDs.

The stable artifact directory was excluded from cleanup:

```text
/Volumes/hubdisk/workspace/artifacts/ydb/25.3.1.25/darwin-x86_64
```

Its nine-file `MANIFEST.sha256` passed verification. Raw Yatool/CMake logs,
metadata, and the unsuccessful and successful patch stacks were compacted into
`sessionless-ydb-issue96-provenance-20260911.tar.gz` (13 MiB), with SHA-256
`c0b199175accc680af1819337008f07e7c553cd5188a4ae33bb3f8c5872fc1f3` and an
adjacent `PROVENANCE.sha256`. No VM disk, container image, persistent Docker
volume, home-directory cache, or unrelated checkout was deleted.
