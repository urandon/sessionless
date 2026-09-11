# Dockerless local development on macOS

The Dockerless stand runs the same YDB migrations and Go adapters as the
Compose stand, but every service is a host process. It does not start Docker,
Colima, Lima, a Linux VM, or pull an Ubuntu image. The opt-in contract is an
explicit `YDBD_PATH`; without it, the existing `make e2e-local` path continues
to use Docker Compose.

This path is tracked by [#126](https://gitcode.com/urandon/sessionless/issues/126).
The unsupported upstream build investigation and the retained artifact
provenance are tracked by
[#96](https://gitcode.com/urandon/sessionless/issues/96).

## Required local artifacts

The repository does not download large binaries implicitly. Put them on the
large workspace volume and export their paths:

| Input | Supported development pin | Environment variable |
| --- | --- | --- |
| YDB server | `25.3.1.25`, Darwin x86_64 | `YDBD_PATH` |
| upstream YDB local launcher | same source revision and target | `YDB_LOCAL_LAUNCHER_PATH` |
| MinIO server | `RELEASE.2025-09-07T16-13-09Z`, Darwin arm64 | `MINIO_PATH` or `SESSIONLESS_NATIVE_DEPS_DIR` |
| MinIO client | `RELEASE.2025-08-13T08-35-41Z`, Darwin arm64 | `MC_PATH` or `SESSIONLESS_NATIVE_DEPS_DIR` |
| ElasticMQ | `1.6.16` all-in-one JAR | `ELASTICMQ_JAR` or `SESSIONLESS_NATIVE_DEPS_DIR` |
| Java | Temurin JRE 21, Darwin arm64 | `JAVA_PATH` or `SESSIONLESS_NATIVE_DEPS_DIR` |

`YDB_LOCAL_LAUNCHER_PATH` is needed only to initialize a fresh YDB runtime.
After it has generated `YDB_CONFIG_PATH`, ordinary starts invoke the specified
`ydbd` directly. The launcher is retained because its generated single-node
configuration and `/local` bootstrap are upstream behavior; Sessionless does
not maintain a hand-written substitute.

The default dependency layout is:

```text
workspace/
  ai/
    sessionless/
    sessionless-native-deps/
      bin/minio.RELEASE.2025-09-07T16-13-09Z
      bin/mc.RELEASE.2025-08-13T08-35-41Z
      elasticmq/elasticmq-server-all-1.6.16.jar
      jre-21/Contents/Home/bin/java
  artifacts/ydb/25.3.1.25/darwin-x86_64/
    ydbd-25.3.1.25-darwin-x86_64
    local_ydb-25.3.1.25-darwin-x86_64
```

All mutable data, logs, PID metadata, worker scratch, Go caches, and built Go
binaries default to `.build/dockerless` and `.build` in the checkout. Because
the checkout is under `workspace`, these paths stay on the large volume.
`SESSIONLESS_DOCKERLESS_ROOT` may select another absolute, narrowly scoped
directory on that volume.

## Optional direnv setup

The repository has no committed `.envrc`. A real `.envrc` is ignored because
it is machine-specific and may later load secrets. Copy the safe template,
review the resolved paths, and explicitly allow it:

```sh
cp .envrc.example .envrc
direnv allow
```

Do not add credentials to `.envrc.example` or commit a real `.envrc`.

## Lifecycle

From a checkout on the workspace volume:

```sh
YDBD_PATH=/absolute/path/to/ydbd \
YDB_LOCAL_LAUNCHER_PATH=/absolute/path/to/local_ydb \
make dockerless-up

make dockerless-status
make local-integration
make e2e-local-dockerless
make dockerless-down
```

Supplying `YDBD_PATH` to the ordinary E2E target also selects this backend:

```sh
YDBD_PATH=/absolute/path/to/ydbd make e2e-local
```

The stand is intentionally left running after E2E for inspection. Use
`make dockerless-logs` for bounded component tails and `make dockerless-down`
for a graceful stop. Down validates the recorded command before signalling a
PID, refuses stale or mismatched ownership metadata, never force-kills a
process, and preserves all data. It does not manage a process merely because
that process happens to occupy a known port.

Retained component logs are reduced to their newest 10 MiB before a process
starts, after the stand stops, and after each one-shot worker invocation.
`DOCKERLESS_LOG_MAX_BYTES` may set another positive byte limit. This bounds
growth across repeated development cycles while preserving the latest failure
evidence.

Dockerless E2E uses the same Go test package as Compose. The test backend runs
the one-shot worker binary directly and delegates queue stop/start plus
reconciler restart to the owned-process lifecycle script. This preserves the
outage and recovery assertions instead of skipping them.

## Rebuilding YDB for Darwin x86_64

YDB does not provide a supported Darwin server release or CI lane at this pin.
The following is the exact successful local cross-build shape from #96: the
host tools run natively on Apple Silicon while the target is Darwin x86_64,
then the resulting Mach-O executable runs through Rosetta 2.

Prepare every large path on the workspace volume before invoking Yatool:

```sh
export YDB_BUILD_ROOT=/Volumes/hubdisk/workspace/ai/ydb-native-macos-25.3.1.25
export TMPDIR=$YDB_BUILD_ROOT/tmp
export YA_CACHE_DIR=$YDB_BUILD_ROOT/cache
export YA_CACHE_DIR_TOOLS=$YDB_BUILD_ROOT/cache/tools
export YA_TOKEN_PATH=$YDB_BUILD_ROOT/no-ambient-token

cd "$YDB_BUILD_ROOT/source"
./ya make ydb/apps/ydbd -r \
  --target-platform DEFAULT-DARWIN-X86_64 \
  -j6 --link-threads=2 \
  -B "$YDB_BUILD_ROOT/build-native-x86_64"

./ya make ydb/public/tools/local_ydb -r \
  --target-platform DEFAULT-DARWIN-X86_64 \
  -j6 --link-threads=2 \
  -B "$YDB_BUILD_ROOT/build-native-x86_64"
```

The successful build used tag `25.3.1.25`, commit
`8187ce049bed9e63a5ec04769da2d83dd237c08a`. Do not silently substitute
`main`. The server build took about 11 hours 32 minutes on the 24 GiB M4 host;
the retained build root reached about 116 GiB. `-j6` with two linker threads
was the faster stable incremental setting found after a conservative `-j2`
start. Treat those figures as capacity guidance, not a performance guarantee.

At this revision the legacy Yatool registry SNI was reset on the host network.
The successful build used the supported, process-local `YA_REGISTRY_ENDPOINT`
override pointed at a loopback relay whose fetched objects were checked for
exact size, archive integrity, and SHA-256. Do not change system DNS, proxy
settings, Shadowrocket, or disable TLS verification. The incident and relay
safety boundary are recorded in
`workspace/network/docs/yatool-registry-sni-reset-2026-09-09.md`.

Yatool places final files beneath content-addressed `symres` directories.
Resolve them from the successful build log/graph, verify the architecture, and
copy them to the stable artifact directory:

```sh
file /path/from/symres/ydbd /path/from/symres/local_ydb
shasum -a 256 /path/from/symres/ydbd /path/from/symres/local_ydb
/usr/bin/arch -x86_64 /path/from/symres/local_ydb --help
```

The retained unmodified server artifact is 812,026,360 bytes with SHA-256:

```text
012ec66f9d64b548b9608be47ed54469f6218dde4b39ef2dbdc26bc10ee5f25c
```

The retained unmodified launcher is 94,234,464 bytes with SHA-256:

```text
7e28ecb8a9a7cffee8be4bd0a46bd30fe298ad361363fc3494d53199eb0bebd8
```

## Stripping and artifact publication

Never strip the only copy. On measured copies, `/usr/bin/strip -S -x` changed
the server from 812,026,360 to 579,484,512 bytes (28.6% smaller). `zstd -19`
then produced 121,412,424 bytes, compared with about 154 MiB for the retained
compressed unmodified build. The stripped server remained an x86_64 Mach-O,
loaded under Rosetta, and kept the same dynamic-library set. Its measured
SHA-256 values are:

```text
stripped:      43d9c1dbdbd87fa4622ae5eb700ab0a4bd0ee22314031020ceefb06bae544250
stripped.zst:  ddc7556b1316852064da615dcba4e589dbb6eb99a7077264835eb1be33327001
```

The same operation reduced the launcher from 94,234,464 to 80,521,872 bytes;
its `--help` smoke passed. This size saving is less compelling because the
launcher is used only for provisioning.

Recommendation: do not commit either binary to Git. Publish a checksummed
`.tar.zst` release asset containing the unmodified `ydbd`, unmodified
`local_ydb`, `MANIFEST.sha256`, the exact source revision/build command, and
the upstream license/notices. The unmodified pair is the diagnostic baseline.
Offer a clearly suffixed stripped runtime asset only after it passes fresh
deployment, migrations, Dockerless E2E, restart, and failure-log validation;
stripping removes symbols useful for unsupported-platform crash diagnosis.
GitHub Releases on the authoritative mirror are suitable for laptop download
and existing release automation; Git LFS or ordinary repository blobs are not.

A local unmodified transfer bundle has been prepared at
`/Volumes/hubdisk/workspace/artifacts/ydb/25.3.1.25/darwin-x86_64/sessionless-ydb-local-25.3.1.25-darwin-x86_64.tar.zst`.
It is 171 MiB and has SHA-256
`6b669c1b264e300ec8f4c92ae11298090e2370a49e6704788b58ebb46cf2c6dd`.
The adjacent `MANIFEST.sha256` covers both baseline and stripped files. The
bundle itself contains the unmodified server and launcher, its own checksum
manifest, build provenance, `LICENSE`, and `AUTHORS`. It is a local retained
artifact, not a published or supported upstream release.

The local build is unsigned. Record `codesign -dv`, `otool -L`, `file`, the
macOS/Xcode toolchain, and both compressed and uncompressed checksums in the
release manifest. A user downloading the asset may need to handle quarantine
according to local macOS policy; do not instruct users to disable Gatekeeper
globally.

## Troubleshooting and cleanup

- If startup says `YDB_LOCAL_LAUNCHER_PATH` is required, the selected runtime
  has no generated configuration yet. Supply the launcher built from the same
  source revision as `ydbd`.
- If a port belongs to an unmanaged process, inspect it and stop it through its
  original owner. The Dockerless script deliberately refuses to adopt or kill
  it.
- A YDB monitoring response is liveness only. Successful embedded migrations
  are the query-backed readiness gate.
- `make dockerless-down` preserves YDB and MinIO data. There is intentionally
  no Dockerless reset target; inspect and confirm an exact runtime directory
  before deleting local data manually.
- Build-root cleanup is separate from runtime cleanup. On 2026-09-11 the
  audited Yatool and historical CMake roots were deleted after explicit
  confirmation, reclaiming 126,325,592 KiB (about 120.5 GiB) of hubdisk space.
  The stable artifact directory remains intact. Build and experiment logs,
  metadata, and patch provenance are retained in
  `sessionless-ydb-issue96-provenance-20260911.tar.gz` beside the binaries;
  verify it with the adjacent `PROVENANCE.sha256` before use. Regenerating
  intermediate objects requires the pinned build recipe above.
