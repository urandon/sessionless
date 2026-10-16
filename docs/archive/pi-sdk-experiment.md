# Archived Pi integration experiment

This branch preserves abandoned, **non-release** Pi SDK/SPU/HTTP work for
historical inspection. It is not an active MVP path and must not be merged back
as a working adapter. Cleanup owner: issue #213.

- Main snapshot before removal: `fc47a5809e5411e517b77e48fd0e34cad83779d1`,
  preserved as `codex/archive-pi-main`.
- Last committed experiment before this archive snapshot:
  `1b90ef33fe980f8e37b729c1cb7234162dce582a`.
- This snapshot also preserves the six previously untracked Pi source/test
  files, including the earlier boundary prototype and four external-package
  constructor/SDK functional-test files.

Some isolated synthetic composition tests passed; the latest repeated and full
functional runs failed on process deadlines. Independent source review does not
turn those failures into runtime acceptance. Real-profile sizing, installed
transport policy, private final-result custody and complete product integration
were not delivered. No real provider/model/credential or cloud proof is claimed.

The branch contains source and synthetic fixtures only. SDK dependency trees,
build caches, models, credentials and private runtime payloads are not archived
here. The ordinary application must not select this experiment.

The architecture direction is to keep isolation/process supervision and any
future common sidecar at the shared worker boundary, not to maintain a Pi-only
replacement HTTP transport around an SDK. Generic local inference connection
work is tracked separately in issue #212.
