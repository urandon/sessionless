# Codex App Server replay fixtures

These files are synthetic replay recipes derived from the stable schema emitted
by `codex-cli 0.148.0-alpha.15`. They contain no real account, token, device
code, workspace, thread, or user content.

Each line is one JSON replay record:

- `{"kind":"frame","direction":"client|server","message":{...}}` wraps one
  exact JSONL protocol frame;
- `{"kind":"raw","direction":"server","data":"..."}` injects raw bytes,
  usually malformed input;
- `{"kind":"repeat",...,"prefix":"...","unit":"x","count":N,"suffix":"..."}`
  constructs a large raw frame without storing it verbatim;
- `{"kind":"stall","direction":"server","durationMs":N}` advances the fake
  clock without producing bytes.

Record order is authoritative. A replay test sends `client` frames to the fake
server, feeds `server` frames/raw data to the adapter, and interprets stalls
using a fake clock. Values are illustrative protocol data, not observations of
a real provider account.

`malformed-oversized.jsonl` uses 1 MiB as a proposed parser test bound. The
production bound must be explicit and may be smaller; the test should construct
`configuredMaximum + 1` bytes rather than depend on that candidate value.
