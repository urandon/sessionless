# README and documentation information architecture

This document records the reader/content audit, the chosen information
architecture, and the deterministic visual-refresh procedure for issue
[#99](https://gitcode.com/urandon/sessionless/issues/99).

## Reader and content audit

The pre-change README was captured from the rendered GitHub `main` page on
2026-09-06 at commit `eb58cb978a9b`. Its first README viewport contained five
dense architecture and inventory paragraphs before any runnable path, visual,
status summary, or reader-specific call to action. The page then duplicated a
large component list and a flat command dump that are maintained more reliably
in the repository's docs and `Makefile`.

The audit also found:

- a stale statement that the browser UI was a separate future slice even though
  the Svelte UI and canonical Web API are implemented;
- no distinction between implemented, feature-disabled experimental, and
  planned capabilities;
- no representative product image or concise end-to-end model;
- 45 durable pages under `docs/`, but no goal-oriented top-level docs index;
- duplicated local-stand lifecycle and test commands across `development.md`,
  `local-development-stand.md`, and `local-e2e.md`;
- no automated repository-relative Markdown link and anchor gate.

The source capture is retained as
[`readme-audit-before-2026-09-06.jpg`](assets/readme-audit-before-2026-09-06.jpg).

## Reader journeys

| Reader | First answer | Next action |
| --- | --- | --- |
| Evaluator | What Sessionless is and what actually works | Inspect the product visual, then run the deterministic local flow. |
| Operator | Which environment and recovery boundary applies | Open the local or cloud runbook; never infer destructive commands from the overview. |
| Contributor | Which checks and code boundary own a change | Open contribution/development/testing docs and the component map. |
| Architecture or security reviewer | Where identity, ordering, credentials, and isolation are owned | Open normative contracts and security/trust-boundary docs. |
| Integrator | Which frontend, harness/provider, or attached-worker interface applies | Enter through the integration-surface map. |

## Chosen hierarchy

1. Promise, maturity, CI/license signals, and primary calls to action.
2. A real implemented-product visual with an explicit fixture caption.
3. Five user-value properties and one concise end-to-end diagram.
4. The shortest complete local path and a non-destructive stop note.
5. A delivery-status matrix tied to the four active implementation epics.
6. Reader-specific routes into `docs/README.md`.
7. Contribution, security, and license links.

Component inventory and command reference moved to `components.md`. The root
README is intentionally not the canonical owner of detailed architecture or
operations.

## Competitor pattern review

The review used pinned local snapshots from the centralized competitor corpus,
so its observations remain reproducible even if upstream READMEs change.

| Project and snapshot | Useful pattern | Decision for Sessionless |
| --- | --- | --- |
| [OpenAI Codex](https://github.com/openai/codex) `f5420174dafb` | One-sentence value, real product screenshot, quickstart, then docs | Adopt the order and visual grounding. |
| [OpenCode](https://github.com/anomalyco/opencode) `3a31c4ea8019` | Compact identity, badges, screenshot, install, docs | Adopt compact signals; avoid marketing density. |
| [Pi](https://github.com/earendil-works/pi) `6c4f36026439` | Clear harness framing and explicit permission warning | Adopt explicit trust and maturity language. |
| [Zed](https://github.com/zed-industries/zed) `d9ad6aff67e4` | Short promise with direct install/docs routes | Adopt reader-first routing. |
| [Hermes Agent](https://github.com/NousResearch/hermes-agent) `c80a0a551c70` | Strong visual hierarchy but long feature marketing | Use hierarchy, not an unverified feature catalogue. |
| [DeepSeek Harness](https://github.com/deepseek-ai/deepseek-harness) `b150a551b8d4` | Explicit developer-preview maturity and shortest runnable command | Adopt maturity language and a minimal local path. |
| [claude-code mirror](https://github.com/codeaashu/claude-code) `6a2590911df2` | Non-authoritative leaked-source framing | Retain as research evidence only; do not treat it as a product or security authority. |

## Visual refresh procedure

The visual must show the implemented Svelte UI, never a hand-drawn mock or
private environment. Its content is the same deterministic API fixture used by
browser tests.

1. Run `make web-ci`.
2. Run `make readme-visual-preview` and keep the printed loopback server open.
3. Open `/sessions/session-alpha` at a 1280 × 720 viewport in the in-app browser.
4. Capture only the product viewport to
   `docs/assets/sessionless-webui-session.jpg`.
5. Confirm the caption still identifies the fixture and that no credential,
   capability URL, private identifier, or user data is visible.
6. Run `make docs-check` and `make ci`.
7. Inspect the rendered branch README on both GitCode and the GitHub mirror;
   verify the image, Mermaid flow, headings, badges, and local links.

The preview server binds to `127.0.0.1`, serves `web/build`, implements only
read-only fixture endpoints, rejects non-GET requests, and has no cloud or
credential integration. `scripts/readme-visual-preview.mjs` is the canonical
fixture owner.

## Maintenance rules

- Update the delivery matrix only with issue/CI evidence from epics
  [#6](https://gitcode.com/urandon/sessionless/issues/6),
  [#13](https://gitcode.com/urandon/sessionless/issues/13),
  [#29](https://gitcode.com/urandon/sessionless/issues/29), and
  [#72](https://gitcode.com/urandon/sessionless/issues/72).
- Keep screenshots deterministic, source-owned, compressed, and accompanied by
  alt text and a fixture/maturity caption.
- Add new durable documents to `docs/README.md`; `make docs-check` fails when a
  page has no inbound documentation link.
- Put exact commands in the `Makefile` and their canonical runbook. Other pages
  should link instead of maintaining divergent copies.
