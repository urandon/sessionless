# Product design rules

Status: shared product presentation/acceptance baseline; no visual overhaul or browser control activation.
Owner: [#138](https://gitcode.com/urandon/sessionless/issues/138).
Product scope: [UX contract](product-ux-contract.md); administrative authority:
[admin contract](tenant-federation-admin-contract.md).

Source provenance: wiki snapshot inspected on 2026-10-01 at
`1a6b208468dc99a53a9eb9f4e1444e9716a8aad4`; documentation delivery base
`a463e9780651667be8a28b0bdce339688c7ad453`. Relevant domain/ports/BFF/Svelte,
blob-delivery and existing UX paths were unchanged between these revisions.
File/line evidence below is pinned to the inspection snapshot, not a promise
about an arbitrary future branch.

Independent source-snapshot resolutions:
[#138 CLEAN](https://gitcode.com/urandon/sessionless/issues/138#note_192407004)
and [#139 CLEAN](https://gitcode.com/urandon/sessionless/issues/139#note_192407016).
[Owner acceptance](https://gitcode.com/urandon/sessionless/issues/139#note_192407231)
selects the **initial** D1–D5 scope only. The extracted document delta still
requires normal independent MR review and exact-head checks/CI; source CLEAN
does not transfer to changed text automatically. No implementation, deployment,
migration, provider/credential access or runtime activation is authorized here.

## Shared semantic and interaction rules

1. Scope is visible and persistent: workspace, environment and resource/owner context. Production-destructive controls require stronger differentiation than color.
2. Keep chat primary. Progressive disclosure reveals attempts, worker diagnostics, accounting and recovery without making the transcript an infrastructure console.
3. Reuse #82's truth vocabulary: authoritative, observed, derived, historical failure, acknowledgement, unknown. Current errors and historical failures must not overwrite one another.
4. Preserve lifecycle distinctions: requested vs acknowledged vs process-stopped vs canonical-committed; fenced does not mean erased. RunStatus remains the existing domain enum.
5. Unknown/partial/stale is not zero, healthy, ready or succeeded. Include freshness and coverage on operational and usage views.
6. Controls come from server capabilities plus reasons. Hide irrelevant controls; disabled relevant controls explain the prerequisite and point to an authorized next step.
7. Every mutating control maps to a typed port/API, exact target/scope, idempotency and revision guard. High-risk operations require preview/confirmation and inspectable receipt.
8. Never acquire credentials, probe a provider, enroll a worker or repair state by opening a read screen.
9. Reuse the current visual foundation: warm light paper, ink/muted text, rust accent and blue focus token from `web/src/styles.css`. No simultaneous visual overhaul. Proposed spacing rhythm: 8px multiples; interactive target aim: 44px; verify rather than claim compliance.
10. Keyboard, visible focus, labels and text+icon statuses are mandatory. Async updates preserve focus; status regions avoid announcing every poll. Error messages give next actions.
11. At narrow widths, prioritize transcript/composer and use a recoverable drawer for detail. Never require a hover-only interaction or horizontal scrolling to perform the main action.
12. Poll bounded active views with server hints/backoff; pause hidden views, cancel discarded requests and bind caches to authorization generation. Existing SessionDetail already pauses hidden-tab polling; do not duplicate that task.
13. Metering displays metric scope/time/unit/provenance/freshness/coverage/precision. Token observations can be shown before invoice reconciliation, but not relabelled reconciled costs.
14. Shared-host consent states that the host operator can technically access admitted execution data. Omitting prompts from an admin UI does not provide host privacy.

## Reusable drawer and confirmation acceptance

| Interaction | Verifiable acceptance |
|---|---|
| Open/close from keyboard | Trigger is labelled and keyboard-operable. Enter/Space follows native semantics; Escape closes dismissible overlays without submitting a mutation. Modal focus containment applies only to a true modal; non-modal drawers retain an appropriate navigation model. |
| Initial/return focus | Opening places focus at a documented useful heading/control. Explicit close returns focus to the trigger; if it disappeared after permission/state change, use a documented safe fallback. |
| Refresh/action updates | Poll/status refresh never steals focus or resets unsent input. After permission loss, remove unavailable controls safely and expose the new reason. |
| Error association | Each field error is programmatically associated with its input; submission errors have a labelled summary/next action and reachable focus target. |
| Announcements | Announce material completion/denial/permission changes with appropriate live-region semantics; do not announce every poll/spinner. |
| Zoom/narrow view | Main action, cancel and receipt remain usable at 200% zoom and a 320 CSS-pixel viewport without hover-only interaction or horizontal scrolling for the primary task. |

Map this matrix to #141 explanation drawer, #143 compute choice, #145 Stop/recovery, #147 tenant confirmations and #149 pool contribution/withdrawal fixtures. Reuse existing skip links, focus tokens and hidden-tab pause; these are acceptance targets, not claims that browser tests were run or current UI fails them.


## Reuse and evidence

The current foundation is [styles.css](../web/src/styles.css) and existing
[AppShell](../web/src/lib/components/AppShell.svelte),
[SessionDetail](../web/src/lib/components/SessionDetail.svelte) and
[worker action catalog](../web/src/lib/components/AttachedWorkerActionCatalog.svelte).
Reuse focus/skip-link/hidden-tab behavior rather than introduce another parallel
system. The 8px rhythm and 44px target are design aims, not measured compliance
or a waiver of route-specific accessibility/browser testing.

Wireflows and contracts precede new interactive affordances. A missing command
must stay unavailable with a safe explanation; it must not be faked as a
working control. Platform read-only initial scope and tenant/resource owner
permissions follow the accepted scope, not the existence of a navigation tab.

## Source custody and review limits

| Reviewed wiki body | Exact UTF-8 SHA-256 |
| --- | --- |
| [Product-UX-Contract-Draft-2026-10-01.md](https://gitcode.com/urandon/sessionless/wiki/Product-UX-Contract-Draft-2026-10-01.md) | `c1065fb360df108fa1e95bbb71992fe786397fbd5888b4c5060272c16a271514` |
| [Tenant-Federation-Admin-Draft-2026-10-01.md](https://gitcode.com/urandon/sessionless/wiki/Tenant-Federation-Admin-Draft-2026-10-01.md) | `5f11c974aa436b6e8e5b36f23be1d4156c92f0d8633708207e4610392b964b4e` |
| [UX-Delivery-Roadmap-Draft-2026-10-01.md](https://gitcode.com/urandon/sessionless/wiki/UX-Delivery-Roadmap-Draft-2026-10-01.md) | `54de32273868786754c87ad51ffd0388fa3bcff3cd951d352446df523e8a0986` |

These are source-body hashes, not hashes of these extracted files. The source
wiki remains an immutable review reference for this delivery; this extraction
splits design rules, incorporates the additive owner decision receipt, labels
future capabilities, and adds a screen/API/evidence handoff. Review those
changes independently. GitCode issues own live execution status; this document
does not certify production readiness or close the issues.
