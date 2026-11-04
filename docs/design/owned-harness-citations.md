# Durable answer citations

Version: 0.1. Implementation owner: [#196](https://gitcode.com/urandon/sessionless/issues/196);
integration owner: [#197](https://gitcode.com/urandon/sessionless/issues/197).
This refines the accepted [search citation requirement](owned-harness-web-search.md#normalized-results-and-citations)
without changing search dispatch, consent, credential or activation authority.

The managed harness returns a typed final answer whose source references resolve
only to successful, durable search results from the same Run and Attempt. The
trusted finalizer, not the model or browser, supplies source metadata. WebUI
labels links as search excerpts, not proof of page reading or claim support.

## Evidence and alternatives

The accepted [comparison](../research/web-search-mvp.md#agent-and-harness-mechanisms)
separates search results from page retrieval, records structured citations in
provider APIs, and rejects provider-native opaque loops as our portable search
contract. Reuse bounded source IDs and explicit provenance, not provider-specific
annotations or an agent SDK. Its observations are pinned or historical; this
contract does not claim a current competitor implementation or quality result.

| Representation | Decision |
| --- | --- |
| A strict JSON value in final message text | Select for the explicitly admitted citation instruction version. It works through the existing final-text response and needs no provider-native JSON or citation capability. Invalid output fails; there is no repair call. |
| Native provider citation annotations | Do not use as canonical authority. They are not portable to the configured local inference path and do not prove our durable operation lineage. |
| URLs or citation markers scraped from prose | Do not use as the manifest. Escaping, accidental markers and model-supplied URLs must not select trusted source metadata. |
| A new final-answer tool | Do not add another tool/schema or consume another dispatch allowance just to terminate the loop. |

The [configured response](../../internal/configuredinference/contract.go) already
separates final text from tool calls. The [compaction decoder](../../internal/configuredinference/compaction_summary.go)
provides an existing pattern for bounded strict JSON inside that text. Neither
pattern authenticates the model's statements.

## Model final answer format

Add one explicit server-admitted instruction version
`sessionless.owned-agent.citations.v1`. Its exact prompt bytes, digest and token
fit belong to the admitted profile; selecting it changes the profile/binding
identity. Do not infer the format from a successful JSON parse, the presence of a
source, an environment flag or the latest history item. Existing instruction
versions and final-text bytes keep their current interpretation.

In this version, a final MODEL response has `StopFinalV1`, no function call, and
text containing exactly this object:

```json
{"schema":"sessionless.cited-answer.v1","answer":"A bounded answer.","source_ids":[]}
```

Apply the existing final-answer scalar constraints to the DECODED `answer`:
nonempty valid UTF-8, no U+0000, and at most the admitted `FinalTextBytes`.
An escaped `\u0000` in otherwise valid outer JSON therefore denies the answer.
Preserve the validated decoded text exactly, including whitespace and Unicode;
the v2 encoder must not call the v1 `NormalizedSummary` trim/fallback helper.
There is no HTML interpretation. `source_ids` is an ordered
array of zero to ten distinct complete IDs using the existing
`source-<64 lowercase hexadecimal characters>` grammar. There is no model-supplied
URL, title, digest, tenant selector or claim-support assertion. Preserve array
order; do not merge different source IDs merely because their URLs match.

The complete JSON text is at most the smaller of the existing admitted
`FinalTextBytes` and 16 KiB. Count wire text, JSON escaping and references within
that same allowance; it is not an additional answer budget. Bound nesting to
three levels and members to sixteen. Reject duplicate/aliased/unknown fields,
null/missing fields, wrong types, invalid UTF-8, duplicate IDs, trailing data,
Markdown fences and overflow. Do not repair, truncate, silently remove bad
references, fall back to ordinary text or repeat the model operation.

The existing normalized MODEL record retains the exact final JSON text and
operation correspondence. A pure decoder returns typed candidate answer DATA;
it grants no right to publish. An empty reference list remains an uncited answer,
including when search returned nothing or the model does not use its results.
The product must not call such an answer grounded merely because a search ran.

## Trusted source resolution

The original terminal owner derives its source table from the complete durable
operation frontier that already feeds terminal validation, not from a browser
request, public projection, cached URL list, model echo or caller-created map.
For each eligible SEARCH it verifies the original reservation, completed
observation with reason `none`, exact response bytes/digest, strict
`WebSearchResultV1` and the existing source-ID/rank/URL validation. Only
`known_results` contributes entries. Known-empty/service-error/unknown/rejected
outcomes contribute none and never become invented successful evidence.

Each selected reference must resolve uniquely in that exact tenant, owner,
Session, Run, Attempt and physical claim. Existing source IDs include the
operation digest; retain them unchanged. Reject invented IDs, crossed owners or
Attempts, duplicate IDs with conflicting metadata and any mismatch between
retained MODEL text, SEARCH observation and resolved source. A different Run's
old citation can remain quoted history but is not eligible for this Run's new
manifest. A valid map supplied to a pure parser is not proof of this join.

Resolution performs no DNS lookup, page fetch, provider request, new grant read
or operation replay. It consumes the original terminal owner's verified bytes
and accounting/custody frontier, with its existing current-authority and cleanup
checks. Historical readers later authenticate retained seals rather than
requiring live credentials or reconstructing erased requests.

## One citation manifest

Use one canonical `AnswerCitationManifestV1` value with schema
`sessionless.answer-citations.v1`, `run_id`, `attempt_id` and ordered `sources`.
Each entry has exactly:

| Field | Source |
| --- | --- |
| `source_id`, `rank`, `url`, `title`, `title_truncated` | Exact selected `WebSearchSourceV1` values |
| `operation_digest` | Original SEARCH reservation's operation digest |
| `observation_digest` | Digest of its original durable completed observation |
| `response_digest` | That observation's digest of the exact retained normalized response bytes |
| `retrieved_at` | Original `WebSearchResultV1.RetrievedAt` |
| `published_at` | Exact source publication time when present; otherwise omit |
| `content_kind` | Fixed value `search_excerpt` |

Do not include excerpts, raw queries, arguments, credentials, grant/consent
receipts, vendor error bodies or a `verified`/`supports_claim` boolean. A query
can appear in model prose or an untrusted title; the manifest is not a private
data classifier or a promise that arbitrary provider text lacks query words.

The complete canonical manifest is capped at 64 KiB and ten entries, within all
existing output/materialization/artifact/capsule budgets. URL/title limits remain
the existing 2,048/512 UTF-8 bytes; never shorten a required URL or digest to fit.
Its strict decoder limits depth to five and members to 160 and rejects unknown,
duplicate, aliased or null fields, wrong types and trailing data; only
`published_at` may be omitted. Validate every source's existing ID grammar,
digests, safe-URL syntax and timestamps before allocating public render state.
Before any affected PUT or send, the existing prospective fit uses the SAME
encoder and bounds for the final JSON, resolved manifest, assistant payload,
capsule, failure residue and all owned copies. This adds no allowance to the
original four-MiB logical-call pool or any operation/terminal limit. A profile
that cannot reserve the complete supported representation must stop before the
affected effect, not discard citations after paying for it.

## Canonical output and refresh

Introduce `sessionless.assistant-message.v2` with the exact fields `schema`,
`summary`, `artifact_manifest_id` and `citations`. `summary` is the decoded answer;
`citations` is the single typed manifest above, including an empty `sources`
array. Do not append fields to v1, infer a version by field presence, or change
old MCP-only/text payloads and goldens.

The original finalizer rederives the manifest from the captured frontier and
checks its exact canonical bytes before producing v2 output. Its prepared output,
terminal capsule/accounting seal, existing `WorkerCompletion` event drafts and
prepared canonical material bind that same payload and its BlobRef/digest. The
canonical assistant blob is the citation-bearing output artifact; this contract
does not add a duplicate downloadable file, global citation table or alternate
artifact lifecycle. A transient `ExecutionResult` field alone is insufficient.

Versioned readers must land before the v2 writer: canonical material preparation,
retained completion/replay/history/deletion, session/context readers and public
Web projection all select the explicit schema and enforce its closed grammar.
V1 remains v1, unsupported versions deny, and erased or corrupt retained material
does not turn into an ordinary uncited success. No schema migration is required
for a versioned immutable event payload.

Refresh and re-login return the same canonical answer/manifest through the
existing membership-authorized event read, without rerunning search or resolving
URLs again. Permission loss clears private rendered content by the existing Web
state lifecycle. Compaction preserves source IDs, operation/observation/response
digests and complete tool lineage as historical DATA; it cannot mint a new
eligible source or turn a derived summary into a verified original. Keep
compaction-reference IDs and web-search source IDs in their distinct grammars.

## WebUI behavior

Show a labelled list such as “Search excerpts referenced by this answer” below
the answer, in manifest order. Each keyboard-accessible link uses the exact
validated HTTPS URL and a text-escaped title with an accessible source label.
Use `rel="noopener noreferrer"` for new tabs and `referrerpolicy="no-referrer"`.
Do not insert HTML, inline page previews, iframes, link prefetches or automatic
fetch/DNS requests. A deliberate user navigation is not a harness fetch tool.

Show retrieval time separately from optional publication time. Explain that
search excerpts do not establish that the page was read or that the answer's
claims are supported. Empty manifests do not produce an “all sources verified”
badge. Mark abbreviated titles when `title_truncated` is true. Ordinary prose
URLs are not promoted to provenance links. This minimum
uses an answer-level source list, not sentence offsets, a claim graph or a
relevance score; useful claim-to-source support is measured separately.

## Implementation and verification

The [#176 implementation epic](https://gitcode.com/urandon/sessionless/issues/176)
already links #196 and #197. Keep their original assignments; record these
bounded work items in the executor's comment:

1. Add strict candidate/final manifest types and parsers, trusted resolver,
   pure encoders and explicit instruction version. Preserve old bytes; test
   exact bounds, duplicate keys, escaping, malformed JSON, fabricated/crossed
   source IDs, two searches, unknown/error outcomes and no repair/replay.
   Pair decoded-NUL rejection with escaped-Unicode and ordinary-text positive
   controls, and prove exact decoded text survives v2 output and context framing
   without the v1 summary trim/fallback path.
2. Add all v2 output/material/retention/context/Web readers before writer
   activation. Update the entity registry classification for the existing
   canonical payload, without creating storage entities. Test mixed history,
   corruption, deletion, compaction identity and denied permission reads.
3. Connect the original finalizer and prospective fit to the SAME encoder,
   durable frontier and payload seal. Prove actual MODEL → SEARCH → MODEL
   answer resolution, source substitution denial and no post-effect truncation.
4. Add accessible Web source lists and generated public contract, with refresh,
   re-login, permission-loss, hostile title/URL, empty/error and keyboard tests.
5. Join the two-owner #197 composition and existing race/shuffle/browser/exact-head
   CI gates. Keep synthetic contract proof separate from useful real-model
   relevance, factual fidelity, freshness, latency and full-cost measurements.

Independent review starts from #196 and the accepted search scope. Reader-only
delivery, a pure parser, browser fixtures or accepted design alone do not close
the implementation task. Real model/search calls and activation remain under
their existing separately authorized gates.
