# Web search for the managed MVP

Version: 0.1 draft, reviewed sources 2026-10-10. Research owner:
[#94](https://gitcode.com/urandon/sessionless/issues/94); design consumers
[#179](https://gitcode.com/urandon/sessionless/issues/179),
[#180](https://gitcode.com/urandon/sessionless/issues/180), PR !160.

The owner's PR !160 request adds bounded general web search to the managed
minimum. Documentation lookup alone no longer meets the requested tool surface.
Search returns source evidence for the Sessionless-owned loop; it must not become
another agent, browser, session authority or automatic provider fallback.

Recommend a trusted provider-neutral search adapter, initially **Tavily Basic**,
alongside the already proposed constrained Docs MCP capability. This is an
engineering-fit proposal, not a measured quality winner or approved paid service.
Domain/time controls, explicit depth and normalized results make its first
contract straightforward to bound. Exa and Brave are strong alternatives;
Parallel and Perplexity also merit measured comparison. Do not choose only from
price or a vendor leaderboard. Exact terms, credential custody, substrate egress
and a separately authorized useful-query trial remain enablement gates.

## Provider comparison

These are current published contracts/quotes, not executed observations. Prices
are USD per 1,000 indicated requests, before Sessionless model/context, storage
and infrastructure costs. Quotes are not sealed price evidence or account terms.
No accounts, keys, purchases, production requests or benchmarks were run.

| Service and primary source | Search and retrieval distinction | Published price basis | MVP disposition |
| --- | --- | --- | --- |
| Tavily [Search](https://docs.tavily.com/documentation/api-reference/endpoint/search), [credits](https://docs.tavily.com/documentation/api-credits) | Domain/date controls, title/URL/content results; generated answer and raw content are optional; Extract is separate | Basic 1 credit, advanced 2; PAYG $0.008/credit, hence $8/$16 | Proposed Basic only, explicit options; no automatic depth/answer/raw-content expansion |
| Exa [Search](https://exa.ai/docs/reference/search), [pricing](https://exa.ai/pricing) | Domain/path/date filters; search types and content/synthesis options need sealing separately | Instant $4; Fast/Auto $7; Deep $12; Deep-Reasoning $15, up to 10 results; Contents $1/1,000 pages; extra results/summaries cost extra | Strong alternative, particularly MCP integration; no implicit Auto/deep/content upgrade |
| Brave [Search API](https://api-dashboard.search.brave.com/api-reference/web/search/post), [pricing](https://api-dashboard.search.brave.com/documentation/pricing) | Independent-index claim; plain URL/snippet search differs from LLM Context and generated Answers | Search $5; Answers has separate query/token billing | Strong plain-search alternative; explicit query/operator/filter and context-endpoint tests needed |
| Parallel [quickstart](https://docs.parallel.ai/search/search-quickstart), [pricing](https://docs.parallel.ai/getting-started/pricing) | Search objective/queries returns URLs/excerpts; Extract and Responses are different products | Fast/Turbo $1; Basic/Advanced $5 up to 10 results; additional results $1/1,000 | Strong low-latency candidate; ~700 ms Fast / p50 200 ms Turbo are vendor claims, not our measurements; use current /v1, not benchmark's assumed old surface |
| Perplexity [Search](https://docs.perplexity.ai/docs/search/quickstart), [pricing](https://docs.perplexity.ai/docs/getting-started/pricing) | Raw ranked results with domains/language/region/content controls, separate from Agent-generated answer | Search $5; Fast $1; successful POST request billing, even empty results; no search token surcharge stated | Comparable raw-results candidate; do not treat a generated Sonar/Agent answer as fetched evidence |
| Keenable [product](https://keenable.ai/), [NEEDLE](https://keenableai.github.io/needle/) | Advertises independent index, search_web_pages and fetch_page_content MCP; product claims are not an observed catalog | Advertised cloud tier $4; exact mode/account/free-credit terms unverified | Keep candidate; do not select from its own benchmark or treat anonymous/keyless access as unlimited/free |
| Bing through [SearchAPI](https://www.searchapi.io/docs/bing) | Aggregator returns organic title/link/snippet data; distinguish intermediary from Microsoft first-party product | Exact selected plan not established in this pass | Broader #94: support/provenance/legal/data terms before routing choice |
| Google through [Serper](https://serper.dev/) | Google SERP intermediary, not Google's own API or a full-page reader | Starter advertises $50 prepaid/50,000 credits ($1/1,000), six-month validity | Low sticker price does not establish licensing, region availability or source-content rights |
| [You Search](https://you.com/docs/guides/search) | Official search offering; exact returned schema/mode not frozen in this pass | Not established here | Broad comparison remains open; no invented price/latency/retention claim |
| [SearXNG API](https://docs.searxng.org/dev/search_api.html) | Self-hosted metasearch, configurable engines and JSON output; not a free independent index | Hosting/upstream usage/operations, no universal per-call price | Useful local/research alternative, not first serverless service: upstream availability, rate/legal constraints and maintenance remain ours |

Every shortlisted provider still needs profile-specific retention/training,
query logging, regional access, deletion/redistribution terms, authentication,
rate/error/billing and actual quality evidence. Public query classification does
not remove these requirements. Brave's [privacy notice](https://api-dashboard.search.brave.com/privacy-policy)
and enterprise ZDR offer are not permission to assume identical private-data
terms for all plans/providers. Unknown data/cost policy denies enablement.

## Agent and harness mechanisms

Local sources below use the already stored competitor snapshots, not claims
about latest releases. Links pin the inspected revision. Official documentation
is dated here but mutable; exact deployed binary/schema still needs capture.

| Agent surface and evidence class | Mechanism | Reuse or reject for Sessionless |
| --- | --- | --- |
| Codex official [configuration](https://learn.chatgpt.com/docs/config-file/config-basic); OpenAI [web tool](https://developers.openai.com/api/docs/guides/tools-web-search) | Codex cached/indexed/live/disabled modes; API search/open/find actions, URL citations and complete-source option | Preserve modes and provenance distinctions; cached content is still untrusted. API-specific native tooling is not the same thing as a portable client or our canonical tool ledger |
| Anthropic official [web search](https://platform.claude.com/docs/en/agents-and-tools/tool-use/web-search-tool) | Server tool with max_uses/domain controls and structured citations; tool errors may appear inside HTTP 200; $10/1,000 plus content tokens | Parse tool/domain errors, not only HTTP status. Claude Code internals are not established by this API contract; do not use the unofficial mirror as authority |
| OpenCode source [websearch.ts](https://github.com/anomalyco/opencode/blob/3a31c4ea801915c0b050df4b3842997ea62b6e93/packages/opencode/src/tool/websearch.ts) | Permission prompt; Exa or Parallel through MCP, 25-second call; provider override/flags, otherwise Session checksum selects a provider. Parallel args include native session/model metadata | Reuse narrow client/schema/time boundaries; reject implicit routing and sending native Sessionless identity/model metadata as routine query fields |
| Pi source [README](https://github.com/earendil-works/pi/blob/6c4f360264397c59801f6da2bdac13e3b1fcbe91/packages/coding-agent/README.md) | Enumerated built-ins are file/shell tools, no built-in MCP; extensions can supply capabilities | Minimal core plus explicit extension is a valid architecture; absence in this list is not proof that no Pi extension can search |
| Hermes source [web_tools.py](https://github.com/NousResearch/hermes-agent/blob/c80a0a551c7038517456ee0aeb60203ec92aedb6/tools/web_tools.py), [Tavily adapter](https://github.com/NousResearch/hermes-agent/blob/c80a0a551c7038517456ee0aeb60203ec92aedb6/plugins/web/tavily/provider.py) | Explicit backend remains the configured choice, but default-on one-shot keyless rescue can reroute a failed keyed/configured call; next call tries the chosen backend again. Unconfigured setup uses a credential ladder. Tavily normalizes paid results and disables raw/images | Reuse normalized results, not implicit rescue/discovery, provider rotation, query logging or raw exception strings |
| Zed official [tools](https://zed.dev/docs/ai/tools) | URL fetch produces Markdown; governed by tool permissions/profile/trust, not terminal sandbox network grants | Search and fetch are distinct; prove actual network boundary, not a terminal sandbox setting. No general native search conclusion from this bounded page inspection |
| Z Code official [product](https://zcode.z.ai/en) | Closed product; homepage task/search UI is not a public web-search API/schema/privacy contract | Record unknown, not inferred implementation. Z Code, Cursor, OpenHands and DeepSeek detailed search coverage remain broad #94 work |

## NEEDLE methodology and decision limits

Inspected existing clone
`needle@28fe4b2a80e78bb702616a63d8b7293359a7b0d4`,
[README](https://github.com/keenableai/needle/blob/28fe4b2a80e78bb702616a63d8b7293359a7b0d4/README.md)
and [client factory](https://github.com/keenableai/needle/blob/28fe4b2a80e78bb702616a63d8b7293359a7b0d4/src/needle/shared/search/factory.py).
The factory fixes distinct provider modes. README explains title/snippet judging,
nDCG@5, pooled oracle-normalized ultimate, and excluding failed search/judge
queries from the quality mean rather than scoring them zero. Unsupported query
operators can be dropped by adapters. These source observations make the chart
useful discovery evidence, not an availability-adjusted score, full-page reading
test or grounded final-answer quality guarantee. It is competitor-operated;
live API/mode changes and judge calibration remain additional limitations.

No reproduction or human-agreement audit was performed. #94 remains open for
its independent pinned reproduction and broader governance/provider coverage.
This MVP slice must not mark its full checklist complete.

## Proposed bounded evaluation

Before selecting an enabled profile, freeze 20 public queries: five technical
primary-source lookups, five dated official announcements, five multi-source
comparisons and five domain/locale/deep-tail cases. Compare Tavily Basic, Exa
Fast, Brave Search and Parallel Fast on identical query/time windows; Perplexity
Fast is a useful fifth candidate if explicitly authorized. Pin request mappings,
options, price revisions and response hashes. Do not silently drop unsupported
filters or failures. Paid requests and model-judge calls need a separately
approved finite execution manifest; this plan does not run them.

Report top-five source relevance, primary-source hit rate, date/domain compliance,
empty/error/timeout rate, p50/p95 latency, normalized context bytes and full
search-plus-model cost separately. Human-check all 20 gold requirements and the
final cited claims; distinguish snippets from full-page verification. Proposed
minimum gates: zero forbidden-domain/private-query dispatches, zero invented
source references, at least 18/20 successful bounded responses and 16/20 gold
requirements supported by retrieved evidence. Thresholds require owner
acceptance; a safety failure cannot be averaged away.

Deterministic adversarial fixtures cover malicious snippets/URLs, unknown dates,
redirect/private-address citation targets, oversized/malformed JSON, extra
provider charges, 200-with-domain-error, 429, cancellation, ambiguous send,
revocation and two-owner isolation. They prove our boundary, not vendor quality.

## Design handoff

PR !160 adds [bounded web search](../design/owned-harness-web-search.md) with
provider-neutral results, explicit public-data consent, source-linked canonical
output and cost/effect limits. Keep constrained MCP as a distinct requirement;
a REST tool cannot silently stand in for MCP conformance. Implementation children
follow accepted #180 design under #176. Full fetch/find/browser, provider-native
opaque search loops, adaptive fallback and private queries are not this MVP slice.
