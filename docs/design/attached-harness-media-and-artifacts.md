# Attached harness media and artifact delivery

Version: 0.1.1, 2026-10-12.
Owner: [#198](https://gitcode.com/urandon/sessionless/issues/198), under
[#72](https://gitcode.com/urandon/sessionless/issues/72).
Consumers: [#13](https://gitcode.com/urandon/sessionless/issues/13) and
[#35](https://gitcode.com/urandon/sessionless/issues/35).
Status: independently reviewed engineering design accepted under the agreed-MVP
delegation; implementation, profile registration and activation remain separate.
The Pi comparison below is historical research, not an active route proposal;
see the [retirement record](../development.md#retired-pi-experiment-record-historical-only).

The attached MVP route must use the contents of admitted files and images and
return useful, durable answers and artifacts. Extend the prepared Codex path
with an explicitly selected media profile rather than weakening the delivered
text-only adapter. The initial output cohort is provider-authored structured
text files serialized by Sessionless, not arbitrary workspace exports. Real
model capability, private-data policy and platform conformance gate activation.

This engineering design does not implement or enable behavior. The managed
route retains its separate loop/export implementation under #176/#191/#175;
neither route falls back to the other. Generated images, arbitrary binary
exports, shell access and a general writable workspace are not requirements of
this bounded profile. Image input remains required and is not substituted by
a filename, OCR, or a summary of an image.

## Research and candidate selection

The [source comparison](https://gitcode.com/urandon/sessionless/issues/198#note_193228300)
records pinned Codex, OpenCode and Pi source versions. These establish input
and event surfaces, not released artifact or model/platform conformance. Only
Codex is the current #133 attached activation candidate; the Pi row is retained
as historical competitor research, not as a selectable or fallback route.

| Candidate | Relevant surface | Decision for this profile |
| --- | --- | --- |
| [Codex f5420174](https://github.com/openai/codex/tree/f5420174dafba153913a3e697f89002c338dfd7e) | CLI images become LocalImage; final AgentMessage is text; output-schema shapes a structured final. Local image read failure produces a placeholder. | Extend the existing prepared integration, conditionally on exact released artifact/schema/image/tool conformance. Validate usable images before launch; a placeholder is not successful media consumption. |
| [OpenCode 3a31c4ea](https://github.com/anomalyco/opencode/tree/3a31c4ea801915c0b050df4b3842997ea62b6e93) | File parts and image/PDF reads; write tool creates files. Native export contains session/messages. | Retain as an alternative, not a silent replacement for #133. A new prepared adapter, confinement and authentic export composition would be necessary. |
| [Pi 6c4f3602](https://github.com/earendil-works/pi/tree/6c4f360264397c59801f6da2bdac13e3b1fcbe91) | RPC has typed images; CLI files become text/images. Native transcript exports and filesystem writes are distinct. Retry defaults enabled. | Historical research only. Retired by #213; not an active alternative, selectable route, or fallback. |

Use the smallest Codex extension that needs no model-selected filesystem write:
read-only native image input, bounded text-file material, and an explicitly
typed final answer plus artifact contents. A trusted serializer creates the
downloadable files from those exact semantic fields. This supports requested
JSON/text deliverables; it does not certify native arbitrary file generation.
Failure of this option's exact conformance produces a no-go and a separately
reviewed alternative, not permission to enable general tools.

## Versioned profile and authority

Proposed immutable profile identifier: `CODEX-EXEC-MEDIA-2026-10-V1`.
Pin its schema, admitted formats, numeric limits, model/context evidence,
official executable digest/provenance, configuration/argv, native image
preparation mode and limits, isolation and egress policy in the existing
resource/harness binding. No mutable per-run model,
endpoint, schema, tool, path or quota override is allowed.

The closed #81 text-only profile and grammar remain unchanged. Media requests
require explicit registration of this new profile and capability digest; they
must not widen an existing text binding or automatically select a new profile
based on an attachment. All constructor, sealed-input, prepared-driver and
receipt consumers must agree on the same selected version before dispatch.
Unsupported or unknown versions fail before credential Issue. No aliases,
legacy-name fallback or silently accepted unknown event variants are added.

The original Sessionless Session, Run, Attempt, lease/fence, reservation and
current authenticated attached Session remain the only execution authority.
The same prepared outer runner owns Issue, Materialize, the actual supervised
process, WriteBack and Release. Media preparation does not attach, claim,
create another runner, resume a native thread or extend credential lifetime.
Maintain the original current-head and deletion/revocation checks at each
existing admission/effect/finalization boundary.

## Admitted input and useful semantics

The first cohort admits UTF-8 `text/plain` and `application/json` files plus
native PNG/JPEG images. PDF, DOCX, archives, SVG, animated media, audio and other
formats are explicitly unsupported here. Web upload capabilities may remain
broader; upload success is not admission to this execution profile. Report the
unsupported input before Issue and do not remove it from the context.

For every canonical attachment in the admitted context window, retain its
event/sequence, name, declared media type and complete immutable BlobRef.
Verify exact size/hash/body and original ordered association from authenticated
canonical projection, including snapshot-contained and historical attachments.
Same filenames in different events are different inputs. Never consume just
the newest trigger while dropping historical attachments. Oversized or
unsupported history requires an explicit new session/profile decision, not
silent truncation or a harness-owned history fork.

Validate UTF-8 and reject NUL-containing text. Validate JSON syntax without
re-marshalling or changing numeric tokens; pass the original bytes as bounded
quoted task data in the canonical prompt, with its source association. Text
content and names remain untrusted data, not policy or tool instructions.
Sessionless performs no extraction, OCR, resizing or lossy conversion. The
original canonical and staged image bytes stay unchanged; the native Codex
representation sent to the model is a separate, explicitly bounded derivation.

Check actual PNG/JPEG signatures, dimensions and supported nonanimated form
with a bounded decoder before launch, not only supplied MIME/extension. Pass
the exact verified original bytes through private native image files and pinned
CLI image arguments. A missing/unreadable/corrupt image fails before Issue; verify
this using the actual released executable because upstream may emit a textual
placeholder instead of failing. Image-content-dependent tests must distinguish
real visual consumption from merely transmitting a path.

### Native image preparation

Pinned Codex CLI supplies `LocalImage` with unspecified detail. Its core may
choose different preparation modes from model metadata and managed features;
even `Original` detail is not a promise of unchanged provider bytes. The
[preparation implementation](https://github.com/openai/codex/blob/f5420174dafba153913a3e697f89002c338dfd7e/codex-rs/core/src/image_preparation.rs)
and [image codec](https://github.com/openai/codex/blob/f5420174dafba153913a3e697f89002c338dfd7e/codex-rs/utils/image/src/lib.rs)
show resizing and PNG/JPEG re-encoding when output dimensions change.

This profile selects `DetailBased` with effective `High` detail, maximum
dimension 2048 and 2500 patches of 32 by 32 pixels. Pin the exact model metadata
and release configuration that disables `UnifiedImageBudget`; do not leave the
mode to an ambient feature default. An artifact that cannot enforce this exact
mode and its limits is ineligible. A different mode, codec or limit needs a new
reviewed profile revision, not a per-run override.

Allow only the pinned native proportional resize and PNG/JPEG encoding on the
admitted original images. This may change bytes, metadata and JPEG fidelity;
do not claim lossless processing or byte-identical provider input. Keep the
unchanged original BlobRef/digest and ordered event/ordinal association distinct
from the prepared dimensions, encoding and digest. The association must reach
the model-visible image order and trusted request conformance evidence, not
only an unrelated private list. A native placeholder is never a successful
derivation. Unknown preparation or a provider request outside the exact profile
denies before a physical provider send; an unenforceable request envelope makes
the profile ineligible rather than weakening its bounds.

The original cohort is not reduced to avoid transformation. A 3000 by 1000 PNG
within the original compressed/pixel ceilings exercises the resize path; a
small PNG/JPEG within both dimension and patch limits exercises the unchanged
encoded-byte path. Conformance must check both against the exact released
artifact, source association, actual prepared request and content-dependent
visual task. Test orientation/color-sensitive cases and explicitly show any
loss of task-relevant detail. Source inspection determines this design, not a
claim that those artifact/model experiments have already passed.

Byte and pixel limits do not prove model context fit. The admitted exact model
must have a conservative validated bound for text and prepared-image context
usage, including native resize notices/metadata, and the required quota/resource
authority. Unknown fit or authority denies before
the provider effect. Do not treat a file's byte length as a token estimate or
claim private-data eligibility from a public-text/free-model smoke.

## Finite staging and process policy

Reuse private attempt-scoped preparation and verified canonical material.
Generate ordinal filenames independently of user names; associate them back
to the original ordered refs in private memory. Expose only the read-only
history/image set inside the isolated process. Staging uses exclusive,
no-follow regular files, private directory permissions and ownership/inode
checks; it never follows stdout paths, symlinks or host HOME/config locations.
Close/clear/remove only the actual owned objects, including failure paths.

The model receives no credential bytes, upload capabilities or storage keys.
Only the existing owner-local credential boundary delivers auth.json to the
existing supervised process. Native media flags and a fixed output schema are
part of the reviewed argv/config digest, not arbitrary user arguments.

Model tools, filesystem mutation, shell/command execution, MCP, search and
dynamic discovery are excluded in this profile. Pinned source has a shell
feature gate, but disabling shell alone does not prove every tool is removed.
Before registration, enumerate the exact release's complete tool set and
configuration and prove the allowed set at the actual effect sinks. Retain OS
read-only confinement regardless of advertised tools; unexpected event/effect
frames fail rather than being ignored. If the artifact cannot enforce the
closed profile, it is ineligible; writable workspaces are not a workaround.
Network access remains limited to separately reviewed provider egress. Do not
flip the current NetworkDenied flag without an explicit actual-platform profile.

## Provider authored artifacts and producer evidence

Define a fixed strict structured final schema with an answer and at most two
artifact entries, each containing a validated name, declared `text/plain` or
`application/json` type, and UTF-8 content. Unknown/duplicate fields, invalid
names, case-fold name collisions, invalid UTF-8/JSON, excess entries or limits
reject the entire final result. An absent artifact list means an answer-only
turn, not automatically generated summary.txt. A task requesting a supported
file deliverable must actually produce the requested meaningful artifact to
pass the product usefulness check.

Only the private same-run completed protocol result may create this typed
artifact carrier. Bind it to the actual supervisor observation, original
invocation identity, profile/schema digest and digest of the original final
bytes. The trusted serializer copies each declared content byte-for-byte;
it must not invent artifact content from Summary or from input material.
Keep answer bytes, artifact bytes, full structured-final integrity and process
observation digests distinct. Caller-populated structs or cleanup booleans
cannot mint producer evidence.

This is provider-authored structured content with Sessionless serialization,
not a native workspace export. No directory scan or filesystem harvest exists
in this version, and no stdout filename grants read access. It prevents the
coordinator from accidentally publishing auth/config/input copies as arbitrary
files. A malicious owner-controlled host can still lie about observations;
neither hashes nor daemon attestations cryptographically prove provider
authorship against that host. Preserve the existing user-managed-host threat
boundary and do not transfer its assertions into managed-cloud assurance.

## Upload custody and canonical publication

An attached export needs a worker-output upload route, not browser upload
credentials or unrestricted BlobStore access. Add a server-owned bounded
attempt-output capability under existing transport authentication. The server
binds owner/tenant/worker/connection, Run/Attempt/fence, selected profile,
current accepted head, private producer result identity, exact output ordinal,
declared size/hash and finite expiry. It chooses the immutable candidate key
and fixed destination; the model cannot choose bucket, key or endpoint.

Keep capabilities private in the trusted coordinator, outside the child
environment/stdin, public receipts and logs. Validate full serialized upload
requests and enforce count/body/time limits on client and server. Admission
rechecks current attempt authority; capability creation alone does not authorize
another model call. This is new transport composition requiring its own
versioned contract and tests, not a claim that existing Web upload intents
already authorize worker outputs.

Freeze artifact bytes after actual process quiescence. Perform upload only
after original WriteBack/Release and staging cleanup evidence has succeeded;
the coordinator retains finite private semantic bytes, not provider credentials.
An earlier lease, run or caller deadline bounds the entire export phase.
Success creates only verified attempt-scoped source objects, not product events.
Retain owned bytes and immutable candidate identity through the original
receipt/ACK journal lifecycle, then clear them.

Reuse `attachedworkeroutput.Materialize`: server rereads bounded exact source
bytes, verifies full scope/size/hash, copies into server-owned canonical keys
and generates manifest/event identifiers. READY receipt and existing fenced
canonical finalizer/TerminalAck remain the sole product publication. Reuse the
accepted attached receipt material contract and its explicit implementation
status; no metadata-only hint becomes a prepared canonical proof.

## Failure and reconciliation

Preflight failures have zero Issue, launch or upload effects. Known failed or
cancelled turns publish no success artifacts. Process/credential/staging
cleanup failure cannot be converted into a successful export. Changed
membership, lease/fence, profile, deletion state or resource authority denies
new upload/publication without rerunning the completed model.

Unknown upload acceptance or lost response retains the existing ambiguity and
deletion barrier. Reconcile the exact capability/key/size/hash when permitted;
do not blindly repeat Put, overwrite an object, regenerate an artifact or
execute the model again. No elapsed timeout proves remote quiescence. Any
additional upload capability or retention ledger shape must be explicitly
integrated with existing deletion/receipt ownership before registration.

READY receipt loss/ACK loss reuses the original immutable receipt identity and
journal; fresh authenticated delivery performs receipt-only reconciliation.
Retire semantic bytes only after exact ACK and journal retirement or an
existing authorized definitive disposition. Stale/foreign/divergent candidates
cannot create a second terminal. Automatic crash-time artifact regeneration
is not a new MVP requirement.

## Proposed profile ceilings

These initial engineering ceilings require maximum-case conformance before
profile registration. Every limit is the minimum of this profile, the original admitted
job/product limits, the actual transport limit and the earlier authority.
Never raise inherited limits or truncate material to make a case fit.

| Resource | Initial ceiling |
| --- | --- |
| Attached input files | Two text/JSON files, 128 KiB each, 256 KiB total. |
| Native image input | Two PNG/JPEG images, 1 MiB compressed each, 2 MiB total; 4 million pixels each and at most 32 MiB decoded RGBA total. |
| Native preparation | Fixed DetailBased/High: 2048 maximum dimension and 2500 32-by-32 patches per prepared image. Original decoded and resampled buffers each have a 32 MiB aggregate ceiling; count overlapping copies, codec scratch and native caches against the separately pinned process memory ceiling. |
| Prepared image encoding | At most 2 MiB per PNG/JPEG representation, 4 MiB aggregate; base64 is at most 4 times ceil(encoded bytes / 3), plus data-URL/JSON/metadata framing. The complete provider request must fit its separately pinned wire ceiling and all inherited limits before send; otherwise deny, never truncate or raise a limit. |
| Total canonical input/context | Original MaxInputBytes, MaxContextBytes, event/attachment bounds and exact wire ceilings still apply, including history, metadata and framing. |
| Model final | 128 KiB encoded structured JSON; answer at most 32 KiB UTF-8 and existing 32000-rune ceiling; at most two artifacts of 32 KiB each, 64 KiB total. |
| Protocol | Preserve existing 1 MiB/frame, 16 MiB stdout, 4096 events, depth32 and 256 KiB raw-final maxima; narrower profile/wire checks apply before decode/copy. |
| Upload/export | Two actual body sends at most, 64 KiB aggregate artifact bytes; 15 seconds or earlier remaining run/lease/caller deadline across authorization, upload and reconciliation. No auto-retry or deadline reset. |
| Output materializer | Existing min(MaxInputBytes,64 MiB) byte ceiling and MaxArtifacts; this profile only narrows them. Summary/metadata/manifest/receipt wire bytes count separately. |

Count failed or ambiguous authorized operations and every allocation/copy,
expanded original/resampled image buffer, codec scratch/cache, encoded image,
data-URL copy, native resize notice, encoded frame and upload request. JSON
escaping and base64 framing consume real wire bytes; original compressed size
does not bound a re-encoded representation or decoded memory. The native codec
decodes before resizing, so a small prepared image does not bound the original
allocation. Enforce limits before allocation/sensitive sinks, with an excess
detection byte where appropriate. Maximum valid cases must fit with race
instrumentation and the declared platform memory/time/disk envelope.

## Retrieval and verification

WebUI reads the canonical answer/manifest after the original terminal commit.
Downloads use current participant authorization and exact short-lived object
capabilities. Treat text and JSON exports as downloads with safe filenames,
Content-Disposition attachment and nosniff; do not inline untrusted active
content or store presigned URLs as the durable artifact identity. Refresh and
re-login must return the same bytes; cross-owner/revoked-member access denies.

Deterministic tests cover snapshot/tail equivalence; historical and same-name
attachments; MIME/signature/UTF-8/JSON/oversize inputs; image read failure;
foreign/stale scope; path/symlink/extra bodies; structured-final duplicates,
schema drift and useful artifact association; false producer carriers;
unchanged versus resized/re-encoded native images, preparation-feature drift,
original/prepared source-order mismatches, orientation/color/detail loss;
cleanup/cancel/revoke/expiry; upload ambiguity and late Put/deletion; READY and
ACK loss with zero provider resend; output read/hash/copy corruption; maximum
wire/expanded-memory/count/time budgets; refresh and permission loss.

Use causal red/green controls, injected clocks/barriers, race/shuffle/stress and
the normal two-owner Web-to-attached-to-receipt path. Fake process/YDB tests
prove only their stated composition. Separate exact released artifact/image
usefulness, actual-platform confinement/egress and bounded real-profile/browser
evidence from deterministic implementation acceptance. Per-resource activation
remains reversible and defaults off under #133/#129; cloud/live/credential
authority and its budget require a separately approved finite manifest.

## Implementation handoff

After independent original-requirements-first review and primary acceptance,
create linked bounded children under existing #72, not another epic:

1. Versioned capability/profile and prepared canonical media input: exact
   history/source association, pinned native preparation/request bounds,
   context-fit evidence and finite native staging.
2. Strict typed final/producer carrier and bounded worker-output upload custody:
   actual authenticated transport, immutable candidate and ambiguity/deletion
   handling through existing authorities.
3. Normal joined receipt/download conformance, exact released artifact/tool
   profile proof and bounded activation/browser evidence under #133/#35.

Each child has explicit dependencies, labels/milestone, finite negative/positive
tests and original lifecycle/canonical acceptance. Start each implementation
child only after its applicable predecessor contracts are delivered; design
acceptance does not tick media or release checklists.
No deployment, secrets, paid/provider/tool calls, shared migration, destructive
cleanup, network/Shadowrocket mutation or MVP scope reduction is authorized.
