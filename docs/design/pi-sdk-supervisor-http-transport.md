# Pi SDK supervisor HTTP transport draft

Version: 0.1.3. Engineering proposal under
[#202](https://gitcode.com/urandon/sessionless/issues/202) and
[#203](https://gitcode.com/urandon/sessionless/issues/203).

The proposed transport keeps Pi's genuine loop, standard Responses provider,
SDK construction and parser, while the original Go supervisor owns the sole
provider connection. The child retains network denial. Secret-free supervisor
IPC and the original file-only credential lifecycle remain requirements.

The accepted 0.1.3 [Pi profile](pi-sdk-attached-media-profile.md) remains the
governing contract. Its 0.1.4 SPU2 section is a separate transport draft; this
proposal must receive independent requirements-led acceptance before replacing
that section or authorizing implementation. It enables no profile, provider
call, credential access, platform change or rollout.

The fixed HTTP receive subsection permits separate source acceptance of its
private zero-effect framing parser. That parser produces unapproved DATA;
it cannot authorize a connection or forward a response. Executable transport
acceptance still requires the complete sink, semantic guard and fit contracts.

## Concrete readiness findings

| Boundary | Source evidence | Engineering consequence |
| --- | --- | --- |
| Child network | [Supervisor](../../internal/attachedworkerdaemon/supervisor.go) requires `NetworkDenied`; [OCI launcher](../../internal/attachedworkeroci/launcher.go) uses and verifies `--network none`. | The unmodified launcher does not provide provider egress. Do not flip its assertion or select host networking. |
| Actual fetch | [Entry](../../internal/pisdkmedia/native/entry.mjs) captures child `globalThis.fetch`; [bootstrap](../../internal/pisdkmedia/native/bootstrap.mjs) passes the guarded fetch to ModelRuntime; [physical boundary](../../internal/pisdktransport/request.mjs) eventually invokes it. | A successful phase reply cannot repair the network mismatch. A concrete transport adaptation is required. |
| Native tuple | The opt-in [functional fixture](../../internal/pisdkmedia/prepared_execution_functional_test.go) selects a Darwin Node executable; OCI preflight requires a Linux engine and image and mounts the selected executable. | That unchanged local tuple is not a Linux OCI runtime. Qualify an actual Linux Node, SDK and image tuple, not the Darwin test's digest. |
| Credential ownership | [Native reader](../../internal/pisdkmedia/native/credential_file.mjs) requires a regular, single-link, mode0600 file owned by child `getuid()`. OCI uses an explicit nonroot UID. | Prove the selected UID mapping and immutable read-only file mount. A mismatch is conditional, not proof all Linux mappings fail. The subscription `auth.json` writeback bridge is not an API-file substitute. |
| Qualification | [Profile validation](../../internal/domain/attached_pi_sdk_profile.go) checks structural commitments; [original owner](../../internal/attachedworkersealedinput/pi_prepared_owner.go) still returns `ErrMissingProducer`. | Declared digests, context sizes, synthetic budgets and fake transport success do not qualify an installed runtime or full model fit. |

Docker documents that the [none network driver](https://docs.docker.com/engine/network/drivers/none/)
creates only loopback. Its [foreground attachment contract](https://docs.docker.com/engine/containers/run/#foreground-and-background)
provides standard streams; it does not make an outer client's extra descriptors
into inner workload descriptors. The existing duplex supervisor channel is the
portable candidate across local Linux and a Darwin-hosted Linux VM.

## Selected design candidate

Use one fixed byte transport inside the SAME Go supervisor and original
invocation. It is not a model client, listening proxy, sidecar, second runner,
second session or separate credential lifecycle. It neither constructs model
messages nor interprets a successful model answer. The real Pi provider still
constructs the request and the real Pi parser still produces the result.

The transport receives the complete actual serialized SDK body and canonical
nonsecret headers through a new closed protocol. The parent independently
compares them with its pre-Issue projection. An opaque capability belonging to
the SAME immutable file owner verifies the SDK's authorization commitment and
writes the exact Bearer header only into that invocation's fixed TLS sink.
Neither raw Authorization nor key bytes cross the process channel.

Rejected alternatives are removing network denial, injecting an ambient host
fetch/client/proxy, changing file delivery to a direct/environment credential,
and carrying Authorization in child stdout. Discarded stdout alone does not
preserve a secret-free IPC contract. A generic host Unix socket is not selected:
its Darwin-to-VM behavior and ownership would introduce another platform gate.

## Original credential custody

Prepare a bounded, single-use transport plan and a fresh 32-byte invocation
nonce before Issue on the original ledger. The nonce is comparison data, not
permission. Failure to obtain it denies preparation. The existing Issue and
Materialize occur once; the genuine native reader still reads the original
immutable file and the genuine ModelRuntime still resolves its literal key.

At the final SDK fetch hook, validate the complete actual method, endpoint,
headers and body as in the accepted profile. Construct an HMAC-SHA256 commitment
using the exact actual Bearer key and a domain-separated, length-delimited
transcript containing the nonce, original harness-binding digest, backend
profile digest, exact request digest and canonical nonsecret-header digest.
The transcript encoding and cross-language vectors must be fixed before code;
string concatenation or a caller-chosen digest format is not a substitute.

### Authorization transcript

Let `u32(x)` be exactly four unsigned big-endian bytes and `bytes(x)` the
specified byte sequence, never a JSON string or hexadecimal text. The domain
prefix is ASCII `Sessionless/Pi/SPU3/Auth/1` followed by one zero byte. Append
these seven fields in order, each as `u32(length) || bytes`:

1. The 32-byte pre-Issue random invocation nonce.
2. The 32 decoded bytes of the original harness-binding SHA-256 digest.
3. The 32 decoded bytes of the backend-profile SHA-256 digest P.
4. The four ASCII bytes `POST`.
5. The exact UTF-8 sealed endpoint, without normalization or a trailing zero.
6. The 32-byte SHA-256 digest of the canonical nonsecret header block below.
7. The 32-byte SHA-256 digest of the complete actual SDK request body.

The MAC is the raw 32-byte HMAC-SHA256 of that transcript. Its key is the exact
visible-ASCII token after the single literal `Bearer ` prefix at the actual
SDK hook, without trimming, case conversion or another decoding step. The
parent computes it from its original active materialization's token and uses
constant-time equality. An attempted comparison consumes the plan even on
wrong scope, malformed data or mismatch; it is not a repeatable key oracle.
Transcript and digest buffers are prospectively funded before Issue. No MAC
result substitutes for actual body/header equality or original physical-send
authorization.

A canonical header block starts with a big-endian uint16 pair count. Each pair
is `u16(name length) || lowercase ASCII name || u32(value length) || value`.
Names are in ascending ASCII order, unique, and have no zero-length value.
Values are exact printable ASCII from the SDK's validated header snapshot;
internal spaces are allowed (including the SDK user-agent), but leading or
trailing whitespace, CR/LF, other controls and non-ASCII bytes deny. All
lengths count bytes.
The request block contains exactly the eleven existing allowed SDK headers
other than `authorization`, with the same pinned values from the original
request projection. It includes no `host`, content-length, connection or URL
override. The fixed parent HTTP implementation supplies its own sealed Host,
exact body content-length and connection-close framing. Response headers use
the same encoding but a separate admitted response-header policy; they are
never reflected from the request block.

The synthetic transcript vector uses nonce bytes `00..1f`, a binding of 32
`11` bytes, P of 32 `22` bytes, endpoint
`https://api.example.invalid/v1/responses`, body `{"fixture":true}` and key
`fixture-only-api-key`. Header values, in canonical name order, are:

```text
accept: application/json
content-type: application/json
user-agent: fixture/1
x-stainless-arch: arm64
x-stainless-lang: js
x-stainless-os: Linux
x-stainless-package-version: 6.40.0
x-stainless-retry-count: 0
x-stainless-runtime: node
x-stainless-runtime-version: v24.19.0
x-stainless-timeout: 30
```

The header block is 331 bytes; the domain prefix is 27 bytes including its
zero terminator; the transcript is 259 bytes. Expected values are:

```text
headers SHA256  4eb66216ea83097b10b2fd2ad62e11739725fdb164324c97fd8a7fcd89cd04b8
body SHA256     ffbc2dfc402782325da71132100e74ff511d1585dd80e4ea196ed4bcace3fef2
transcript SHA  32c4a36582a2d76aa57cb01339c6834ff63a5a41e534eb7d2d59300b6b1844ba
HMAC SHA256     39dea11e34ea383bc5a3299aeba6577bc397e2737d0792df93ad0f67912a0e04
```

These are byte-encoding vectors, not an admitted SDK request or an enabled
endpoint. The eventual Go and pinned Node implementations must independently
reproduce them and reject one-bit changes in every field, the key, field order,
length encoding and header order. Equality of one synthetic vector alone does
not establish either runtime's conformance.

The file owner's proposed private capability must verify that commitment
against its original active Materialize record, exact handle/resource/generation,
immutable descriptor and fingerprint. It must not read a secret backend again,
return plaintext, acquire another handle, reenter the callback's state mutex,
accept an arbitrary writer, or outlive the genuine callback. Existing
`VerifyMaterialized` does not already provide this operation.

Only the fixed transport's concrete sink, bound to the SAME profile and
original request, may consume that capability once. Its TLS trust, endpoint
and request identity must be verified by construction, not by a caller's
Boolean or claimed connection metadata. The capability emits the Bearer header
to that sink without exposing it as return data. This is an explicit proposed
extension of the original file owner's internal consumer contract; it is not
permission to reinterpret `file` as the public `direct` delivery arm.

### Package and lifetime ownership

Keep the fixed HTTP, HMAC and quarantine implementation private inside
`providercredential`, next to `immutableAPIFileUse`. The original synchronous
Materialize already owns a verified secret slice until callback cleanup joins
and the existing `zero(secret)` runs. A new private field may borrow that same
slice during the callback; no backend reread, additional plaintext file API,
key-returning method or file-callback plaintext is introduced. Verify the same
immutable file and fingerprint before comparing the SDK commitment.

The new file consumer binds its pre-Issue transport recipe to the SAME
`PreparedImmutableAPIFileV1` pointer, context, frozen binding, body/profile and
ledger. After Issue it accepts only the actual acquired handle and original
Materialize record. The recipe has no arbitrary URL, writer, fetch function,
secret parameter or caller-supplied successful response. It exposes only a
bounded approved response view after the guard, borrowed until replay joins.
Retain the exact original Materialize context separately from the derived
cancellable consumer context: the former is compared to the send's owner
context, while the latter bounds and cancels borrowed IO.

Do not replace the daemon's private physical-send owner with a structural
`Authorize` interface or caller callback. Add a daemon-owned binder that type
asserts the actual private `*preparedAPIPhysicalSend` and returns an opaque
borrow with unexported fields and no public constructor. The original runner
seals identity and ledger at creation, the validated handle once after Issue,
and the actual materialization once before executor entry. The binder checks
those exact values, original outer use context, complete body and profile.
Provider passes values from its private active file-use record, not a wire DTO.

Bind before any blocking Source call. On shutdown the borrow can nonblockingly
revoke the SAME original send before joining; only its one subsequent consume
may call the existing physical-send Source authorizer. The SPU3 pump must not
also call Authorize as SPU2 does. A failed HMAC/file/request comparison burns
and revokes the attempt with no connection; no wrapper can reset the original
send, mint F or create source eligibility. All original Source verification
remains in the runner's existing closure.

This introduces the production import direction `providercredential` to
`attachedworkerdaemon`, not the reverse; the daemon's present transitive
production dependency closure does not reach providercredential. Relocate only
the existing genuine-service receiver integration test and its exclusive
helpers from the daemon's internal test package to an external integration
test, preserving the actual receiver assertion through a test-only bridge if
needed. Do not weaken the test or move the Source owner to conceal a cycle.

Use one nonqueueing file-use gate. No state-mutex reentry occurs inside its
Materialize callback. One attempted verification spends send authority; the
same borrowed record remains alive only for the bounded response guard and
cleanup. Callback retirement marks it inactive, cancels its derived context,
revokes the original send, joins its transport/control borrowers, drops its
secret alias, and only then lets the existing secret zeroing and state unlock
proceed. Cancellation must reach a held Source RPC as well as socket and pipe
IO; waiting for the gate before revoking would deadlock retirement. Finalize
and Release remain after callback unlock, never nested inside the consumer.

Before any response bytes enter parent-to-child IPC, quarantine the complete
bounded response, including headers, until valid terminal data and actual HTTP
EOF/join. Check UTF-8/JSON/SSE structure and effects, decoded strings, and the
semantic delta streams that Pi would concatenate, against the same credential
without returning the key. A key split across two valid `response.output_text.delta`
events must reject the entire response before either frame is forwarded. Raw
chunk matching, per-frame matching and a final child-side check are insufficient.

Reserve the entire quarantine, parser offsets, decoded strings, semantic joins,
matching work and subsequent IPC copies on the SAME ledger before Issue.
Neither incremental buffer growth nor a fresh budget after receiving a frame
is allowed. Inherited total-response and per-frame limits remain binding;
overflow, unknown semantic assembly, malformed terminal data or missing EOF
rejects without forwarding. The closing design must specify the exact admitted
semantic streams, escape handling and cumulative bounds and prove they fit;
it must not silently truncate the response or narrow the original input cohorts.

Only after the complete guard succeeds may the parent replay the original
bytes unchanged in fixed-size transport chunks. Pull credit controls this
replay, not upstream reading.
This candidate explicitly replaces live one-credit streaming with a bounded
whole-response quarantine; it does not promise time-to-first-token streaming.
The native guard remains in place, and parity and split/escaped cross-event
tests must establish the shared rejection contract. Neither guard replaces
Pi's Responses parser.

### Actual Pi semantic assemblies

The secret guard follows the pinned
`packages/ai/src/api/openai-responses-shared.ts` `processResponsesStream`
behavior, not a guessed grouping by `content_index` or `item_id`:

- `output_item.added` creates a new empty text or thinking block and associates
  it with `output_index`. Replacement leaves the previous output block in the
  output list; the guard must not discard that block's confidentiality state.
- Both `output_text.delta` and `refusal.delta` append to the same text slot.
  Both `reasoning_summary_text.delta` and `reasoning_text.delta` append to the
  same thinking slot. A matching slot is looked up by output_index and type.
- `reasoning_summary_part.done` appends the literal two newline characters to
  the thinking slot. The guard checks the result after every append, even if
  a later event would overwrite it.
- `output_item.done` uses the existing slot, or creates one when absent. For a
  reasoning slot it replaces thinking with `summary.text.join("\n\n")`, or
  `content.text.join("\n\n")`, or its existing text, using Pi's nonempty
  fallback order. For a message slot it replaces text with all output_text and
  refusal content joined with the empty separator. It then removes the slot
  mapping, not the output block. Later reuse of the index creates another block.
- Standalone text/refusal/reasoning `*.done` events do not replace a Pi slot.
  Their decoded strings still receive scalar checks. Reasoning signatures,
  terminal backfill and all nested terminal output strings receive scalar
  checks too. Tool and unknown item/event variants remain denied by policy.

Scan the entire raw quarantined body and admitted header names/values for the
literal credential before replay, including SSE comments, field names and
matches across raw chunk/frame boundaries. Also scan every decoded JSON
key/string, including JSON escape decoding. Neither matching domain replaces
the other: the native policy ignores comment lines, which may still carry raw
secret bytes. Check each intermediate
semantic block above. Bootstrap admits exactly one final text block; it must
not invent a join across distinct blocks to turn a rejected result into a final.
Keep the native final check as
defense in depth. The parent shadow is only a reject/allow predicate: no
shadow-produced message, final or synthesized SSE event enters Pi. Bounded
parity tests against the exact pinned Pi source must cover mixed text/refusal,
mixed summary/reasoning, done-only content, overwrite, index reuse and escapes.
Changed Pi assembly semantics require a new qualified artifact, not permissive
unknown-event handling.

The guard can check appends with a prospectively allocated failure-function
matcher and one state per retained output block, rather than repeatedly
copying the accumulated text. Reset and feed exact done-event replacement
segments and separators in order. Preserve states of overwritten blocks while
they remain in Pi's output list; no matching state is a profile grant. The
exact arena, slot lookup, parser-node and work bounds still require their own
implementation-derived prospective schedule.

## Closed duplex protocol

Use a new canonical private magic `SPU3`, never an optional SPU2 alias. Its
17-byte header retains kind, uint32 metadata length and uint64 body length.
Validate header, order and inherited MIN caps before reading declared regions.
Old profiles and artifacts must not be silently reinterpreted: exact transport,
bootstrap and native artifact commitments select the new grammar. Historical
reconciliation is not permission to execute an old transport.

| Kind | Frame and direction | Exact metadata and body |
| --- | --- | --- |
| 1 | Initialize, parent to child | 96 metadata bytes: nonce32, original binding digest32, P32. Body is the complete original bounded Init with the original file locator, once. |
| 2 | Fetch request, child to parent | `P32 || MAC32 || u32(Hq) || request-header-block`. Metadata length is exactly `68 + Hq`; body is the complete actual SDK request, once. No URL override or raw Authorization. |
| 3 | Response start, parent to child | 46 metadata bytes: `u16(200) || u32(N) || u64(R) || response-digest32`. Body is the admitted response-header block. Only after full quarantine and EOF/join. |
| 4 | Response part, parent to child | Four metadata bytes `u32(index)`, zero-based. Body is one original byte chunk from approved quarantine, unchanged; exactly C bytes except the positive final remainder. |
| 5 | Pull, child to parent | Four metadata bytes `u32(next index)` and zero body. One credit for that next approved part, never upstream reading. |
| 6 | Response end, parent to child | 44 metadata bytes: `u32(N) || u64(R) || response-digest32`; zero body. |
| 7 | Structured final, child to parent | Zero metadata; strict provider-authored original final body after Pi parser/loop and verified Response end. |
| 8 | Retire, parent to child | Zero metadata and body; at most once, revokes and joins; never grants use or cleanup permission. |

All integers are unsigned big-endian; digest and MAC fields contain raw bytes,
not hex. `Hq` is the exact request-header-block size, `N` the positive number
of replay chunks, and `R` their total raw byte count. Before replay, the full
body must contain complete admitted SSE frames with bounded comments and no
partial frame or bytes after the sole terminal frame. Header parsing must reject unknown kind,
wrong direction, unexpected metadata length or inconsistent nested Hq before
body assembly. There is no wire error text, use allowance or retry frame.
The response digest is SHA-256 of exactly the concatenation of Response part
bodies: HTTP entity bytes after transfer-framing removal, with identity content
encoding. It excludes response headers, HTTP status/framing, and SPU3 headers
and metadata. Parent and child hash the same byte sequence without reencoding.

After Initialize the sole Fetch request is the only legal child frame. The
parent validates the complete request before attempting physical authorization.
During quarantine no response frame is emitted and no Pull is legal. After
Response start, child sends Pull(0), receives part(0), then repeats in strict
order. At most one Pull and one corresponding part are in flight; speculative,
duplicate or skipped credit burns the attempt. After part(N-1), parent emits
Response end without another Pull. Child validates count, raw digest and total
before closing the reconstructed response stream. Structured final is legal
only after that end and genuine Pi completion. Completion also requires actual
child EOF, successful process quiescence and original lifecycle retirement.
Early final, premature EOF, extra stdout, malformed/truncated IO, cancellation
and any comparison failure close the invocation without resynchronization.
Retire is permitted from any nonretired state and invalidates remaining work.

Select chunk capacity C before Issue as the minimum of the original
ResponseChunkBytes and process-frame body capacity `FrameCap - 21`. For actual
positive R, `N = 1 + floor((R - 1) / C)`. A Response part is opaque bytes and
may split a UTF-8 scalar, SSE line or event; only the complete reassembled SSE
grammar receives UTF-8 validation. Do not reuse SPU2's unconditional per-body
UTF-8 decoder for these chunks. The child feeds these bytes to the existing
incremental native framer and genuine SDK parser. Transport chunking does not
rewrite SSE bytes or turn comments into model events. IPC counts are independent
of SSE framing: keep the native framer's SSEEvents counter for every complete
frame, including comment-only frames, and the policy's counter for dispatched
events. Byte chunking replaces neither original counter with the replay count.

One bounded control reader remains active during HTTP quarantine so an early
child frame cannot hide in an unread pipe and later become fresh credit. Arm
the replay state immediately before writing Response start; a single pending
Pull may then wait for that write to finish, but cannot produce a part sooner.
State checks refer to parent observation, not an unverifiable child send time.
Use fixed pending records, not a general queue or per-frame goroutines.

The child withholds part(N-1) from its reconstructed Response body until it has
also read and verified Response end. The existing native guarded-response
helper cancels at a terminal event rather than observing underlying EOF, so
it cannot independently establish this guarantee. Local stream cancellation
after that already-verified end may dispose the replay reader, but must not
retire the whole invocation before Pi emits its final. Original invocation
cancellation at any point still revokes all remaining work.

### Directional wire accounting

Let I, B and L be complete Init, request and final body ceilings; Hq and Hs the
request and response header-block ceilings; R the total response ceiling; N
the replay chunk count ceiling derived from R and C; and FrameCap the inherited
process-frame ceiling. Reserve maximums, not observed successful lengths.
Checked arithmetic must establish all these inequalities before Issue:

```text
113 + I         <= FrameCap       initialize
85 + Hq + B     <= FrameCap       fetch request
63 + Hs         <= FrameCap       response start
21 + C          <= FrameCap       each part
61              <= FrameCap       response end
17 + L          <= FrameCap       structured final
21              <= FrameCap       pull
17              <= FrameCap       retire

parent stdin = I + R + Hs + 254 + 21*N
child stdout = B + L + Hq + 102 + 21*N
total IPC    = I + B + L + R + Hq + Hs + 356 + 42*N
frame count  = 6 + 2*N
```

The parent stdin total must fit the existing 4MiB supervisor ceiling, child
stdout its original configured MIN ceiling, every frame the inherited 1MiB
MIN ceiling, and the aggregate frame count the original process event budget.
SSE event, line and body bounds remain independently enforced inside the
quarantine; replay chunk count is not the SSE event count. Use those original
bounds together;
do not lower only the tested count or increase process bounds to make it fit.
These are application wire counts, not a replacement for complete allocation,
native heap, pipe or HTTP/TLS accounting.

The implementation design must additionally close response-header admission,
capability ownership and exact maximum copy/live/node/HTTP totals before this
protocol is executable. It must retain full original bodies, history and both
image cohorts within the existing 1MiB seal/frame and other MIN bounds. No old
SPU2 wire totals can be borrowed for these extra frames. The wire magic is
selected by a newly committed native/profile artifact; existing SPU2 profiles
must never execute this grammar under unchanged commitments.

The leaf [SPU3 wire package](../../internal/pisdkhttpwire/frame.go) owns only
header-first structural validation, caller-buffer encoding, borrowed bounded
decode views, directional wire accounting and ordered byte-stream checks.
Pipe readers use `BeginHeader`, then `PreflightMetadata`, then `AcceptBody`:
order and nested request-header length are rejected before body assembly.
`Accept` validates an already-owned typed frame and is not a pre-read gate.
Decoded views must not be changed or cleared until all consumers join. Its
sequence hash compares the actual concatenated part bytes with Start and End;
its child EOF check does not prove real HTTP EOF or process quiescence. Prepare
its fixed state and all buffers on the original ledger before Issue. This
package supplies no HMAC/key access, semantic response-header admission,
quarantine approval, Source permission, resource-fit certificate or serving
registration. The original owner must provide each of those independently.

## Physical connection and deadlines

### Fixed HTTP receive boundary

The HTTP receiver has a deliberately smaller role than the semantic quarantine:
it removes HTTP framing into prepaid private buffers and proves one complete
HTTP entity plus connection EOF. Its output is untrusted DATA, not permission
to replay. It remains inside `providercredential`; no public reader, response
or approval constructor is added. The semantic guard must subsequently examine
the retained raw header block and complete entity before any IPC output.

Use a private HTTP/1.1 parser over the original TLS connection, without
`http.Client`, redirects, proxy selection, connection pooling or retries. Its
caller supplies the already bounded original use context and fixes the socket
deadline to F. The parser checks cancellation before and after every read;
closing the original socket is still required to interrupt a blocked read.
One read returning no bytes and no error denies rather than spinning.
Count and process every returned byte before handling a simultaneous EOF.
Any non-EOF error denies even with positive data. If the original connection
returns its final entity bytes with EOF, latch that actual EOF; a second
underlying read is unnecessary once complete framing has also been proved.

Let H be the original `ResponseHeaderBytes` MIN ceiling and R the complete
`ResponseBodyBytes` ceiling. Require `1 <= H <= 1MiB`, `1 <= R <= 4MiB`, and
retain the stricter existing wire, response, copy/work and process limits.
These are additional finite parser bounds, not increases to an inherited
ceiling. Preparation reserves one H-byte raw-header buffer, one R-byte entity
buffer, one H-byte canonical replay-header buffer, a fixed 64-entry header
span array and fixed read/framing scratch. No parser map, string conversion,
append growth or per-chunk allocation occurs after Issue. Its original owner
retains the ledger. The private receiver owns three slice headers, borrowed
context/reader references, the 64-entry span
array (four uint32 offsets per field), one byte of read scratch, eighteen bytes
of chunk scratch, counters and a once-only bit. Let M be
`reflect.TypeOf(piHTTPReceive{}).Size()`, including fixed scratch and spans.
Reserve `2*H + R + M` live bytes, four allocation nodes and
`128*H + 4*R + 4*(20*(R+1)) + 1024` cumulative work bytes before allocating
the state or buffers. Duplicate checks cost at most 64 comparisons per name;
all names together occupy at most H. Framing reads and chunk-size scans have
their separate `20*(R+1)` ceiling. Entity bytes enter their prepaid final arena
directly. Successful reads never replenish credits. Clear all three buffers
before retiring their live debit, only after original borrowers have joined.
Check arithmetic before Reserve and test exact/one-short debits. This is a
payload/layout schedule, not TLS/SDK/native heap or semantic-parser fit.

Admit the exact status-line prefix `HTTP/1.1 200 ` (including the final SP),
followed by zero or more printable ASCII reason bytes and CRLF. No other
version or status code matches that prefix. Admit at most 64 headers.
H includes status, every raw header and the final
empty line. Header names are nonempty ASCII HTTP tokens; names compare
case-insensitively, and every duplicate denies, including metadata duplicates.
Reject whitespace before a colon, folded lines, lone LF, controls, non-ASCII
values and empty values. Surrounding SP/HTAB trimming is the sole exception to
control rejection; internal HTAB is not admitted. Keep the original raw bytes
for subsequent secret checks.
Token names and case-insensitive comparison use
[HTTP field syntax](https://www.rfc-editor.org/rfc/rfc9110.html#section-5.1);
the 64-field ceiling and rejection of all duplicates are stricter local policy.

Require Content-Type to be case-insensitive `text/event-stream`, optionally
followed by `*(SP) ";" *(SP) "charset=utf-8"`; no spaces surround `=`.
Outer SP/HTAB is already trimmed. This is a bounded ASCII subset of the native
SSE admission. Content-Encoding must be absent or exactly `identity`, ignoring
ASCII case. Deny Trailer, Set-Cookie, Location, Refresh, WWW-Authenticate,
Proxy-Authenticate, Proxy-Authorization, Upgrade and Content-Disposition.
Other syntactically valid bounded metadata headers are retained for the raw
secret scan but never exposed to Pi. Only Content-Type, optional identity
Content-Encoding and optional Content-Length enter the canonical replay header
block, in lowercase-name order with original trimmed values. Transfer-Encoding
and Connection are never forwarded. Connection, if present, must be `close`.
The canonical block must also fit H; no new header ceiling is invented.

Require exactly one positive canonical decimal Content-Length, or exactly one
Transfer-Encoding value `chunked`, never both. A length must fit R before any
entity read. In chunked mode admit one to sixteen ASCII hexadecimal digits,
without extensions; reject a size exceeding the remaining entity capacity
before reading its data. Every nonzero chunk has its exact CRLF suffix. A zero
chunk must be followed immediately by an empty trailer section. The total
entity must be positive. With at most R nonzero chunks and at most twenty
framing bytes per chunk including its CRLFs, the finite framing-work ceiling
is `20 * (R + 1)`; count every framing byte, including the terminating chunk.
The fixed chunk-size scratch is eighteen bytes. This admits byte-sized HTTP
chunks without confusing socket fragmentation with chunk boundaries.

After declared length or terminal chunk framing, obtain actual connection EOF
(from a previous positive read plus EOF, or one additional original read):
any extra byte, timeout, cancellation or non-EOF error denies. A framed
body finishing is not itself connection EOF. No response is accepted only
because a LimitedReader or chunk decoder reports its own synthetic EOF.
Close-delimited entities, interim statuses, compression and trailers are not
admitted. These framing decisions follow
[RFC 9112 sections 6 to 8](https://www.rfc-editor.org/rfc/rfc9112.html#section-6);
the narrower syntax and metadata policy above are Sessionless choices.

TLS EOF is not a claim that a TLS close-notify was received: the installed Go
TLS reader also accepts a transport EOF at a TLS record boundary. HTTP framing
completion must therefore be established independently. The fixed sink uses
verified TLS and an explicit socket close-on-cancellation callback:
[HandshakeContext alone stops bounding the connection after the handshake](https://pkg.go.dev/crypto/tls#Conn.HandshakeContext).

Receiver tests must include exact and first-over H/R/work reservations, every
socket fragmentation including one-byte reads, both framing modes, malformed
or ambiguous framing, duplicate/forbidden headers, zero-progress reads,
premature EOF, bytes after framing, positive data accompanied by EOF, context
cancellation and a held final EOF probe. They must prove that no header/entity
view escapes on denial. Real TLS/Source/send and semantic-guard tests remain
separate mandatory consumers; parser fixtures cannot satisfy them.

### Original source and network lifetime

After full request and file-capability verification, consume the original
`PreparedAPIPhysicalSendV1` at the actual transport boundary. Retain its exact
monotonic cutoff F. DNS, TCP, TLS, the sole request write, response reads,
frame forwarding and supervised process/control lifetime must all be bounded
by the same earlier original F and cancellation. Preparation creates no
connection and grants no network effect.

The original daemon send prepares its own stopped deadline monitor before
Issue on the same ledger. The original successful Source comparison arms it
once, before publishing F or marking the send authorized. No transport driver
may supply a deadline, renew it or report an `armed` acknowledgement. Expiry
withdraws both the original send context and a separately cancellable
supervisor IO context derived from the same original owner. Normal child exit
immediately withdraws Source/network use but permits already-written Final
and EOF to drain under F; it does not cancel this bounded IO context. Actual
expiry, invalid use and parent cancellation stop the drain. Preserve the exact
original Materialize context as credential-owner identity. Immutable Finalize,
Release and resource retirement keep their separate bounded owner context.

Retirement revokes use before joining Source or the timer callback, then drops
their aliases before releasing the live reservation. Normal disarm is cleanup,
not an expiry or a new permission. Go's [Timer.Stop](https://pkg.go.dev/time#Timer.Stop)
does not join an already-started callback; the monitor requires an explicit
completion join. Its Go metadata reservation does not qualify complete runtime
timer, scheduler, stack or native heap fit.

Use the exact approved HTTPS host, port443, method and path from the sealed
profile; reject IP literals, private/special-use resolutions, redirects,
ambient proxies, alternate addresses/reconnects, compression, trailers and
automatic retries. Pin one validated public address set and one verified TLS
connection. A partial write, lost response, parser rejection or cancellation
burns the attempt; no other model, host or send follows.

The existing [pinned managed HTTPS transport](../../internal/serverlessegress/pinned_https_proxy.go)
is evidence for a one-connection mechanism, not a drop-in Pi implementation:
it seals a DirectOpenRouter route and direct credential delivery. Do not widen
that route, reuse its attestation as Pi authority or alter stopped #189 work.

F governs the actual parent-side network effect; a fresh child timeout or
cross-process clock conversion cannot extend it. Original Source-RPC elapsed
clock provenance, suspend behavior and conservative slack still require their
own qualification. Relocating the socket does not prove those facts.

## Implementation and acceptance sequence

Use the existing #202 design and #203 implementation owners, not another
activation epic. Keep the following work in the #72 decomposition:

- [ ] Independently accept the transport and opaque file-consumer amendment
  against original #203 requirements; resolve the precise transcript, frames,
  allocator schedule and credential-custody contracts before runtime code.
- [ ] Implement the exact protocol and file capability with zero-effect
  budget/foreign-scope/reentry negatives and cross-language vectors.
- [ ] Implement the fixed parent HTTPS sink and prepaid whole-response
  quarantine with decoded-string and cross-event secret/effect checks;
  prove one actual request, public-address/TLS pinning and cancellation using
  credential-free test-owned TLS peers, without altering production roots.
- [ ] Integrate the real Pi SDK and sole original runner through those bytes;
  no fake source/platform result may satisfy the genuine qualification gate.
- [ ] Qualify the actual Linux Node/SDK/image and immutable-file UID mapping,
  full original model/context/vision/allocation fit and the original clock
  bounds before Prepare can return eligible data.
- [ ] Complete actual-body #200 exporter, canonical receipt/ACK/#201 downloads,
  two-owner lifecycle proof, original-first whole-feature review and exact-head
  CI/merge. Keep existing #133/#129/#35 activation separate.

The parent remains ineligible until its original requirements are all proved.
Acceptance of this design must not be recorded as runtime conformance, a
working WebUI answer, real visual/model evidence or rollout permission.
