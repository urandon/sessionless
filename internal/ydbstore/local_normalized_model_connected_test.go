package ydbstore_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"reflect"
	"strconv"
	"testing"

	"gitcode.com/urandon/sessionless/internal/configuredinference"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/localloopback"
	"gitcode.com/urandon/sessionless/internal/operationbody"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

// Real original Source, runtime, issuers, Store, codec/body adapter and owned
// TCP peer and genuine original Source/funding authentication. SQL and blob
// STORAGE remain SCRIPTED; external service documents are synthetic owned
// declarations. No native YDB, actual model, live service funding or complete
// loop is claimed.
func connectedNormalizedModel(t *testing.T) (*ydbstore.LocalLoopbackConnectedFixture, *connectedOperationBlobs, []byte, domain.AttemptOperationUsageV1) {
	t.Helper()
	peer := newLocalLoopbackPeer(t, true)
	f, keeper, blobs := prepareConnectedOperationBody(t, peer)
	f.UseNormalizedModelResultBound(t)
	conn, err := keeper.Preflight(f.Context, f.Allocation, f.ExecutionSource, f.Operation, f.Payload())
	if err != nil {
		t.Fatalf("normalized actual original preflight: %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Error(err)
		}
	})
	peer.waitAccepted(t)
	proof, err := conn.Publication()
	if err != nil {
		t.Fatal(err)
	}
	registered, capability := f.RegisterAndReserve(t, proof)
	result, err := conn.Invoke(f.Context, registered, capability, f.Payload())
	if err != nil || !result.SendAttempted {
		t.Fatalf("normalized original Store-authorized HTTP: err=%v attempted=%t", err, result.SendAttempted)
	}
	f.AssertOriginalCapabilityConsumed(t, capability)
	observed := peer.waitResult(t)
	if observed.err != nil || observed.request == nil || !bytes.Equal(observed.body, f.Payload()) || len(observed.extra) != 0 {
		t.Fatalf("normalized proof requires one exact actual HTTP request: bodyBytes=%d extra=%d err=%v", len(observed.body), len(observed.extra), observed.err)
	}
	if _, exists := observed.request.Header["Authorization"]; exists || observed.request.Method != "POST" || observed.request.RequestURI != "/v1/chat/completions" {
		t.Fatal("normalized HTTP path lost fixed noauth POST route")
	}
	response, err := f.Frozen.DecodeResponse(result.Body)
	if err != nil || response.Stop != configuredinference.StopFinalV1 || response.Text != "owned peer result" || response.ModelProvenance != configuredinference.ModelReportedV1 {
		t.Fatalf("real peer bounded codec response: err=%v stop=%s", err, response.Stop)
	}
	usage, err := response.Usage.OperationUsage()
	if err != nil || usage.State != domain.AttemptOperationUsageProviderReportedV1 || usage.AmountMicrounits != nil {
		t.Fatal("connected usage is a provider report, not measured money")
	}
	if again, err := conn.Invoke(f.Context, registered, capability, f.Payload()); err == nil || again.SendAttempted {
		t.Fatal("normalized operation repeated original physical HTTP effect")
	}
	return f, blobs, bytes.Clone(result.Body), usage
}

func connectedNormalizedBody(t testing.TB, f *ydbstore.LocalLoopbackConnectedFixture, raw []byte) []byte {
	t.Helper()
	body, err := f.Frozen.NormalizeResponseBodyV1(raw, domain.AttemptOperationModelV1, 4096)
	if err != nil || len(body) == 0 || len(body) > 4096 {
		t.Fatalf("actual wire normalization into admitted result slot: bytes=%d err=%v", len(body), err)
	}
	return body
}

func TestLocalNormalizedModelConnectedRecordReadAndHistoricalSourceClose(t *testing.T) {
	f, blobs, raw, usage := connectedNormalizedModel(t)
	body := connectedNormalizedBody(t, f, raw)
	ref := f.WriteOperationBody(t, body)
	authenticated := f.AuthenticateCompletedModelObservation(t, ref, usage)
	if err := f.RecordAuthenticatedModelObservation(t, authenticated); err != nil {
		t.Fatalf("actual normalized body custody/MAC/Store record: %v", err)
	}
	f.AssertObservationWrites(t, 1)
	read, err := f.ReadOriginalModelObservation(t)
	if err != nil || !reflect.DeepEqual(read, authenticated) {
		t.Fatalf("original authenticated observation readback differs: err=%v", err)
	}
	if read.Observation.ResponseRef != ref.Ref || read.Observation.ResponseDigest != ref.Digest || read.Observation.ResponseBytes != ref.Bytes || !reflect.DeepEqual(read.Observation.Usage, usage) {
		t.Fatal("Store lost exact normalized body identity or truthful usage")
	}
	if err := f.RecordAuthenticatedModelObservation(t, authenticated); err != nil {
		t.Fatalf("exact original observation replay: %v", err)
	}
	f.AssertObservationWrites(t, 1)
	f.AssertReplayNoNewCapability(t)
	if puts, opens, closes, deletes := blobs.counts(); puts != 2 || opens < 5 || opens != closes || deletes != 0 {
		t.Fatalf("request/response custody or replay rewrote bodies: put=%d open=%d close=%d delete=%d", puts, opens, closes, deletes)
	}
	profile, err := f.Source.Configuration()
	if err != nil {
		t.Fatalf("capture original decoding DATA before Source destruction: %v", err)
	}
	originalRequest := f.Payload()
	originalOperation := f.Operation.Clone()
	originalObservation := authenticated.Observation.Clone()
	if originalOperation.RequestDigest == originalObservation.ResponseDigest || originalObservation.Usage.InputTokens == nil {
		t.Fatal("retained DATA controls require distinct original request/response hashes and reported input usage")
	}
	if err := f.Source.Close(); err != nil {
		t.Fatal(err)
	}
	// Retained decoding DATA survives retirement; it is not a replacement for
	// authentication by the now-destroyed original installation Source.
	if err := f.ValidateRetainedNormalizedModelData(t, originalOperation, originalObservation); err != nil {
		t.Fatalf("original bootstrap lost retained normalized DATA decoding: %v", err)
	}
	decoded, err := configuredinference.DecodeNormalizedResponseBodyV1(profile, originalRequest, body, originalOperation.Kind)
	if err != nil || decoded.Stop != configuredinference.StopFinalV1 || decoded.Text != "owned peer result" || decoded.Call != nil {
		t.Fatalf("retained normalized DATA changed after Source destruction: stop=%s text=%q err=%v", decoded.Stop, decoded.Text, err)
	}
	decodedUsage, err := decoded.Usage.OperationUsage()
	if err != nil || !reflect.DeepEqual(decodedUsage, originalObservation.Usage) {
		t.Fatalf("retained normalized DATA lost exact original usage: got=%+v want=%+v err=%v", decodedUsage, originalObservation.Usage, err)
	}
	wrongRequest := originalOperation.Clone()
	wrongRequest.RequestDigest = originalObservation.ResponseDigest
	wrongBody := originalObservation.Clone()
	wrongBody.ResponseDigest = originalOperation.RequestDigest
	wrongUsage := originalObservation.Clone()
	(*wrongUsage.Usage.InputTokens)++
	for _, tc := range []struct {
		name        string
		operation   domain.AttemptOperationV1
		observation domain.AttemptOperationObservationV1
	}{
		{name: "request-digest-substitution", operation: wrongRequest, observation: originalObservation},
		{name: "body-digest-substitution", operation: originalOperation, observation: wrongBody},
		{name: "usage-substitution", operation: originalOperation, observation: wrongUsage},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := f.ValidateRetainedNormalizedModelData(t, tc.operation, tc.observation); !errors.Is(err, domain.ErrMembershipDenied) {
				t.Fatalf("retained DATA accepted %s: %v", tc.name, err)
			}
		})
	}
	if err := f.ValidateRetainedNormalizedModelData(t, originalOperation, originalObservation); err != nil {
		t.Fatalf("negative DATA controls mutated the retained original: %v", err)
	}
	beforePuts, beforeOpens, beforeCloses, beforeDeletes := blobs.counts()
	read, err = f.ReadOriginalModelObservation(t)
	if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(read, ports.AuthenticatedAttemptOperationObservationV1{}) {
		t.Fatalf("destroyed Source authenticated a fresh observation read: result=%+v err=%v", read, err)
	}
	if puts, opens, closes, deletes := blobs.counts(); puts != beforePuts || opens != beforeOpens || closes != beforeCloses || deletes != beforeDeletes {
		t.Fatalf("closed-Source authenticated-read denial touched bodies: put=%d/%d open=%d/%d close=%d/%d delete=%d/%d", puts, beforePuts, opens, beforeOpens, closes, beforeCloses, deletes, beforeDeletes)
	}
	if _, err := f.Source.Configuration(); err == nil {
		t.Fatal("closed actual Source still exported current configuration")
	}
	// Historical DATA is not permission to enter a fresh connector or obtain a
	// fresh original operation capability after the actual Source is closed.
	if _, err := localloopback.NewKeeperV1(f.Source, f.OperationIssuer, f.Bootstrap); err == nil {
		t.Fatal("historical configuration enabled a new closed-Source keeper")
	}
	if result, err := f.ReserveOriginalOperationAgain(t); err == nil && result.Capability != nil {
		t.Fatal("historical result minted another physical send capability")
	}
	f.AssertObservationWrites(t, 1)
	if puts, opens, closes, deletes := blobs.counts(); puts != 2 || opens != closes || deletes != 0 {
		t.Fatalf("retained DATA validation/closed-Source denials changed body custody: put=%d open=%d close=%d delete=%d", puts, opens, closes, deletes)
	}
}

func TestLocalNormalizedModelConnectedAuthenticatedInvalidBodiesDenyBeforeWrite(t *testing.T) {
	f, _, raw, usage := connectedNormalizedModel(t)
	body := connectedNormalizedBody(t, f, raw)
	profile, err := f.Source.Configuration()
	if err != nil {
		t.Fatal(err)
	}
	otherRequest, err := configuredinference.FreezeRequestV1(profile, profile.Limits, configuredinference.RequestV1{
		Messages: []configuredinference.MessageV1{{Role: configuredinference.RoleUserV1, Text: "different immutable request"}}, MaxOutputTokens: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	wrongRequest, err := otherRequest.NormalizeResponseBodyV1(raw, domain.AttemptOperationModelV1, 4096)
	if err != nil {
		t.Fatal(err)
	}
	otherProfile := profile.Clone()
	otherProfile.ProfileRevision++
	otherFrozen, err := configuredinference.FreezeRequestV1(otherProfile, otherProfile.Limits, configuredinference.RequestV1{
		Messages: []configuredinference.MessageV1{{Role: configuredinference.RoleUserV1, Text: "owned test peer, not a model"}}, MaxOutputTokens: 1,
	})
	if err != nil || !bytes.Equal(otherFrozen.Body(), f.Payload()) {
		t.Fatalf("profile-only mutation must retain exact original wire request: %v", err)
	}
	wrongProfile, err := otherFrozen.NormalizeResponseBodyV1(raw, domain.AttemptOperationModelV1, 4096)
	if err != nil {
		t.Fatal(err)
	}
	wrongUsage := usage.Clone()
	if wrongUsage.InputTokens == nil {
		t.Fatal("causal wrong-usage control requires original reported input count")
	}
	(*wrongUsage.InputTokens)++
	for _, tc := range []struct {
		name    string
		body    []byte
		usage   domain.AttemptOperationUsageV1
		persist bool
	}{
		{name: "unpersisted-reference", body: body, usage: usage},
		{name: "wrong-original-request", body: wrongRequest, usage: usage, persist: true},
		{name: "wrong-fixed-profile", body: wrongProfile, usage: usage, persist: true},
		{name: "wrong-authenticated-usage", body: body, usage: wrongUsage, persist: true},
		{name: "fixture-body-not-promoted", body: []byte(`{"version":1,"kind":"model_response","text":"done","calls":[]}`), usage: usage, persist: true},
		{name: "raw-HTTP-body-not-normalized", body: raw, usage: usage, persist: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sum := sha256.Sum256(tc.body)
			digest := hex.EncodeToString(sum[:])
			ref := operationbody.ReferenceV1{Ref: "opbody-v1." + strconv.Itoa(len(tc.body)) + "." + digest, Digest: digest, Bytes: uint64(len(tc.body))}
			if tc.persist {
				ref = f.WriteOperationBody(t, tc.body)
			}
			authenticated := f.AuthenticateCompletedModelObservation(t, ref, tc.usage)
			if authenticated.Validate() != nil {
				t.Fatal("negative must possess the genuine structurally valid original issuer authentication")
			}
			if err := f.RecordAuthenticatedModelObservation(t, authenticated); !errors.Is(err, domain.ErrMembershipDenied) {
				t.Fatalf("invalid but genuinely authenticated response accepted: %v", err)
			}
			f.AssertObservationWrites(t, 0)
			if read, err := f.ReadOriginalModelObservation(t); err == nil || !reflect.DeepEqual(read, ports.AuthenticatedAttemptOperationObservationV1{}) {
				t.Fatal("rejected response left a usable authenticated observation")
			}
		})
	}
	f.AssertSingleOriginalWrites(t)
}

func TestLocalNormalizedModelConnectedCorruptedCommittedBodyDeniesRead(t *testing.T) {
	f, blobs, raw, usage := connectedNormalizedModel(t)
	body := connectedNormalizedBody(t, f, raw)
	ref := f.WriteOperationBody(t, body)
	authenticated := f.AuthenticateCompletedModelObservation(t, ref, usage)
	if err := f.RecordAuthenticatedModelObservation(t, authenticated); err != nil {
		t.Fatalf("causal corruption control requires committed normalized observation: %v", err)
	}
	if read, err := f.ReadOriginalModelObservation(t); err != nil || !reflect.DeepEqual(read, authenticated) {
		t.Fatalf("pre-corruption authenticated control: %v", err)
	}
	blobs.corruptOriginal(t) // Last actual PUT is the normalized response, not request.
	if read, err := f.ReadOriginalModelObservation(t); !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(read, ports.AuthenticatedAttemptOperationObservationV1{}) {
		t.Fatalf("corrupted committed body returned authenticated success: %v", err)
	}
	f.AssertObservationWrites(t, 1)
	if puts, opens, closes, deletes := blobs.counts(); puts != 2 || opens != closes || deletes != 0 {
		t.Fatalf("corruption read retried physical write, leaked reader, or deleted evidence: put=%d open=%d close=%d delete=%d", puts, opens, closes, deletes)
	}
}
