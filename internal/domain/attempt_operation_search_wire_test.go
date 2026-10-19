package domain_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func searchWireOperationFixture() domain.AttemptOperationV1 {
	op := toolOperationFixture()
	op.SearchWire = &domain.AttemptOperationSearchWireV1{Version: 1, APIProfile: domain.WebSearchAPIProfileV1, ArgumentsBytes: 4096, CompilerArtifactDigest: strings.Repeat("a", 64)}
	return op
}

func TestAttemptOperationSearchWireOriginalV1Goldens(t *testing.T) {
	// Captured BEFORE the search_wire declaration, using these unchanged
	// original fixture helpers. Expected hashes are literal historical evidence.
	type golden struct {
		bytes         int
		raw, semantic string
	}
	want := map[domain.AttemptOperationKindV1][3]golden{
		domain.AttemptOperationModelV1: {
			{1671, "b34084a4afb9f616c2a09dae7784dcefb4f72e7d0d028ee3615a03c54fafa0ef", "bb4dd20ad78b99a72c79a108d649e23617e5783e23f42c94faff9f32257aae60"},
			{2373, "102589c7bc3fa979eaae3b838e035e7ac8d3352673d6ed49c29c4d9e29eb12bb", "af7b1df95796c73ec69304af7e1ce828b7425ca4c3981820f58700854cbf9654"},
			{424, "e89d15bef219bcd5700a2c0d2a9d25008f0b2592461a10b3790b6296394922b0", "be225c2baf7da6811ad9ccd6d57fd4b6468e8fabc54f09562628284a2c12dfab"},
		},
		domain.AttemptOperationWebSearchV1: {
			{2194, "162172ddd693b063e72e5a88489b5384cfc95bc75534d206b4420b56e4dda33c", "c1a6a4f0f075b382ca148dabaaf731606e26ec2c9e5e499918a0e8f2a740e8c0"},
			{2893, "794db9cf2ce8c0053dbdd35e7db6948614ee5225f4820a9dafeb4c5acf783309", "b24422805534480ac4d7f2694b0dcfa127d5be3979a6ed14bfb507ed215731b8"},
			{424, "8176e686fb1b4557c113ba021d0221913e751870ce5861ab2a0c49a433518691", "3bbc8c797c2c567a07319e3c58a09005448e3beebc154400d5451f4d14fbcca7"},
		},
	}
	for _, op := range []domain.AttemptOperationV1{operationFixture(), toolOperationFixture()} {
		t.Run(string(op.Kind), func(t *testing.T) {
			r := operationReservationFixture(t, op)
			o := completedOperationFixture(t, r)
			od, oe := op.Digest()
			rd, re := r.Digest()
			obsd, obse := o.Digest()
			if oe != nil || re != nil || obse != nil {
				t.Fatalf("original fixture: operation=%v reservation=%v observation=%v", oe, re, obse)
			}
			values := []struct {
				name   string
				value  any
				digest string
			}{{"operation", op, string(od)}, {"reservation", r, string(rd)}, {"observation", o, string(obsd)}}
			for i, item := range values {
				t.Run(item.name, func(t *testing.T) {
					body, err := json.Marshal(item.value)
					if err != nil {
						t.Fatal(err)
					}
					h := sha256.Sum256(body)
					g := want[op.Kind][i]
					if len(body) != g.bytes || hex.EncodeToString(h[:]) != g.raw || item.digest != g.semantic {
						t.Fatalf("historical V1 changed: bytes=%d want=%d raw=%x want=%s semantic=%s want=%s", len(body), g.bytes, h, g.raw, item.digest, g.semantic)
					}
				})
			}
		})
	}
}

func TestAttemptOperationSearchWireDistinctBoundsAndClosedArm(t *testing.T) {
	t.Run("valid-wire-above-old-argument-cap", func(t *testing.T) {
		op := searchWireOperationFixture()
		op.RequestBytes = 8192
		op.Debit.BodyBytes = op.RequestBytes + op.ResultBytes
		if err := op.Validate(); err != nil {
			t.Fatalf("valid separate 4096 argument /8192 physical body rejected: %v", err)
		}
	})
	for _, test := range []struct {
		name   string
		change func(*domain.AttemptOperationV1)
	}{
		{"unknown-version", func(v *domain.AttemptOperationV1) { v.SearchWire.Version = 2 }},
		{"missing-version", func(v *domain.AttemptOperationV1) { v.SearchWire.Version = 0 }},
		{"wrong-profile", func(v *domain.AttemptOperationV1) { v.SearchWire.APIProfile = "tavily.extract.v1" }},
		{"missing-arguments", func(v *domain.AttemptOperationV1) { v.SearchWire.ArgumentsBytes = 0 }},
		{"argument-overflow", func(v *domain.AttemptOperationV1) { v.SearchWire.ArgumentsBytes = 4097 }},
		{"missing-compiler", func(v *domain.AttemptOperationV1) { v.SearchWire.CompilerArtifactDigest = "" }},
		{"invalid-compiler", func(v *domain.AttemptOperationV1) { v.SearchWire.CompilerArtifactDigest = "not-a-digest" }},
		{"physical-overflow", func(v *domain.AttemptOperationV1) {
			v.RequestBytes = 8193
			v.Debit.BodyBytes = v.RequestBytes + v.ResultBytes
		}},
		{"model-kind", func(v *domain.AttemptOperationV1) { arm := v.SearchWire; *v = operationFixture(); v.SearchWire = arm }},
		{"mcp-kind", func(v *domain.AttemptOperationV1) {
			v.Kind = domain.AttemptOperationMCPToolV1
			v.Debit.WebSearches = 0
			v.QueryDigest = ""
			v.ConsentRequestDigest = ""
			v.ConsentDecisionDigest = ""
			v.ConsentDecisionRef = ""
			v.Debit.MCPOutboundMessages = 1
			v.Debit.MCPInboundFrames = 1
			v.Debit.MCPInboundBytes = 1
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			op := searchWireOperationFixture()
			test.change(&op)
			if op.Validate() == nil {
				t.Fatal("partial/foreign search-wire arm ignored")
			}
		})
	}
	t.Run("legacy-search-still-four-KiB", func(t *testing.T) {
		op := toolOperationFixture()
		op.RequestBytes = 4097
		op.Debit.BodyBytes = op.RequestBytes + op.ResultBytes
		if op.Validate() == nil {
			t.Fatal("legacy V1 cap widened")
		}
	})
}

func TestAttemptOperationSearchWireDeepClone(t *testing.T) {
	for _, kind := range []string{"operation", "reservation"} {
		t.Run(kind, func(t *testing.T) {
			op := searchWireOperationFixture()
			var cloned domain.AttemptOperationV1
			if kind == "operation" {
				cloned = op.Clone()
			} else {
				r := operationReservationFixture(t, op)
				cloned = r.Clone().Operation
			}
			cloned.SearchWire.ArgumentsBytes++
			if op.SearchWire.ArgumentsBytes != 4096 {
				t.Fatal("clone aliases original search-wire arm")
			}
		})
	}
}

func TestAttemptOperationSearchWireEveryMutableArmFieldIsCommitted(t *testing.T) {
	base := searchWireOperationFixture()
	original, err := base.Digest()
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		change func(*domain.AttemptOperationV1)
	}{
		{"arguments-bytes", func(v *domain.AttemptOperationV1) { v.SearchWire.ArgumentsBytes-- }},
		{"compiler-artifact", func(v *domain.AttemptOperationV1) { v.SearchWire.CompilerArtifactDigest = strings.Repeat("b", 64) }},
		{"arm-presence", func(v *domain.AttemptOperationV1) { v.SearchWire = nil }},
	} {
		t.Run(test.name, func(t *testing.T) {
			changed := base.Clone()
			test.change(&changed)
			actual, err := changed.Digest()
			if err != nil || actual == original {
				t.Fatalf("valid arm mutation not independently committed: kind=%s digest=%s original=%s err=%v", test.name, actual, original, err)
			}
			if base.SearchWire.ArgumentsBytes != 4096 || base.SearchWire.CompilerArtifactDigest != strings.Repeat("a", 64) {
				t.Fatal("mutation escaped clone into original commitment")
			}
		})
	}
}
