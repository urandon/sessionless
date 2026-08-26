package harnessconformance

import (
	"bytes"
	"errors"
	"os"
	"strings"
	"testing"
)

func TestFixtureCodecRoundTripAndStrictRejection(t *testing.T) {
	fixture := openRouterFixture(t)
	encoded, err := EncodeFixtureV1(fixture)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeFixtureV1(encoded)
	if err != nil {
		t.Fatal(err)
	}
	originalDigest, _ := fixture.Digest()
	decodedDigest, _ := decoded.Digest()
	if decodedDigest != originalDigest {
		t.Fatal("fixture round trip changed canonical digest")
	}

	duplicate := bytes.Replace(encoded, []byte(`"version":1`), []byte(`"version":1,"version":1`), 1)
	caseCollision := bytes.Replace(encoded, []byte(`"version":1`), []byte(`"version":1,"Version":1`), 1)
	unknown := bytes.Replace(encoded, []byte(`"fixture_id":`), []byte(`"unknown_field":1,"fixture_id":`), 1)
	nullValue := bytes.Replace(encoded, []byte(`"fixture_id":"openrouter-ox-alpha-public"`), []byte(`"fixture_id":null`), 1)
	noncanonicalNumber := bytes.Replace(encoded, []byte(`"version":1`), []byte(`"version":1e0`), 1)
	tooDeep := []byte(strings.Repeat(`{"a":`, maxJSONDepth+1) + `0` + strings.Repeat(`}`, maxJSONDepth+1))
	tooManyItems := []byte(`[` + strings.Repeat(`0,`, maxJSONArrayItems) + `0]`)
	tooLongString := []byte(`"` + strings.Repeat("a", maxJSONStringBytes+1) + `"`)
	invalidUTF8 := append([]byte(nil), encoded...)
	invalidUTF8[len(invalidUTF8)-1] = 0xff
	for name, candidate := range map[string][]byte{
		"empty": nil, "bom": append([]byte{0xef, 0xbb, 0xbf}, encoded...), "duplicate": duplicate,
		"case collision": caseCollision, "unknown": unknown, "null": nullValue, "trailing": append(encoded, []byte(`{}`)...),
		"noncanonical number": noncanonicalNumber, "depth": tooDeep, "array": tooManyItems, "string": tooLongString,
		"invalid utf8": invalidUTF8, "total bytes": bytes.Repeat([]byte(" "), MaxFixtureBytes+1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeFixtureV1(candidate); !errors.Is(err, ErrInvalidFixture) {
				t.Fatalf("DecodeFixtureV1 error=%v", err)
			}
		})
	}
}

func TestCommittedFixtureManifestsAreStrictAndCanonical(t *testing.T) {
	for name, expected := range map[string]FixtureV1{
		"deterministic-execute.json":      deterministicFixture(t, OperationExecuteV1),
		"openrouter-ox-alpha-public.json": openRouterFixture(t),
	} {
		t.Run(name, func(t *testing.T) {
			encoded, err := os.ReadFile("../../test/fixtures/provider-conformance/v1/" + name)
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := DecodeFixtureV1(bytes.TrimSuffix(encoded, []byte("\n")))
			if err != nil {
				t.Fatal(err)
			}
			actualDigest, _ := decoded.Digest()
			expectedDigest, _ := expected.Digest()
			if actualDigest != expectedDigest {
				t.Fatalf("fixture digest=%s, want %s", actualDigest, expectedDigest)
			}
			canonical, err := EncodeFixtureV1(decoded)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(canonical, bytes.TrimSuffix(encoded, []byte("\n"))) {
				t.Fatal("committed fixture is not canonical encoder output")
			}
		})
	}
}
