package pisdkmedia

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func TestJSONWireECMAScriptCompleteDataSemantics(t *testing.T) {
	for _, tc := range []struct{ name, source, want string }{
		{"html-line-separators", `{"s":"<>&\u2028\u2029\ud83d\udcac"}`, "{\"s\":\"<>&\u2028\u2029💬\"}"},
		{"index-order", `{"4294967295":"last","01":"ordinary","4294967294":"index","0":"zero","__proto__":{"value":true}}`, `{"0":"zero","4294967294":"index","4294967295":"last","01":"ordinary","__proto__":{"value":true}}`},
		{"controls", `{"s":"\"\b\f\n\r\t\\\u0000"}`, `{"s":"\"\b\f\n\r\t\\\u0000"}`},
		{"safe-boundary", `{"a":9007199254740991,"b":-9007199254740991,"negativeZero":-0,"v":[true,false,null,{},[]]}`, `{"a":9007199254740991,"b":-9007199254740991,"negativeZero":0,"v":[true,false,null,{},[]]}`},
		{"full-schema", `{"type":"object","$defs":{"artifact":{"type":"object","properties":{"name":{"type":"string"},"content":{"type":"string"}},"required":["name","content"],"additionalProperties":false}},"properties":{"answer":{"type":"string"},"artifacts":{"type":"array","items":{"$ref":"#/$defs/artifact"}}},"required":["answer"],"additionalProperties":false}`, `{"type":"object","$defs":{"artifact":{"type":"object","properties":{"name":{"type":"string"},"content":{"type":"string"}},"required":["name","content"],"additionalProperties":false}},"properties":{"answer":{"type":"string"},"artifacts":{"type":"array","items":{"$ref":"#/$defs/artifact"}}},"required":["answer"],"additionalProperties":false}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ledger := fixtureLedger(t, domain.AttachedPiSDKNativeLimitsV1{PreSendAllocationBytes: 1 << 20, ProcessMemoryBytes: 1 << 20, JSONNodes: 10000})
			v, e := parseWireData(context.Background(), []byte(tc.source), 32, 10000, ledger)
			if e != nil {
				t.Fatal(e)
			}
			m := wireWriter{cap: 1 << 20, maxDepth: 32, maxNodes: 10000}
			m.value(v)
			if m.err != nil {
				t.Fatal(m.err)
			}
			out := wireWriter{out: make([]byte, 0, int(m.bytes)), cap: m.bytes, maxDepth: 32, maxNodes: 10000}
			out.value(v)
			if out.err != nil || string(out.out) != tc.want || out.bytes != m.bytes || out.units != m.units || out.nodes != m.nodes {
				t.Fatalf("wire differs got=%q want=%q", out.out, tc.want)
			}
		})
	}
}

func TestJSONWirePreexistingErrorStopsScalarWalk(t *testing.T) {
	primary := errors.New("synthetic primary")
	for _, bytesMode := range []bool{false, true} {
		w := wireWriter{err: primary, cap: 8, maxDepth: 8, maxNodes: 8}
		// Invalid UTF8 would replace the primary failure if the scalar is walked.
		if bytesMode {
			w.bytesPart([]byte{0xff})
		} else {
			w.stringPart(string([]byte{0xff}))
		}
		if w.err != primary || w.bytes != 0 || w.units != 0 || w.nodes != 0 {
			t.Fatal("failed writer continued scalar validation")
		}
	}
}
func TestJSONWireGrammarBeforeEmissionDenials(t *testing.T) {
	for _, input := range []string{`{"a":1,"a":2}`, `{"a":01}`, `{"a":+1}`, `{"a":9007199254740992}`, `{"a":0.1}`, `{"a":1e2}`, `{"a":"\ud800"}`, `{"a":"\udc00"}`, `{"a":NaN}`, `{"a":true,}`, `{"a":[]} trailing`} {
		t.Run(input, func(t *testing.T) {
			l := fixtureLedger(t, domain.AttachedPiSDKNativeLimitsV1{PreSendAllocationBytes: 1 << 20, ProcessMemoryBytes: 1 << 20, JSONNodes: 1000})
			if v, e := parseWireData(context.Background(), []byte(input), 32, 1000, l); e == nil || v != nil {
				t.Fatal("invalid/unsupported source data admitted")
			}
		})
	}
	for _, depth := range []uint64{1, 2} {
		l := fixtureLedger(t, domain.AttachedPiSDKNativeLimitsV1{PreSendAllocationBytes: 1 << 20, ProcessMemoryBytes: 1 << 20, JSONNodes: 1000})
		if _, e := parseWireData(context.Background(), []byte(`{"a":[{"b":true}]}`), depth, 1000, l); e == nil {
			t.Fatal("first over depth admitted")
		}
	}
}
func TestJSONWireStreamedOriginalBase64Padding(t *testing.T) {
	for n := 1; n <= 7; n++ {
		body := bytes.Repeat([]byte{0x81}, n)
		m := wireWriter{cap: 1024, maxDepth: 32, maxNodes: 100}
		m.dataURL("image/png", body)
		w := wireWriter{out: make([]byte, 0, int(m.bytes)), cap: m.bytes, maxDepth: 32, maxNodes: 100}
		w.dataURL("image/png", body)
		if w.err != nil || w.bytes != m.bytes || len(w.out) != int(m.bytes) {
			t.Fatal("streamed measure/emit mismatch")
		}
		if string(w.out) != `"data:image/png;base64,`+base64.StdEncoding.EncodeToString(body)+`"` {
			t.Fatal("streamed original base64 differs from independent encoder")
		}
	}
}

func TestJSONWireExactDepthNodesAndFirstOverflow(t *testing.T) {
	source := []byte(`{"a":[{"b":true}]}`)
	const depth, nodes = 3, 6 // root/key/array/object/key/scalar, container depth3
	for _, tc := range []struct {
		depth, nodes uint64
		ok           bool
	}{{depth, nodes, true}, {depth - 1, nodes, false}, {depth, nodes - 1, false}} {
		l := fixtureLedger(t, domain.AttachedPiSDKNativeLimitsV1{PreSendAllocationBytes: 1 << 20, ProcessMemoryBytes: 1 << 20, JSONNodes: 1000})
		v, e := parseWireData(context.Background(), source, tc.depth, tc.nodes, l)
		if (e == nil) != tc.ok || !tc.ok && v != nil {
			t.Fatalf("boundary depth=%d nodes=%d: %v", tc.depth, tc.nodes, e)
		}
		if tc.ok {
			w := wireWriter{cap: uint64(len(source)), maxDepth: tc.depth, maxNodes: tc.nodes}
			w.value(v)
			if w.err != nil || w.nodes != nodes || w.bytes != uint64(len(source)) {
				t.Fatal("exact positive measure")
			}
			o := wireWriter{out: make([]byte, 0, int(w.bytes)), cap: w.bytes - 1, maxDepth: depth, maxNodes: nodes}
			o.value(v)
			if o.err == nil {
				t.Fatal("first byte overflow emitted")
			}
		}
	}
}
