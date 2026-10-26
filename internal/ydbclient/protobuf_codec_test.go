package ydbclient

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"sync"
	"testing"

	Ydb "github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	YdbTable "github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"
	"google.golang.org/grpc/encoding"
	grpcproto "google.golang.org/grpc/encoding/proto"
	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/protoadapt"
)

// These are generated-message/owned-memory tests, not YDB, transport, credential
// or whole-phase memory proof. The fixture copies used by assertions are not
// claimed to be allocations made by the codec.
func codecYDBRequest(payload []byte) *YdbTable.ExecuteDataQueryRequest {
	return &YdbTable.ExecuteDataQueryRequest{
		SessionId: "synthetic-session",
		Query:     &YdbTable.Query{Query: &YdbTable.Query_YqlText{YqlText: "SELECT $payload, $other"}},
		Parameters: map[string]*Ydb.TypedValue{
			"$payload": {Type: &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_STRING}}, Value: &Ydb.Value{Value: &Ydb.Value_BytesValue{BytesValue: payload}}},
			"$other":   {Type: &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_UTF8}}, Value: &Ydb.Value{Value: &Ydb.Value_TextValue{TextValue: "other"}}},
		},
	}
}

func stockProtobufCodec(t *testing.T) encoding.CodecV2 {
	t.Helper()
	codec := encoding.GetCodecV2(grpcproto.Name)
	if codec == nil {
		t.Fatal("stock protobuf codec is not registered by its package")
	}
	return codec
}

func TestYDBProtobufCodecGeneratedCapacityAndSemantics(t *testing.T) {
	codec := exactCapacityProtobufCodec{}
	if codec.Name() != stockProtobufCodec(t).Name() {
		t.Fatal("protobuf content subtype changed")
	}
	for _, size := range []int{0, 1024, (32 << 10) - 1, 32 << 10, (32 << 10) + 1, 202046, (512 << 10) + 42} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			payload := bytes.Repeat([]byte("x"), size)
			request := codecYDBRequest(payload)
			before := proto.Clone(request)
			encoded, err := codec.Marshal(request)
			if err != nil {
				t.Fatalf("generated request size=%d: %v", size, err)
			}
			t.Cleanup(encoded.Free)
			if len(encoded) != 1 {
				t.Fatalf("generated request returned %d buffers, want 1", len(encoded))
			}
			if cap(encoded[0].ReadOnlyData()) != proto.Size(request) {
				t.Fatalf("generated request capacity=%d, want explicit proto.Size=%d", cap(encoded[0].ReadOnlyData()), proto.Size(request))
			}
			var decoded YdbTable.ExecuteDataQueryRequest
			if err := codec.Unmarshal(encoded, &decoded); err != nil || !proto.Equal(request, &decoded) || !proto.Equal(request, before) {
				t.Fatalf("generated request semantic round-trip/nonmutation size=%d: %v", size, err)
			}
			stock, err := stockProtobufCodec(t).Marshal(request)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(stock.Free)
			var stockDecoded YdbTable.ExecuteDataQueryRequest
			if err := codec.Unmarshal(stock, &stockDecoded); err != nil || !proto.Equal(&decoded, &stockDecoded) {
				t.Fatalf("stock/candidate request semantics size=%d: %v", size, err)
			}
			// Parameters are maps: byte ordering is intentionally not compared.
		})
	}
}

func TestYDBProtobufCodecGeneratedResultFragmented(t *testing.T) {
	codec := exactCapacityProtobufCodec{}
	result := &YdbTable.ExecuteQueryResult{ResultSets: []*Ydb.ResultSet{{Rows: []*Ydb.Value{
		{Items: []*Ydb.Value{{Value: &Ydb.Value_BytesValue{BytesValue: bytes.Repeat([]byte("r"), 202046)}}}},
	}}}}
	encoded, err := stockProtobufCodec(t).Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(encoded.Free)
	raw := encoded.Materialize()
	before := bytes.Clone(raw)
	fragments := mem.BufferSlice{mem.SliceBuffer(raw[:1]), mem.SliceBuffer(raw[1:17]), mem.SliceBuffer(raw[17:])}
	var got YdbTable.ExecuteQueryResult
	if err := codec.Unmarshal(fragments, &got); err != nil || !proto.Equal(result, &got) || !bytes.Equal(raw, before) {
		t.Fatalf("fragmented generated result semantic/borrowed-input mismatch: %v", err)
	}
	candidate, err := codec.Marshal(&got)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(candidate.Free)
	if cap(candidate[0].ReadOnlyData()) != proto.Size(&got) {
		t.Fatal("generated result borrowed a larger pool bucket")
	}
	var stockGot YdbTable.ExecuteQueryResult
	if err := stockProtobufCodec(t).Unmarshal(candidate, &stockGot); err != nil || !proto.Equal(&got, &stockGot) {
		t.Fatalf("stock cannot decode candidate result: %v", err)
	}
}

type codecLegacyMessage struct {
	Text string `protobuf:"bytes,1,opt,name=text,proto3"`
}

func (m *codecLegacyMessage) Reset()         { *m = codecLegacyMessage{} }
func (m *codecLegacyMessage) String() string { return m.Text }
func (*codecLegacyMessage) ProtoMessage()    {}

func TestYDBProtobufCodecLegacyAndNilParity(t *testing.T) {
	codec := exactCapacityProtobufCodec{}
	var typedNil *YdbTable.ExecuteDataQueryRequest
	for _, value := range []any{nil, 17, struct{}{}, typedNil, &YdbTable.ExecuteDataQueryRequest{}, &codecLegacyMessage{Text: "legacy"}, &YdbTable.ExecuteDataQueryRequest{SessionId: "\xff"}} {
		t.Run(fmt.Sprintf("%T", value), func(t *testing.T) {
			got, gotErr := codec.Marshal(value)
			t.Cleanup(got.Free)
			want, wantErr := stockProtobufCodec(t).Marshal(value)
			t.Cleanup(want.Free)
			if (gotErr == nil) != (wantErr == nil) {
				t.Fatalf("marshal error parity got=%v want=%v", gotErr, wantErr)
			}
			if gotErr != nil {
				if gotErr.Error() != wantErr.Error() || len(got) != 0 {
					t.Fatalf("marshal error/output changed got=%v want=%v buffers=%d", gotErr, wantErr, len(got))
				}
				return
			}
			// These fixtures contain no maps; compare the independent stock wire.
			if !bytes.Equal(got.Materialize(), want.Materialize()) {
				t.Fatal("legacy/nil/empty stock protobuf wire changed")
			}
		})
	}
	legacy := &codecLegacyMessage{Text: "legacy"}
	encoded, err := codec.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(encoded.Free)
	var decoded codecLegacyMessage
	if err := codec.Unmarshal(encoded, &decoded); err != nil || !proto.Equal(protoadapt.MessageV2Of(legacy), protoadapt.MessageV2Of(&decoded)) {
		t.Fatalf("legacy v1 adaptation: %v", err)
	}
	for _, value := range []any{nil, 17, struct{}{}} {
		got := codec.Unmarshal(nil, value)
		want := stockProtobufCodec(t).Unmarshal(nil, value)
		if got == nil || want == nil || got.Error() != want.Error() {
			t.Fatalf("invalid unmarshal target %T: got=%v want=%v", value, got, want)
		}
	}
}

func TestYDBProtobufCodecUnknownNonminimalMalformedAndReset(t *testing.T) {
	codec := exactCapacityProtobufCodec{}
	for _, tc := range []struct {
		name string
		raw  []byte
	}{
		{name: "empty"},
		{name: "nonminimal-length", raw: []byte{0x0a, 0x81, 0x00, 'x'}},
		{name: "unknown-nonminimal-varint", raw: []byte{0xa0, 0x06, 0x81, 0x00}},
		{name: "truncated", raw: []byte{0x0a, 0x04, 'x'}},
		{name: "invalid-tag", raw: []byte{0}},
		{name: "invalid-utf8", raw: []byte{0x0a, 0x01, 0xff}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := bytes.Clone(tc.raw)
			before := bytes.Clone(raw)
			mid := len(raw) / 2
			fragments := mem.BufferSlice{mem.SliceBuffer(raw[:mid]), mem.SliceBuffer(raw[mid:])}
			got, want := codecYDBRequest([]byte("must-reset")), codecYDBRequest([]byte("must-reset"))
			gotErr := codec.Unmarshal(fragments, got)
			wantErr := stockProtobufCodec(t).Unmarshal(fragments, want)
			if (gotErr == nil) != (wantErr == nil) || !proto.Equal(got, want) || !bytes.Equal(raw, before) {
				t.Fatalf("decode parity/nonmutation got=%v want=%v", gotErr, wantErr)
			}
			if gotErr != nil {
				if gotErr.Error() != wantErr.Error() {
					t.Fatalf("malformed error changed got=%v want=%v", gotErr, wantErr)
				}
				return
			}
			encoded, err := codec.Marshal(got)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(encoded.Free)
			var restored YdbTable.ExecuteDataQueryRequest
			if err := codec.Unmarshal(encoded, &restored); err != nil || !proto.Equal(got, &restored) || !bytes.Equal(got.ProtoReflect().GetUnknown(), restored.ProtoReflect().GetUnknown()) {
				t.Fatalf("unknown fields/nonminimal semantics lost: %v", err)
			}
		})
	}
}

type codecObservedPool struct{ puts int }

func (*codecObservedPool) Get(size int) *[]byte { b := make([]byte, size); return &b }
func (p *codecObservedPool) Put(*[]byte)        { p.puts++ }

func TestYDBProtobufCodecBorrowedReceiveReferences(t *testing.T) {
	raw, err := proto.Marshal(codecYDBRequest(bytes.Repeat([]byte("p"), 4096)))
	if err != nil {
		t.Fatal(err)
	}
	before := bytes.Clone(raw)
	pool := new(codecObservedPool)
	data := mem.BufferSlice{mem.NewBuffer(&raw, pool)}
	refs := 1
	t.Cleanup(func() {
		for refs > 0 {
			data.Free()
			refs--
		}
		if pool.puts != 1 {
			t.Errorf("receive backing array returned %d times, want 1", pool.puts)
		}
	})
	data.Ref() // The transport and an independent retained reader both own it.
	refs++
	var got YdbTable.ExecuteDataQueryRequest
	if err := (exactCapacityProtobufCodec{}).Unmarshal(data, &got); err != nil || pool.puts != 0 || !bytes.Equal(raw, before) {
		t.Fatalf("codec released or mutated caller receive references: puts=%d err=%v", pool.puts, err)
	}
	data.Free()
	refs--
	if pool.puts != 0 {
		t.Fatal("release of one caller reference retired the other reader's bytes")
	}
	data.Free()
	refs--
	if pool.puts != 1 {
		t.Fatalf("last receive reference did not release backing array: puts=%d", pool.puts)
	}
}

func TestYDBProtobufCodecRetainedSendReferences(t *testing.T) {
	data, err := (exactCapacityProtobufCodec{}).Marshal(codecYDBRequest(bytes.Repeat([]byte("q"), 202046)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(data.Free)
	want := data.Materialize()
	data.Ref()
	data.Ref()
	data.Free() // The codec's initial handoff may end before queue/retry owners.
	var group sync.WaitGroup
	errors := make(chan string, 2)
	for range 2 {
		group.Go(func() {
			defer data.Free()
			if !bytes.Equal(data[0].ReadOnlyData(), want) {
				errors <- "retained queue/retry send bytes changed"
			}
		})
	}
	group.Wait()
	close(errors)
	for err := range errors {
		t.Error(err)
	}
	// SliceBuffer Free is intentionally not asserted to erase successful data.
}

func TestYDBProtobufAllocationOptionsAtSharedConstructor(t *testing.T) {
	// Source guard only: no driver, credentials, endpoint or service is opened.
	file, err := parser.ParseFile(token.NewFileSet(), "client.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var shared *ast.FuncDecl
	for _, decl := range file.Decls {
		if function, ok := decl.(*ast.FuncDecl); ok && function.Name.Name == "open" {
			shared = function
		}
	}
	if shared == nil {
		t.Fatal("shared operational/scripting constructor missing")
	}
	calls := make(map[string]int)
	ast.Inspect(shared, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		if owner, ok := selector.X.(*ast.Ident); ok {
			calls[owner.Name+"."+selector.Sel.Name]++
		}
		return true
	})
	for _, required := range strings.Fields("ydb.Open environ.WithEnvironCredentials ydb.With config.WithGrpcOptions experimental.WithBufferPool grpc.WithDefaultCallOptions grpc.ForceCodecV2") {
		if calls[required] != 1 {
			t.Errorf("shared constructor %s calls=%d, want 1", required, calls[required])
		}
	}
}
