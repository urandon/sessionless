package ydbclient

import (
	"fmt"

	grpcproto "google.golang.org/grpc/encoding/proto"
	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/protoadapt"
)

// exactCapacityProtobufCodec changes only this YDB driver's allocation policy.
// Keep stock protobuf semantics and adaptation; do not register it globally.
// This is not a bound on SQL encoding, decoded fields or whole-phase memory.
type exactCapacityProtobufCodec struct{}

func (exactCapacityProtobufCodec) Name() string { return grpcproto.Name }

func (exactCapacityProtobufCodec) Marshal(value any) (mem.BufferSlice, error) {
	message := protobufMessageV2(value)
	if message == nil {
		return nil, fmt.Errorf("proto: failed to marshal, message is %T, want proto.Message", value)
	}
	// As in the stock codec, Size/UseCachedSize require an immutable message.
	// Size can overestimate lazy nonminimal input; retain the actual result,
	// not the entire allocation, and do not require len(encoded) == size.
	size := proto.Size(message)
	owned := make([]byte, 0, size)
	encoded, err := (proto.MarshalOptions{UseCachedSize: true}).MarshalAppend(owned, message)
	if err != nil {
		// Only this allocation is known to be ours. Do not assume a custom
		// protobuf implementation's partial return is an owned buffer.
		clear(owned[:cap(owned)])
		return nil, err
	}
	// Immutable generated YDB messages fit the Size allocation. A custom or
	// concurrently mutated message is not a prospective-capacity proof. Once
	// handed off, transport/retry references own its lifetime: Free on a
	// SliceBuffer does not erase it and this codec must not clear it early.
	return mem.BufferSlice{mem.SliceBuffer(encoded)}, nil
}

func (exactCapacityProtobufCodec) Unmarshal(data mem.BufferSlice, value any) error {
	message := protobufMessageV2(value)
	if message == nil {
		return fmt.Errorf("failed to unmarshal, message is %T, want proto.Message", value)
	}
	// One fragment is borrowed with another reference; multiple fragments
	// require an exact-sized concatenation. Neither case permits clearing the
	// borrowed input. Standard Unmarshal retains its reset/unknown/error rules.
	buffer := data.MaterializeToBuffer(mem.NopBufferPool{})
	defer buffer.Free()
	return proto.Unmarshal(buffer.ReadOnlyData(), message)
}

func protobufMessageV2(value any) proto.Message {
	switch message := value.(type) {
	case protoadapt.MessageV1:
		return protoadapt.MessageV2Of(message)
	case protoadapt.MessageV2:
		return message
	default:
		return nil
	}
}
