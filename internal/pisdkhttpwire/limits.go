// Package pisdkhttpwire implements SPU3 byte structure only. No value in this
// package proves file custody, native fit, Source permission or model use.
// Callers must prospectively fund buffers/work on the original ledger before
// invoking these helpers and retain borrowed views until their consumers join.
package pisdkhttpwire

import "errors"

var ErrInvalid = errors.New("invalid Pi HTTP wire data")

const HeaderBytes = 17

type Limits struct {
	FrameBytes, InitializeBytes, RequestBytes, FinalBytes uint64
	RequestHeaderBytes, ResponseHeaderBytes               uint64
	ResponseBytes, ResponseChunkBytes                     uint64
	ParentBytes, ChildBytes, Frames                       uint64
}

type Accounting struct {
	ParentBytes, ChildBytes, TotalBytes, Frames, Chunks, ChunkBytes uint64
}

// Measure reserves maximum wire appearances, not observed successful lengths.
// It is NOT a copy/live/node/HTTP/native allocation certificate.
func Measure(l Limits) (Accounting, error) {
	if l.FrameBytes < 114 || l.FrameBytes > 1<<20 || l.ParentBytes == 0 || l.ParentBytes > 4<<20 ||
		l.ChildBytes == 0 || l.ChildBytes > 16<<20 || l.Frames == 0 || l.Frames > 4096 ||
		l.InitializeBytes == 0 || l.InitializeBytes > l.FrameBytes-113 ||
		l.RequestHeaderBytes < 2 || l.RequestHeaderBytes > l.FrameBytes-85 ||
		l.RequestBytes == 0 || l.RequestBytes > l.FrameBytes-85-l.RequestHeaderBytes ||
		l.ResponseHeaderBytes < 2 || l.ResponseHeaderBytes > l.FrameBytes-63 ||
		l.FinalBytes == 0 || l.FinalBytes > 128<<10 || l.FinalBytes > l.FrameBytes-17 ||
		l.ResponseBytes == 0 || l.ResponseBytes > l.ParentBytes ||
		l.ResponseChunkBytes == 0 || l.ResponseChunkBytes > l.ResponseBytes {
		return Accounting{}, ErrInvalid
	}
	c := min(l.ResponseChunkBytes, l.FrameBytes-21)
	n := 1 + (l.ResponseBytes-1)/c
	// Every term is now bounded by inherited ceilings, so additions cannot
	// overflow uint64 (or a supported host int). Never add unchecked inputs.
	a := Accounting{ChunkBytes: c, Chunks: n, Frames: 6 + 2*n,
		ParentBytes: l.InitializeBytes + l.ResponseBytes + l.ResponseHeaderBytes + 254 + 21*n,
		ChildBytes:  l.RequestBytes + l.FinalBytes + l.RequestHeaderBytes + 102 + 21*n}
	a.TotalBytes = a.ParentBytes + a.ChildBytes
	if a.ParentBytes > l.ParentBytes || a.ChildBytes > l.ChildBytes || a.Frames > l.Frames {
		return Accounting{}, ErrInvalid
	}
	return a, nil
}
