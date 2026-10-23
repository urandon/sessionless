package mcpwire

import (
	"bytes"
	"mime"
	"strings"
)

// DecodeResponseFrameV1 unwraps finite, already read-bounded JSON or SSE DATA.
// Exactly one terminal message is accepted. It has no retry, stream resumption,
// unsolicited request, callback or session authority. Each SSE record, including
// comments, counts against the original invocation's inbound-frame budget.
func DecodeResponseFrameV1(body []byte, contentType string, limits LimitsV1) ([]byte, uint64, error) {
	if limits.Validate() != nil || len(body) == 0 || uint64(len(body)) > limits.MessageBytes {
		return nil, 0, ErrProtocol
	}
	media, params, err := mime.ParseMediaType(contentType)
	if err != nil || len(params) > 1 || (len(params) == 1 && !strings.EqualFold(params["charset"], "utf-8")) {
		return nil, 0, ErrProtocol
	}
	switch media {
	case "application/json":
		if _, err := parse(body, limits.MessageBytes, limits); err != nil {
			return nil, 0, ErrProtocol
		}
		return bytes.Clone(body), 1, nil
	case "text/event-stream":
	default:
		return nil, 0, ErrProtocol
	}
	var message, data []byte
	var frames uint64
	hasRecord, hasData, event := false, false, false
	finish := func() bool {
		if !hasRecord {
			return true
		}
		frames++
		if frames > limits.SSEEvents {
			return false
		}
		if hasData {
			if message != nil || len(data) == 0 {
				return false
			}
			if data[len(data)-1] == '\n' {
				data = data[:len(data)-1]
			}
			if _, err := parse(data, limits.SSEEventBytes, limits); err != nil {
				return false
			}
			message = bytes.Clone(data)
		} else if event {
			return false
		}
		clear(data)
		data = nil
		hasRecord, hasData, event = false, false, false
		return true
	}
	for remaining := body; len(remaining) != 0; {
		line, rest, found := bytes.Cut(remaining, []byte{'\n'})
		// Require terminated records; EOF is not a synthetic stream delimiter.
		if !found || uint64(len(line)) > limits.SSELineBytes {
			clear(message)
			clear(data)
			return nil, 0, ErrProtocol
		}
		remaining = rest
		line = bytes.TrimSuffix(line, []byte{'\r'})
		if bytes.ContainsRune(line, '\r') {
			clear(message)
			clear(data)
			return nil, 0, ErrProtocol
		}
		if len(line) == 0 {
			if !finish() {
				clear(message)
				clear(data)
				return nil, 0, ErrProtocol
			}
			continue
		}
		hasRecord = true
		if line[0] == ':' {
			continue
		}
		name, value, _ := bytes.Cut(line, []byte{':'})
		value = bytes.TrimPrefix(value, []byte{' '})
		switch string(name) {
		case "event":
			if event || !bytes.Equal(value, []byte("message")) {
				clear(message)
				clear(data)
				return nil, 0, ErrProtocol
			}
			event = true
		case "data":
			if uint64(len(data)+len(value)+1) > limits.SSEEventBytes {
				clear(message)
				clear(data)
				return nil, 0, ErrProtocol
			}
			data = append(data, value...)
			data = append(data, '\n')
			hasData = true
		default:
			// Includes id/retry: resumption is not implemented by this profile.
			clear(message)
			clear(data)
			return nil, 0, ErrProtocol
		}
	}
	if hasRecord || message == nil {
		clear(message)
		clear(data)
		return nil, 0, ErrProtocol
	}
	return message, frames, nil
}
