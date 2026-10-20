package mcpwire

import "bytes"

// ParseDescriptorV1 restores complete canonical descriptor DATA retained by an
// installation. It grants neither schema support nor permission to call. The
// owning admission code must approve every schema keyword separately.
func ParseDescriptorV1(body []byte, limits LimitsV1) (DescriptorV1, error) {
	if limits.Validate() != nil {
		return DescriptorV1{}, ErrProtocol
	}
	v, err := parse(body, limits.DescriptorBytes, limits)
	if err != nil {
		return DescriptorV1{}, ErrProtocol
	}
	d, err := descriptor(v, limits)
	if err != nil || d.taskRequired || !bytes.Equal(d.body, body) {
		return DescriptorV1{}, ErrProtocol
	}
	return d, nil
}
