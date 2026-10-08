//go:build ydbintegration

package attachedworkerdaemontransport

import "time"

// NewTestProviderMaterializer is compiled only for the YDB integration gate.
// The shipped constructor remains credentialless and rejects provider jobs.
func NewTestProviderMaterializer(source SealedInputSource, maxBytes int, now func() time.Time) (*BoundMaterializer, error) {
	materializer, err := NewBoundMaterializer(source, maxBytes, now)
	if err != nil {
		return nil, err
	}
	materializer.allowTestProvider = true
	return materializer, nil
}
