//go:build ydbintegration

package attachedworkersealedinput

import "time"

// NewTestProviderService is available only in the YDB integration test build.
// Production binaries always use NewService and cannot admit provider input.

func NewTestProviderService(authorizer Authorizer, jobs JobStore, blobs BlobStore, now func() time.Time) (*Service, error) {
	if now == nil {
		return nil, ErrInvalid
	}
	service, err := NewService(authorizer, jobs, blobs)
	if err != nil {
		return nil, err
	}
	service.allowTestProvider = true
	service.testProviderNow = now
	return service, nil
}
