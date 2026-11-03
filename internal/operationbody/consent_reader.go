package operationbody

import (
	"context"
	"io"
	"reflect"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

type bodyOpenerV1 interface {
	Open(context.Context, domain.TenantID, domain.BlobRef) (io.ReadCloser, error)
}

// ConsentArgumentReaderV1 verifies only immutable DATA for the private BFF.
// The Store selects the authenticated original intent before I/O and rejoins
// current authority afterwards. This reader is neither that authorizer nor
// the original worker's write, uncertainty, inventory or erasure owner.
type ConsentArgumentReaderV1 struct{ opener bodyOpenerV1 }

var _ ports.AttemptOperationBodyStoreV1 = (*ConsentArgumentReaderV1)(nil)

func NewConsentArgumentReaderV1(opener bodyOpenerV1) (*ConsentArgumentReaderV1, error) {
	if opener == nil {
		return nil, ErrBody
	}
	v := reflect.ValueOf(opener)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		if v.IsNil() {
			return nil, ErrBody
		}
	}
	return &ConsentArgumentReaderV1{opener: opener}, nil
}

func (r *ConsentArgumentReaderV1) ReadAttemptOperationBody(ctx context.Context, scope domain.AttemptOperationScopeV1, value string, cap uint64) ([]byte, error) {
	if r == nil || r.opener == nil || ctx == nil || ctx.Err() != nil || cap == 0 || cap > domain.WebSearchArgumentsBytesV1 {
		return nil, ErrBody
	}
	// This exact parser excludes ordinary relative references and all grant,
	// arbitrary historical and foreign-scope keys before storage is touched.
	ref, err := domain.ParseAttemptOperationConsentArgumentsRefV1(scope, value, cap)
	if err != nil {
		return nil, ErrBody
	}
	call, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return readVerifiedBody(call, r.opener, scope.TenantID, ref)
}

func (*ConsentArgumentReaderV1) String() string     { return "ConsentArgumentReaderV1{private:[redacted]}" }
func (r *ConsentArgumentReaderV1) GoString() string { return r.String() }
