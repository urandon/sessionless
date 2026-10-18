package providerloopsession

import (
	"errors"
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/configuredinference"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

func TestLocalPreparationErrorRetainsOnlyClosedCapacityClassification(t *testing.T) {
	private := errors.New("private storage diagnostic")
	for _, tc := range []struct {
		name     string
		cause    error
		capacity bool
	}{
		{name: "authority_error", cause: private},
		{name: "context_error", cause: configuredinference.ErrContextNoFit},
		{name: "capacity_error", cause: ydbstore.ErrTerminalCapacity, capacity: true},
		{name: "wrapped_capacity", cause: errors.Join(private, ydbstore.ErrTerminalCapacity), capacity: true},
		{name: "mixed_classification", cause: errors.Join(configuredinference.ErrContextNoFit, ydbstore.ErrTerminalCapacity), capacity: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, base := range []error{ErrSession, ErrIncomplete} {
				got := localPreparationError(base, tc.cause)
				if !errors.Is(got, base) || errors.Is(got, ydbstore.ErrTerminalCapacity) != tc.capacity {
					t.Errorf("base=%v: got %v, want capacity=%t and original public classification", base, got, tc.capacity)
				}
				if errors.Is(got, private) || errors.Is(got, configuredinference.ErrContextNoFit) || strings.Contains(got.Error(), private.Error()) {
					t.Errorf("base=%v: leaked private cause or promoted context overflow: %v", base, got)
				}
			}
		})
	}
}
