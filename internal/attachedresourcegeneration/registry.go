// Package attachedresourcegeneration owns immutable, operator-installed
// generation baselines. Configuration is DATA, never execution or entitlement
// authority. Serving callers must still join current authenticated state.
package attachedresourcegeneration

import (
	"encoding/json"
	"errors"

	"gitcode.com/urandon/sessionless/internal/domain"
)

const (
	MaxBaselinesV1   = 64
	MaxConfigBytesV1 = 1 << 20
)

// ErrInvalidConfig is content-free: callers must not log configuration or
// wrap raw file paths, JSON bodies, or underlying decoder errors.
var ErrInvalidConfig = errors.New("attached resource generation configuration is invalid")

type resourceKey struct {
	tenant   domain.TenantID
	owner    domain.UserID
	resource domain.SubscriptionConnectionID
}

type workerKey struct {
	tenant domain.TenantID
	owner  domain.UserID
	worker domain.AttachedWorkerID
}

// Registry has no setters or reload operation. Its constructor owns all
// nested values and every returned baseline is an independent clone.
type Registry struct {
	resources map[resourceKey]domain.AttachedResourceGenerationBaselineV1
	workers   map[workerKey]struct{}
}

func NewRegistry(baselines []domain.AttachedResourceGenerationBaselineV1) (*Registry, error) {
	if len(baselines) == 0 || len(baselines) > MaxBaselinesV1 {
		return nil, ErrInvalidConfig
	}
	registry := &Registry{resources: make(map[resourceKey]domain.AttachedResourceGenerationBaselineV1, len(baselines)), workers: make(map[workerKey]struct{}, len(baselines))}
	owned := make([]domain.AttachedResourceGenerationBaselineV1, 0, len(baselines))
	for _, baseline := range baselines {
		if baseline.Validate() != nil {
			return nil, ErrInvalidConfig
		}
		resource := resourceKey{baseline.TenantID, baseline.OwnerUserID, baseline.SubscriptionConnectionID}
		worker := workerKey{baseline.TenantID, baseline.OwnerUserID, baseline.WorkerID}
		if _, found := registry.resources[resource]; found {
			return nil, ErrInvalidConfig
		}
		if _, found := registry.workers[worker]; found {
			return nil, ErrInvalidConfig
		}
		baseline = baseline.Clone()
		registry.resources[resource], registry.workers[worker] = baseline, struct{}{}
		owned = append(owned, baseline)
	}
	encoded, err := json.Marshal(registryConfigV1{Version: 1, Baselines: owned})
	if err != nil || len(encoded) > MaxConfigBytesV1 {
		return nil, ErrInvalidConfig
	}
	return registry, nil
}

func (registry *Registry) Lookup(tenant domain.TenantID, owner domain.UserID, resource domain.SubscriptionConnectionID) (domain.AttachedResourceGenerationBaselineV1, bool) {
	if registry == nil {
		return domain.AttachedResourceGenerationBaselineV1{}, false
	}
	baseline, found := registry.resources[resourceKey{tenant, owner, resource}]
	if !found {
		return domain.AttachedResourceGenerationBaselineV1{}, false
	}
	return baseline.Clone(), true
}

func (registry *Registry) ContainsWorker(tenant domain.TenantID, owner domain.UserID, worker domain.AttachedWorkerID) bool {
	if registry == nil {
		return false
	}
	_, found := registry.workers[workerKey{tenant, owner, worker}]
	return found
}

func (registry *Registry) Len() int {
	if registry == nil {
		return 0
	}
	return len(registry.resources)
}
