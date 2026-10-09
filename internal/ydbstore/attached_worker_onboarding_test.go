package ydbstore

import (
	"bytes"
	"gitcode.com/urandon/sessionless/internal/domain"
	"testing"
	"time"
)

func TestOnboardingWorkerComparisonRejectsEveryChangedAuthority(t *testing.T) {
	at := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	w := domain.AttachedWorker{TenantID: "tenant", OwnerUserID: "owner", ID: "worker", DisplayName: "laptop", IdentityPublicKey: bytes.Repeat([]byte{1}, 32), EnrollmentGeneration: 1, ConnectionGeneration: 0, Revision: 1, DesiredState: domain.AttachedWorkerDesiredActive, ObservedState: domain.AttachedWorkerObservedOffline, CreatedAt: at, UpdatedAt: at}
	if !sameOnboardingWorker(w, w) {
		t.Fatal("same worker did not compare equal")
	}
	for _, test := range []struct {
		name   string
		change func(*domain.AttachedWorker)
	}{
		{name: "tenant", change: func(w *domain.AttachedWorker) { w.TenantID = "other" }},
		{name: "owner", change: func(w *domain.AttachedWorker) { w.OwnerUserID = "other" }},
		{name: "worker", change: func(w *domain.AttachedWorker) { w.ID = "other" }},
		{name: "name", change: func(w *domain.AttachedWorker) { w.DisplayName = "other" }},
		{name: "key", change: func(w *domain.AttachedWorker) { w.IdentityPublicKey = bytes.Repeat([]byte{2}, 32) }},
		{name: "enrollment", change: func(w *domain.AttachedWorker) { w.EnrollmentGeneration++ }},
		{name: "connection", change: func(w *domain.AttachedWorker) { w.ConnectionGeneration++ }},
		{name: "revision", change: func(w *domain.AttachedWorker) { w.Revision++ }},
		{name: "desired", change: func(w *domain.AttachedWorker) { w.DesiredState = domain.AttachedWorkerDesiredDrain }},
		{name: "observed", change: func(w *domain.AttachedWorker) { w.ObservedState = domain.AttachedWorkerObservedOnline }},
		{name: "created", change: func(w *domain.AttachedWorker) { w.CreatedAt = at.Add(time.Second) }},
		{name: "updated", change: func(w *domain.AttachedWorker) { w.UpdatedAt = at.Add(time.Second) }},
		{name: "revoked", change: func(w *domain.AttachedWorker) { w.RevokedAt = at.Add(time.Second) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			changed := w
			test.change(&changed)
			if sameOnboardingWorker(w, changed) {
				t.Fatalf("changed %s accepted as same authority", test.name)
			}
		})
	}
}
