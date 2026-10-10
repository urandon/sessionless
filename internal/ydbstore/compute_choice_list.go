package ydbstore

import (
	"context"
	"database/sql"
	"time"

	"gitcode.com/urandon/sessionless/internal/computechoice"
	"gitcode.com/urandon/sessionless/internal/domain"
)

type computeChoiceAttachedRegistration struct {
	enumeration computechoice.Registration
	pin         computeChoiceAttachedPin
}

type computeChoiceCatalogKey struct {
	tenant domain.TenantID
	owner  domain.UserID
	choice string
}

// An immutable source-reader catalog, NOT the full approved eligibility
// manifest. It cannot construct HarnessBinding, consent, price or a public
// eligible choice. The cutover pins are trusted deployment inputs, never a hash
// invented from this enumeration. No route consumes this private groundwork.
type computeChoiceAttachedCatalog struct {
	registry *computechoice.Registry
	bindings map[computeChoiceCatalogKey]computeChoiceAttachedPin
	cutover  computeChoiceCutoverPins
	cursor   *computechoice.CursorCodec
}

func newComputeChoiceAttachedCatalog(cutover computeChoiceCutoverPins, cursor *computechoice.CursorCodec, entries []computeChoiceAttachedRegistration) (*computeChoiceAttachedCatalog, error) {
	if !inventoryDigestPattern.MatchString(cutover.manifestDigest) || !fullCommitPattern.MatchString(cutover.writerCommit) || cursor == nil || len(entries) > computechoice.MaxRegistrations {
		return nil, errComputeChoiceSource
	}
	registrations := make([]computechoice.Registration, 0, len(entries))
	bindings := make(map[computeChoiceCatalogKey]computeChoiceAttachedPin, len(entries))
	for _, entry := range entries {
		pin := entry.pin
		registration := entry.enumeration
		if pin.tenantID != registration.TenantID || pin.ownerUserID != registration.OwnerUserID || pin.subscriptionID.Validate() != nil || pin.actorID.Validate() != nil || pin.workerID.Validate() != nil || pin.connectionID.Validate() != nil || !computeChoiceAttachedProviderValid(pin.provider) || pin.enrollmentGeneration == 0 || pin.connectionGeneration == 0 || pin.workerRevision == 0 || pin.connectionRevision == 0 || pin.capabilityDigest.Validate() != nil {
			return nil, errComputeChoiceSource
		}
		key := computeChoiceCatalogKey{registration.TenantID, registration.OwnerUserID, registration.ChoiceID}
		if _, exists := bindings[key]; exists {
			return nil, errComputeChoiceSource
		}
		bindings[key] = pin
		registrations = append(registrations, registration)
	}
	registry, err := computechoice.NewRegistry(registrations)
	if err != nil {
		return nil, errComputeChoiceSource
	}
	return &computeChoiceAttachedCatalog{registry: registry, bindings: bindings, cutover: cutover, cursor: cursor}, nil
}

type computeChoiceAttachedCandidate struct {
	registration computechoice.Registration
	sources      computeChoiceAttachedSnapshot
}

type computeChoiceAttachedPage struct {
	rateLimited      bool
	retryAfter       time.Duration
	readAt           time.Time
	throughSequence  uint64
	registryRevision string
	candidates       []computeChoiceAttachedCandidate
	lastPosition     uint32
	more             bool
}

func (computeChoiceAttachedPage) MarshalJSON() ([]byte, error) {
	return nil, computechoice.ErrPrivateRegistrationNotSerializable
}

// This PRIVATE, unmounted operation joins the real authTx/current readers/rate
// debit and six-source candidate graph. Nothing it returns establishes complete
// eligibility: reviewed model/route/price/PRIVATE-context evidence and eventual
// closed public DTO, shortest-authority cursor issuance, send/admission and
// receipt lifecycle integration remain mandatory. Never export it as permission
// for a later transaction. No public next_cursor or selector is minted here.
func (store *Store) readRatedComputeChoiceAttachedPage(ctx context.Context, digest domain.SecretDigest, sessionID domain.SessionID, input computechoice.ListQuery, serverRequestID string, catalog *computeChoiceAttachedCatalog) (computeChoiceAttachedPage, error) {
	if input.Validate() != nil {
		return computeChoiceAttachedPage{}, computechoice.ErrInvalidQuery
	}
	if digest.Validate("web_session.session_digest") != nil || sessionID.Validate() != nil || domain.ValidateOpaqueID("request_id", serverRequestID) != nil || catalog == nil || catalog.registry == nil || catalog.cursor == nil {
		return computeChoiceAttachedPage{}, errComputeChoiceSource
	}
	ctx, cancel := context.WithTimeout(ctx, computechoice.ResourceDeadline)
	defer cancel()
	budget := computechoice.NewResourceBudget()
	var pending computeChoiceAttachedPage
	err := store.authTx(ctx, "compute_choice.attached_sources_v1", func(tx *sql.Tx) error {
		// The same budget survives SDK retries; speculative output does not.
		pending = computeChoiceAttachedPage{}
		attempt, err := budget.NewAttempt()
		if err != nil {
			return err
		}
		defer attempt.Finish()
		query := &computeChoiceQuery{tx: tx, budget: attempt}
		auth, err := readComputeChoiceAuth(ctx, query, digest, sessionID)
		if err != nil {
			return err
		}
		if err := readComputeChoiceCutover(ctx, query, catalog.cutover, auth.at); err != nil {
			return err
		}
		decision, err := readComputeChoiceListRate(ctx, query, auth, input, serverRequestID)
		if err != nil {
			return err
		}
		if !decision.Allowed {
			pending = computeChoiceAttachedPage{rateLimited: true, retryAfter: decision.RetryAfter}
			return nil
		}
		// Owner filtering precedes positions/hashing and every candidate lookup.
		scoped, err := catalog.registry.Scoped(auth.scope)
		if err != nil {
			return errComputeChoiceSource
		}
		var after uint32
		if input.Cursor != "" {
			after, err = catalog.cursor.Decode(input.Cursor, auth.scope, scoped, input.InputKind, auth.at, auth.authorityExpiresAt)
			if err != nil {
				return err
			}
		}
		page, err := scoped.Page(after, input.Limit)
		if err != nil {
			return errComputeChoiceSource
		}
		result := computeChoiceAttachedPage{readAt: auth.at, throughSequence: auth.lastEventSequence, registryRevision: scoped.Revision(), lastPosition: page.LastPosition, more: page.More, candidates: make([]computeChoiceAttachedCandidate, 0, len(page.Registrations))}
		for _, registration := range page.Registrations {
			pin, exists := catalog.bindings[computeChoiceCatalogKey{auth.scope.TenantID, auth.scope.UserID, registration.ChoiceID}]
			if !exists {
				return errComputeChoiceSource
			}
			sources, err := readComputeChoiceAttached(ctx, query, auth, pin)
			if err != nil {
				return err
			}
			result.candidates = append(result.candidates, computeChoiceAttachedCandidate{registration: registration, sources: sources})
		}
		pending = result
		return nil
	})
	if err != nil {
		return computeChoiceAttachedPage{}, err
	}
	return pending, nil
}
