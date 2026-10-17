package ydbstore

import (
	"context"
	"reflect"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// This is only the existing publisher's exact-key storage mechanism. The
// publication verifier must authenticate a genuinely minted installation and
// source; a valid DTO or pair of receipts cannot establish that provenance.
// The default Store has no such producer and denies before reaching this code.
func publishLocalProfileInstallationTx(ctx context.Context, q *ownedAuthorityQuery, grant ports.OwnedProfilePublicationGrantV1, successorLocator, intentDigest string) (ports.OwnedProfilePublicationReceiptV1, error) {
	denied := func() (ports.OwnedProfilePublicationReceiptV1, error) {
		return ports.OwnedProfilePublicationReceiptV1{}, domain.ErrMembershipDenied
	}
	installation := grant.LocalInstallation
	if installation == nil || installation.ValidateForProfile(grant.Profile) != nil {
		return denied()
	}
	next := grant.Profile.Clone()
	next.UpdatedAt, next.Window.PublishedAt = q.at, q.at
	if next.ValidateInitial() != nil || next.ValidateAt(q.at) != nil {
		return denied()
	}
	priorNext, nextAudited, err := readOwnedProfilePublicationAudit(ctx, q, grant, successorLocator, intentDigest)
	if err != nil {
		return denied()
	}
	if installation.Predecessor == nil {
		head, exists, err := loadOwnedProfileProjection(ctx, q, next.Locator)
		if err != nil {
			return denied()
		}
		if nextAudited {
			if !exists || !localProfileHistoricalHeadMatches(priorNext, head) {
				return denied()
			}
			return priorNext, nil // exact historical receipt, no head writes
		}
		if exists {
			return denied() // never overwrite an existing head or tombstone
		}
		return writeOwnedProfilePublicationTx(ctx, q, next, successorLocator, intentDigest)
	}
	predecessor := installation.Predecessor
	retired := localProfileRetirementTemplate(predecessor.Profile, installation.RetirementMutationID, grant.IssuedAt)
	predecessorLocator, err := predecessor.Profile.Locator.Digest()
	if err != nil {
		return denied()
	}
	priorOld, oldAudited, err := readOwnedProfilePublicationAuditTemplate(ctx, q, retired, grant.IssuedAt, predecessorLocator, intentDigest)
	if err != nil || nextAudited != oldAudited {
		return denied() // partial/mismatched immutable intent never recovers success
	}
	oldHead, oldExists, err := loadOwnedProfileProjection(ctx, q, predecessor.Profile.Locator)
	if err != nil || !oldExists {
		return denied()
	}
	newHead, newExists, err := loadOwnedProfileProjection(ctx, q, next.Locator)
	if err != nil {
		return denied()
	}
	if nextAudited {
		oldDigest, digestErr := oldHead.Digest()
		if digestErr != nil || oldHead.State != domain.OwnedAgentCurrentRevokedV1 || oldDigest != priorOld.Digest || !priorOld.Profile.UpdatedAt.Equal(priorNext.Profile.UpdatedAt) || !newExists || !localProfileHistoricalHeadMatches(priorNext, newHead) {
			return denied()
		}
		return priorNext, nil // both exact audits and retained heads, no replay write
	}
	oldDigest, digestErr := oldHead.Digest()
	if newExists || digestErr != nil || oldDigest != predecessor.Digest || oldHead.RevocationGeneration != predecessor.Profile.RevocationGeneration || oldHead.ValidateAt(q.at) != nil {
		return denied()
	}
	retired = localProfileRetirementTemplate(oldHead, installation.RetirementMutationID, q.at)
	if retired.ValidateTransitionFrom(oldHead) != nil {
		return denied()
	}
	// Serializable reads above arbitrate both competing successor locators through
	// the same predecessor. Both heads and both immutable audits share this commit.
	if _, err := writeOwnedProfilePublicationTx(ctx, q, retired, predecessorLocator, intentDigest); err != nil {
		return ports.OwnedProfilePublicationReceiptV1{}, err
	}
	return writeOwnedProfilePublicationTx(ctx, q, next, successorLocator, intentDigest)
}

func localProfileRetirementTemplate(old domain.OwnedAgentProfileCurrentV1, mutation string, at time.Time) domain.OwnedAgentProfileCurrentV1 {
	retired := old.Clone()
	retired.State = domain.OwnedAgentCurrentRevokedV1
	retired.RevocationGeneration++ // grant validation bounds predecessor generation
	retired.UpdatedAt, retired.MutationID = at, mutation
	return retired
}

// A later legitimate replacement can retire this successor. Historical recovery
// may return its original receipt, but must still find the unchanged installed
// package or its retained revoked tombstone. It can never return a current grant
// or write the historical package back into the head.
func localProfileHistoricalHeadMatches(receipt ports.OwnedProfilePublicationReceiptV1, head domain.OwnedAgentProfileCurrentV1) bool {
	original := receipt.Profile
	if head.Locator != original.Locator || head.RevocationGeneration < original.RevocationGeneration || head.UpdatedAt.Before(original.UpdatedAt) {
		return false
	}
	if head.RevocationGeneration == original.RevocationGeneration {
		digest, err := head.Digest()
		return err == nil && digest == receipt.Digest
	}
	if head.State != domain.OwnedAgentCurrentRevokedV1 || head.MutationID == original.MutationID {
		return false
	}
	expected := original.Clone()
	expected.State, expected.RevocationGeneration, expected.UpdatedAt, expected.MutationID = head.State, head.RevocationGeneration, head.UpdatedAt, head.MutationID
	return reflect.DeepEqual(expected, head)
}
