package ydbstore

import (
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/operationbody"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Explicit closed writer-version dispatch, never "try V3 then fall back V2".
// Two installed producers, an absent producer or a version mismatch are denied.
func (p *LocalPreparedOutputV1) accountingRows() ([]ports.OwnedOperationResidualRowV1, error) {
	if !p.original() {
		return nil, domain.ErrMembershipDenied
	}
	switch p.capsuleVersion {
	case 1:
		if p.accounting != nil && p.accountingV3 == nil && p.accountingV4 == nil && p.accounting.historicalValid() {
			return p.accounting.operations, nil
		}
	case 2:
		if p.accounting == nil && p.accountingV3 != nil && p.accountingV4 == nil && p.accountingV3.historicalValid() {
			return p.accountingV3.operations, nil
		}
	case 3:
		if p.accounting == nil && p.accountingV3 == nil && p.accountingV4 != nil && p.accountingV4.historicalValid() {
			return p.accountingV4.operations, nil
		}
	}
	return nil, domain.ErrMembershipDenied
}

func (p *LocalPreparedOutputV1) accountingDigest() (string, error) {
	if _, err := p.accountingRows(); err != nil {
		return "", err
	}
	if p.capsuleVersion == 1 {
		return p.accounting.Digest()
	}
	if p.capsuleVersion == 3 {
		return p.accountingV4.Digest()
	}
	return p.accountingV3.Digest()
}

func (p *LocalPreparedOutputV1) residualDigest(retired domain.RegisteredAllocationV2, erased *operationbody.NamespaceErasureV1) (string, error) {
	if _, err := p.accountingRows(); err != nil {
		return "", err
	}
	if p.capsuleVersion == 1 {
		v, err := p.accounting.Residual(retired, erased)
		if err != nil {
			return "", err
		}
		return v.Digest()
	}
	if p.capsuleVersion == 3 {
		v, err := p.accountingV4.Residual(retired, erased)
		if err != nil {
			return "", err
		}
		return v.Digest()
	}
	v, err := p.accountingV3.Residual(retired, erased)
	if err != nil {
		return "", err
	}
	return v.Digest()
}
