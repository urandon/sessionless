package operationbody

import "gitcode.com/urandon/sessionless/internal/domain"

// Bodies returns immutable DATA copies of every ordinary/consent key attempted by this
// original adapter before its successful fence/erasure. It includes unreserved
// writes, excludes retained/output objects, and grants no deletion authority.
func (r *NamespaceErasureV1) Bodies() ([]domain.BlobRef, error) {
	if !r.original() {
		return nil, ErrBody
	}
	st := r.state
	select {
	case st.active <- struct{}{}:
	default:
		return nil, ErrBusy
	}
	defer func() { <-st.active }()
	n := &st.namespace
	if !n.fenced || !n.eraseAttempted || n.receipt != r || n.count > 24 || n.count != r.deletedBodies {
		return nil, ErrBody
	}
	return append([]domain.BlobRef(nil), n.bodies[:n.count]...), nil
}
