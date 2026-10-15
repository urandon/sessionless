package webbff

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/webcontract"
)

type AttachedPiAPIOwnerPolicyService interface {
	Read(context.Context, ports.ReadAttachedPiAPIPolicyV1) (ports.AttachedPiAPIPolicyOfferV1, error)
	Confirm(context.Context, ports.ConfirmAttachedPiAPIPolicyV1) (ports.AttachedPiAPIPolicyDecisionV1, error)
	Revoke(context.Context, ports.RevokeAttachedPiAPIPolicyV1) error
}

// Closed flat selectors only; duplicate/unknown/nested/null fields and trailing
// JSON reject before the service. This operation never accepts R/M/P/E JSON.
func decodePiPolicyRequest(r *http.Request, target any) error {
	// encoding/json's field matching is case-insensitive. Reject aliases at
	// the token boundary against this exact operation's canonical names.
	allowed := [8]string{"version", "resource_id", "model_vendor_id", "model_id", "input_data_class"}
	required := 5
	switch value := target.(type) {
	case *webcontract.AttachedPiAPIPolicySelectorV1:
		if value == nil {
			return domain.ErrMembershipDenied
		}
	case *webcontract.AttachedPiAPIPolicyConfirmationV1:
		if value == nil {
			return domain.ErrMembershipDenied
		}
		allowed[5], allowed[6], allowed[7] = "terms_digest", "expected_generation", "mutation_id"
		required = 8
	case *webcontract.AttachedPiAPIPolicyRevocationV1:
		if value == nil {
			return domain.ErrMembershipDenied
		}
		allowed[5], allowed[6] = "expected_generation", "expected_mapping"
		required = 7
	default:
		return domain.ErrMembershipDenied
	}
	if r.Body == nil || r.URL.RawQuery != "" || r.URL.ForceQuery || len(r.Header.Values("Content-Type")) != 1 || r.Header.Get("Content-Type") != "application/json" || len(r.Header.Values("Content-Encoding")) != 0 || r.ContentLength > 4096 {
		return domain.ErrMembershipDenied
	}
	defer r.Body.Close()
	var arena [4097]byte
	n, err := io.ReadFull(r.Body, arena[:])
	if n == 0 || n > 4096 || err != io.ErrUnexpectedEOF {
		return domain.ErrMembershipDenied
	}
	d := json.NewDecoder(bytes.NewReader(arena[:n]))
	d.UseNumber()
	first, err := d.Token()
	if err != nil || first != json.Delim('{') {
		return domain.ErrMembershipDenied
	}
	seen := make(map[string]bool, 8)
	for d.More() {
		key, err := d.Token()
		name, ok := key.(string)
		exact := false
		for _, canonical := range allowed[:required] {
			if name == canonical {
				exact = true
				break
			}
		}
		if err != nil || !ok || !exact || seen[name] || len(seen) >= required {
			return domain.ErrMembershipDenied
		}
		seen[name] = true
		value, err := d.Token()
		if err != nil || value == nil {
			return domain.ErrMembershipDenied
		}
		if _, nested := value.(json.Delim); nested {
			return domain.ErrMembershipDenied
		}
	}
	last, err := d.Token()
	if err != nil || last != json.Delim('}') {
		return domain.ErrMembershipDenied
	}
	if _, err := d.Token(); err != io.EOF {
		return domain.ErrMembershipDenied
	}
	if len(seen) != required {
		return domain.ErrMembershipDenied
	}
	d = json.NewDecoder(bytes.NewReader(arena[:n]))
	d.DisallowUnknownFields()
	if d.Decode(target) != nil {
		return domain.ErrMembershipDenied
	}
	return nil
}

func piPolicySelector(a ports.WebAuthorization, s webcontract.AttachedPiAPIPolicySelectorV1) (ports.ReadAttachedPiAPIPolicyV1, error) {
	l := ports.AttachedPiAPIPolicyLocatorV1{TenantID: a.Session.ActiveTenantID, OwnerUserID: a.Session.UserID, ResourceID: s.ResourceID, ModelVendorID: s.ModelVendorID, ModelID: s.ModelID, InputDataClass: s.InputDataClass}
	if s.Version != 1 || l.Validate() != nil {
		return ports.ReadAttachedPiAPIPolicyV1{}, domain.ErrMembershipDenied
	}
	return ports.ReadAttachedPiAPIPolicyV1{Locator: l, WebSessionDigest: a.Session.SessionDigest}, nil
}
func (h *Handler) readAttachedPiAPIPolicy(w http.ResponseWriter, r *http.Request) {
	a, err := h.authorizeMutation(r, domain.TenantPermissionWrite)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	var input webcontract.AttachedPiAPIPolicySelectorV1
	if decodePiPolicyRequest(r, &input) != nil {
		h.writeError(w, r, domain.ErrMembershipDenied)
		return
	}
	request, err := piPolicySelector(a, input)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	result, err := h.config.AttachedPiAPIPolicies.Read(r.Context(), request)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	h.writeBoundedJSON(w, r, result, 16<<10)
}
func (h *Handler) confirmAttachedPiAPIPolicy(w http.ResponseWriter, r *http.Request) {
	a, err := h.authorizeMutation(r, domain.TenantPermissionWrite)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	var input webcontract.AttachedPiAPIPolicyConfirmationV1
	if decodePiPolicyRequest(r, &input) != nil {
		h.writeError(w, r, domain.ErrMembershipDenied)
		return
	}
	request, err := piPolicySelector(a, input.AttachedPiAPIPolicySelectorV1)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	result, err := h.config.AttachedPiAPIPolicies.Confirm(r.Context(), ports.ConfirmAttachedPiAPIPolicyV1{ReadAttachedPiAPIPolicyV1: request, CSRFTokenDigest: a.Session.CSRFTokenDigest, TermsDigest: input.TermsDigest, ExpectedGeneration: input.ExpectedGeneration, MutationID: input.MutationID})
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	h.writeBoundedJSON(w, r, result, 16<<10)
}
func (h *Handler) revokeAttachedPiAPIPolicy(w http.ResponseWriter, r *http.Request) {
	a, err := h.authorizeMutation(r, domain.TenantPermissionWrite)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	var input webcontract.AttachedPiAPIPolicyRevocationV1
	if decodePiPolicyRequest(r, &input) != nil {
		h.writeError(w, r, domain.ErrMembershipDenied)
		return
	}
	request, err := piPolicySelector(a, input.AttachedPiAPIPolicySelectorV1)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	if err := h.config.AttachedPiAPIPolicies.Revoke(r.Context(), ports.RevokeAttachedPiAPIPolicyV1{ReadAttachedPiAPIPolicyV1: request, CSRFTokenDigest: a.Session.CSRFTokenDigest, ExpectedGeneration: input.ExpectedGeneration, ExpectedMapping: input.ExpectedMapping}); err != nil {
		h.writeError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
