package localoperator_test

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/synctest"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/localoperator"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func sourceSearchCredential(m localoperator.ManifestV1, g domain.WebSearchResourceGrantV2) ports.SearchCredentialDeclarationV1 {
	return ports.SearchCredentialDeclarationV1{Version: 1, TenantID: m.Configuration.TenantID, Resource: domain.ProviderResourceBindingV1{Kind: domain.ProviderResourceWebSearchAccountV1, ResourceID: g.CredentialRef, OwnerUserID: g.OwnerUserID, Revision: 17, CredentialMode: domain.ProviderCredentialInvocationV1, CredentialGeneration: 23}, Audience: g.CredentialAudience, DeliveryKind: domain.ProviderCredentialDeliveryDirectV1}
}

func TestSourceSearchCredentialExactOriginalDeclaration(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		root, exe, m, grant := searchSourceFiles(t)
		d := sourceSearchCredential(m, grant)
		m.SearchCredential = &d
		writeFitManifest(t, root, m)
		s := openFitSource(t, t.Context(), root, exe)
		receipt := acceptSearchSourceTestPostimage(t, s)
		ref := domain.OwnedAgentProfileReferenceV1{Locator: receipt.Profile.Locator, RevocationGeneration: receipt.Profile.RevocationGeneration, Digest: receipt.Digest}
		got, err := s.ReadSearchCredentialDeclaration(t.Context(), ref)
		if err != nil || got != d {
			t.Fatalf("original declaration=%+v want=%+v err=%v", got, d, err)
		}
		if got.Resource.Revision == grant.ResourceRevision || got.Resource.CredentialGeneration == grant.RevocationGeneration {
			t.Fatal("credential revision was inferred from grant revision")
		}
		got.Resource.Revision++
		again, err := s.ReadSearchCredentialDeclaration(t.Context(), ref)
		if err != nil || again != d {
			t.Fatal("returned declaration aliases Source", err)
		}
		foreign := ref
		foreign.Locator.TenantID = "foreign-tenant"
		if _, err := s.ReadSearchCredentialDeclaration(t.Context(), foreign); err == nil {
			t.Fatal("foreign profile disclosed declaration")
		}
		configuration, err := s.Configuration()
		raw, readErr := os.ReadFile(filepath.Join(root, "manifest.json"))
		if err != nil || readErr != nil || configuration.OriginalSource.Digest != fitDigest(raw) || receipt.Profile.InstalledManifest != configuration.OriginalSource || !bytes.Contains(raw, []byte(`"search_credential":`)) {
			t.Fatalf("whole manifest failed to commit declaration err=%v read=%v", err, readErr)
		}
		if err := s.VerifyOriginalSpendingSources(receipt, nil); err != nil {
			t.Fatal("original installation failed final recheck", err)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := s.ReadSearchCredentialDeclaration(t.Context(), ref); err == nil {
			t.Fatal("closed Source disclosed declaration")
		}
	})
}

func TestSourceSearchCredentialOmissionPreservesOriginalManifest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		root, exe, m, _ := searchSourceFiles(t)
		raw, err := json.Marshal(m)
		if err != nil || bytes.Contains(raw, []byte("search_credential")) {
			t.Fatal("omitted declaration changed old representation", err)
		}
		s := openFitSource(t, t.Context(), root, exe)
		r := acceptSearchSourceTestPostimage(t, s)
		ref := domain.OwnedAgentProfileReferenceV1{Locator: r.Profile.Locator, RevocationGeneration: r.Profile.RevocationGeneration, Digest: r.Digest}
		if _, err := s.ReadSearchCredentialDeclaration(t.Context(), ref); err == nil {
			t.Fatal("omission discovered ambient/latest credentials")
		}
		g, err := s.ReadWebSearchResourceGrant(t.Context(), ref, domain.WebSearchResourceGrantMaxBytesV2)
		if err != nil || len(g) == 0 {
			t.Fatal("omission damaged original grant custody", err)
		}
		var decoded localoperator.ManifestV1
		if err := json.Unmarshal(raw, &decoded); err != nil {
			t.Fatal(err)
		}
		again, err := json.Marshal(decoded)
		if err != nil || !bytes.Equal(raw, again) {
			t.Fatal("old omitted declaration bytes changed", err)
		}
	})
}

func TestSourceSearchCredentialRejectsForeignOrOpenDeclarations(t *testing.T) {
	for _, name := range []string{"version", "kind", "owner", "tenant", "resource", "mode", "revision", "generation", "audience", "delivery", "missing-grant", "missing-expenditure", "null", "duplicate", "alias", "unknown", "secret"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				root, exe, m, g := searchSourceFiles(t)
				d := sourceSearchCredential(m, g)
				m.SearchCredential = &d
				switch name {
				case "version":
					d.Version = 2
				case "kind":
					d.Resource.Kind = domain.ProviderResourceAPIAccountV1
				case "owner":
					d.Resource.OwnerUserID = "foreign-owner"
				case "tenant":
					d.TenantID = "foreign-tenant"
				case "resource":
					d.Resource.ResourceID = "uncommitted-credential"
				case "mode":
					d.Resource.CredentialMode = domain.ProviderCredentialNoneV1
				case "revision":
					d.Resource.Revision = 0
				case "generation":
					d.Resource.CredentialGeneration = 0
				case "audience":
					d.Audience = "model"
				case "delivery":
					d.DeliveryKind = domain.ProviderCredentialDeliveryEnvironmentV1
				case "missing-grant":
					m.Profile.WebSearchResourceGrant = nil
				case "missing-expenditure":
					m.SearchExpenditure = nil
				}
				raw, err := json.Marshal(m)
				if err != nil {
					t.Fatal(err)
				}
				switch name {
				case "null":
					encoded, err := json.Marshal(d)
					if err != nil {
						t.Fatal(err)
					}
					raw = bytes.Replace(raw, append([]byte(`"search_credential":`), encoded...), []byte(`"search_credential":null`), 1)
				case "duplicate":
					raw = bytes.Replace(raw, []byte(`"search_credential":{`), []byte(`"search_credential":null,"search_credential":{`), 1)
				case "alias":
					raw = []byte(strings.Replace(string(raw), `"audience":"tavily.search"`, `"Audience":"tavily.search"`, 1))
				case "unknown":
					raw = bytes.Replace(raw, []byte(`"search_credential":{`), []byte(`"search_credential":{"extra":true,`), 1)
				case "secret":
					raw = bytes.Replace(raw, []byte(`"search_credential":{`), []byte(`"search_credential":{"secret_ref":"forbidden",`), 1)
				}
				if err := os.WriteFile(filepath.Join(root, "manifest.json"), raw, 0600); err != nil {
					t.Fatal(err)
				}
				if s, err := localoperator.OpenFresh(t.Context(), root, exe); err == nil {
					_ = s.Close()
					t.Fatalf("case=%s accepted invalid declaration", name)
				}
			})
		})
	}
}
