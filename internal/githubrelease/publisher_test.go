package githubrelease

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

const testSHA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

type releaseServer struct {
	t           *testing.T
	server      *httptest.Server
	mu          sync.Mutex
	release     *Release
	assets      []RemoteAsset
	assetBodies map[string][]byte
	writes      []string
	requests    int
	uploadRace  map[string][]byte
}

func newReleaseServer(t *testing.T) *releaseServer {
	t.Helper()
	state := &releaseServer{t: t, assetBodies: make(map[string][]byte), uploadRace: make(map[string][]byte)}
	state.server = httptest.NewServer(http.HandlerFunc(state.serveHTTP))
	t.Cleanup(state.server.Close)
	return state
}

func (s *releaseServer) serveHTTP(response http.ResponseWriter, request *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requests++
	if request.Header.Get("Authorization") != "Bearer test-token" {
		http.Error(response, `{"private":"tainted-token"}`, http.StatusUnauthorized)
		return
	}
	base := "/repos/acme/sessionless"
	switch {
	case request.Method == http.MethodGet && request.URL.Path == base+"/releases":
		response.Header().Set("Content-Type", "application/json")
		if s.release == nil {
			_, _ = io.WriteString(response, "[]")
			return
		}
		_ = json.NewEncoder(response).Encode([]Release{*s.release})
	case request.Method == http.MethodPost && request.URL.Path == base+"/releases":
		s.writes = append(s.writes, "create")
		var input struct {
			TagName              string `json:"tag_name"`
			Name                 string `json:"name"`
			Body                 string `json:"body"`
			Draft                bool   `json:"draft"`
			Prerelease           bool   `json:"prerelease"`
			GenerateReleaseNotes bool   `json:"generate_release_notes"`
			TargetCommitish      string `json:"target_commitish"`
		}
		decodeJSON(tResponse{response, s.t}, request.Body, &input)
		if !input.Draft || input.GenerateReleaseNotes || input.TargetCommitish != "" {
			s.t.Errorf("unsafe release creation payload: %#v", input)
		}
		s.release = &Release{ID: 7, TagName: input.TagName, Name: input.Name, Body: input.Body,
			Draft: true, Prerelease: input.Prerelease,
			UploadURL: s.server.URL + base + "/releases/7/assets{?name,label}"}
		response.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(response).Encode(s.release)
	case request.Method == http.MethodGet && request.URL.Path == base+"/releases/7/assets":
		response.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(response).Encode(s.assets)
	case request.Method == http.MethodPost && request.URL.Path == base+"/releases/7/assets":
		name := request.URL.Query().Get("name")
		body, err := io.ReadAll(request.Body)
		if err != nil {
			s.t.Errorf("read upload: %v", err)
		}
		if racedBody, ok := s.uploadRace[name]; ok {
			remote := remoteAsset(int64(len(s.assets)+1), name, racedBody)
			s.assets = append(s.assets, remote)
			delete(s.uploadRace, name)
			http.Error(response, "same-name asset", http.StatusUnprocessableEntity)
			return
		}
		remote := remoteAsset(int64(len(s.assets)+1), name, body)
		s.assets = append(s.assets, remote)
		s.assetBodies[name] = append([]byte(nil), body...)
		s.writes = append(s.writes, "upload:"+name)
		response.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(response).Encode(remote)
	case request.Method == http.MethodDelete && strings.HasPrefix(request.URL.Path, base+"/releases/assets/"):
		idPath := strings.TrimPrefix(request.URL.Path, base+"/releases/assets/")
		for index, asset := range s.assets {
			if fmt.Sprint(asset.ID) == idPath {
				s.assets = append(s.assets[:index], s.assets[index+1:]...)
				s.writes = append(s.writes, "delete:"+asset.Name)
				response.WriteHeader(http.StatusNoContent)
				return
			}
		}
		response.WriteHeader(http.StatusNotFound)
	case request.Method == http.MethodPatch && request.URL.Path == base+"/releases/7":
		s.writes = append(s.writes, "publish")
		var input struct {
			Draft *bool `json:"draft"`
		}
		decodeJSON(tResponse{response, s.t}, request.Body, &input)
		if input.Draft == nil || *input.Draft {
			s.t.Errorf("publish payload does not clear draft: %#v", input)
		}
		s.release.Draft = false
		_ = json.NewEncoder(response).Encode(s.release)
	default:
		http.Error(response, "unexpected request", http.StatusNotFound)
	}
}

type tResponse struct {
	http.ResponseWriter
	t *testing.T
}

func decodeJSON(response tResponse, body io.Reader, output any) {
	response.t.Helper()
	if err := json.NewDecoder(body).Decode(output); err != nil {
		response.t.Errorf("decode request: %v", err)
		response.WriteHeader(http.StatusBadRequest)
	}
}

func (s *releaseServer) publisher(t *testing.T) *Publisher {
	t.Helper()
	client, err := NewClient(s.server.URL, "test-token", s.server.Client())
	if err != nil {
		t.Fatal(err)
	}
	publisher, err := NewPublisher(client)
	if err != nil {
		t.Fatal(err)
	}
	return publisher
}

func TestPublishCreatesDraftVerifiesAssetsThenPublishes(t *testing.T) {
	state := newReleaseServer(t)
	request := testRequest(t)
	result, err := state.publisher(t).Publish(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "published" || result.Writes != 5 || state.release == nil || state.release.Draft {
		t.Fatalf("result=%#v release=%#v", result, state.release)
	}
	wantWrites := []string{
		"create",
		"upload:deployment-images.json",
		"upload:deployment-images.sha256",
		"upload:release-notes.md",
		"publish",
	}
	if fmt.Sprint(state.writes) != fmt.Sprint(wantWrites) {
		t.Fatalf("writes=%v, want %v", state.writes, wantWrites)
	}
	for _, asset := range request.Assets {
		if !bytes.Equal(state.assetBodies[asset.Name], asset.Data) {
			t.Fatalf("uploaded %s differs from local bytes", asset.Name)
		}
	}
}

func TestPublishSameTagRetryPerformsZeroWrites(t *testing.T) {
	state := newReleaseServer(t)
	request := testRequest(t)
	publisher := state.publisher(t)
	if _, err := publisher.Publish(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	state.mu.Lock()
	state.writes = nil
	state.mu.Unlock()
	result, err := publisher.Publish(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "verified_existing" || result.Writes != 0 {
		t.Fatalf("result=%#v", result)
	}
	if len(state.writes) != 0 {
		t.Fatalf("same-tag retry mutated release: %v", state.writes)
	}
}

func TestPublishRejectsConflictingAssetBeforeMutation(t *testing.T) {
	state := newReleaseServer(t)
	request := testRequest(t)
	state.release = &Release{ID: 7, TagName: request.Tag, Name: request.Name, Body: string(request.Body),
		Draft: true, UploadURL: state.server.URL + "/repos/acme/sessionless/releases/7/assets{?name,label}"}
	for index, asset := range request.Assets {
		body := asset.Data
		if asset.Name == ManifestAssetName {
			body = []byte("different immutable bytes")
		}
		state.assets = append(state.assets, remoteAsset(int64(index+1), asset.Name, body))
	}
	_, err := state.publisher(t).Publish(context.Background(), request)
	if err == nil || !strings.Contains(err.Error(), "does not match expected immutable bytes") {
		t.Fatalf("error=%v", err)
	}
	if len(state.writes) != 0 {
		t.Fatalf("conflict caused mutation: %v", state.writes)
	}
}

func TestPublishResumesPartialDraft(t *testing.T) {
	state := newReleaseServer(t)
	request := testRequest(t)
	state.release = draftRelease(state, request)
	state.assets = []RemoteAsset{remoteAsset(1, request.Assets[0].Name, request.Assets[0].Data)}
	result, err := state.publisher(t).Publish(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "published" || result.Writes != 3 {
		t.Fatalf("result=%#v writes=%v", result, state.writes)
	}
	if len(state.assets) != 3 || state.release.Draft {
		t.Fatalf("assets=%#v release=%#v", state.assets, state.release)
	}
}

func TestPublishRecoversEmptyStarterOnDraft(t *testing.T) {
	state := newReleaseServer(t)
	request := testRequest(t)
	state.release = draftRelease(state, request)
	for index, asset := range request.Assets {
		remote := remoteAsset(int64(index+1), asset.Name, asset.Data)
		if index == 0 {
			remote.State, remote.Size, remote.Digest = "starter", 0, ""
		}
		state.assets = append(state.assets, remote)
	}
	result, err := state.publisher(t).Publish(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if result.Writes != 3 || state.writes[0] != "delete:"+request.Assets[0].Name {
		t.Fatalf("result=%#v writes=%v", result, state.writes)
	}
}

func TestPublishUploadRaceAcceptsOnlyExactBytes(t *testing.T) {
	for _, test := range []struct {
		name    string
		body    func(Asset) []byte
		wantErr bool
	}{
		{name: "exact", body: func(asset Asset) []byte { return asset.Data }},
		{name: "conflict", body: func(Asset) []byte { return []byte("raced conflict") }, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			state := newReleaseServer(t)
			request := testRequest(t)
			state.release = draftRelease(state, request)
			raced := request.Assets[0]
			state.uploadRace[raced.Name] = test.body(raced)
			_, err := state.publisher(t).Publish(context.Background(), request)
			if test.wantErr && err == nil {
				t.Fatal("conflicting upload race succeeded")
			}
			if !test.wantErr && err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPublishedReleaseMismatchesNeverMutate(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*releaseServer, Request)
	}{
		{name: "missing asset", mutate: func(state *releaseServer, _ Request) { state.assets = state.assets[:2] }},
		{name: "extra asset", mutate: func(state *releaseServer, _ Request) {
			state.assets = append(state.assets, remoteAsset(9, "unexpected.bin", []byte("x")))
		}},
		{name: "duplicate asset", mutate: func(state *releaseServer, _ Request) {
			state.assets[2] = state.assets[1]
		}},
		{name: "body mismatch", mutate: func(state *releaseServer, _ Request) { state.release.Body = "changed" }},
		{name: "metadata mismatch", mutate: func(state *releaseServer, _ Request) { state.release.Name = "other" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			state := newReleaseServer(t)
			request := testRequest(t)
			state.release = draftRelease(state, request)
			state.release.Draft = false
			for index, asset := range request.Assets {
				state.assets = append(state.assets, remoteAsset(int64(index+1), asset.Name, asset.Data))
			}
			test.mutate(state, request)
			if _, err := state.publisher(t).Publish(context.Background(), request); err == nil {
				t.Fatal("published release mismatch succeeded")
			}
			if len(state.writes) != 0 {
				t.Fatalf("mismatch caused writes: %v", state.writes)
			}
		})
	}
}

func TestMalformedLocalEvidenceFailsBeforeGitHub(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, *Request)
	}{
		{name: "wrong source sha", mutate: func(_ *testing.T, request *Request) {
			request.SourceSHA = strings.Repeat("c", 40)
		}},
		{name: "checksum mismatch", mutate: func(_ *testing.T, request *Request) {
			asset(request, ChecksumAssetName).Data = []byte(strings.Repeat("0", 64) + "  deployment-images.json\n")
		}},
		{name: "malformed manifest", mutate: func(_ *testing.T, request *Request) {
			setManifest(request, []byte("{not-json}\n"))
		}},
		{name: "four image manifest", mutate: func(t *testing.T, request *Request) {
			var document map[string]any
			if err := json.Unmarshal(asset(request, ManifestAssetName).Data, &document); err != nil {
				t.Fatal(err)
			}
			delete(document["images"].(map[string]any), "web-bff")
			data, err := json.Marshal(document)
			if err != nil {
				t.Fatal(err)
			}
			setManifest(request, append(data, '\n'))
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			state := newReleaseServer(t)
			request := testRequest(t)
			test.mutate(t, &request)
			if _, err := state.publisher(t).Publish(context.Background(), request); err == nil {
				t.Fatal("malformed local evidence succeeded")
			}
			if state.requests != 0 {
				t.Fatalf("malformed evidence contacted GitHub %d times", state.requests)
			}
		})
	}
}

func TestHTTPErrorDoesNotExposeTaintedResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		http.Error(response, `{"token":"super-secret-upstream-body"}`, http.StatusForbidden)
	}))
	t.Cleanup(server.Close)
	client, err := NewClient(server.URL, "test-token", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.FindRelease(context.Background(), "acme/sessionless", "v1.0.0")
	if err == nil || strings.Contains(err.Error(), "super-secret") || strings.Contains(err.Error(), "upstream-body") {
		t.Fatalf("unsafe error=%q", err)
	}
}

func TestClientPaginatesReleasesAndAssets(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		page := request.URL.Query().Get("page")
		switch {
		case request.URL.Path == "/repos/acme/sessionless/releases" && page == "1":
			releases := make([]Release, 100)
			for index := range releases {
				releases[index] = Release{ID: int64(index + 1), TagName: fmt.Sprintf("v0.0.%d", index)}
			}
			_ = json.NewEncoder(response).Encode(releases)
		case request.URL.Path == "/repos/acme/sessionless/releases" && page == "2":
			_ = json.NewEncoder(response).Encode([]Release{{ID: 501, TagName: "v1.0.0", Name: "target"}})
		case request.URL.Path == "/repos/acme/sessionless/releases/501/assets" && page == "1":
			assets := make([]RemoteAsset, 100)
			for index := range assets {
				assets[index] = RemoteAsset{ID: int64(index + 1), Name: fmt.Sprintf("asset-%d", index)}
			}
			_ = json.NewEncoder(response).Encode(assets)
		case request.URL.Path == "/repos/acme/sessionless/releases/501/assets" && page == "2":
			_ = json.NewEncoder(response).Encode([]RemoteAsset{{ID: 501, Name: "last"}})
		default:
			http.Error(response, "unexpected", http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	client, err := NewClient(server.URL, "test-token", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	release, err := client.FindRelease(context.Background(), "acme/sessionless", "v1.0.0")
	if err != nil || release.ID != 501 {
		t.Fatalf("release=%#v error=%v", release, err)
	}
	assets, err := client.ListAssets(context.Background(), "acme/sessionless", release.ID)
	if err != nil || len(assets) != 101 || assets[100].Name != "last" {
		t.Fatalf("assets=%d error=%v", len(assets), err)
	}
}

func testRequest(t *testing.T) Request {
	t.Helper()
	manifest := testManifest(t)
	hash := sha256.Sum256(manifest)
	notes := []byte("# Sessionless v1.0.0\n\nDeterministic notes.\n")
	return Request{
		Repository: "acme/sessionless",
		Tag:        "v1.0.0",
		SourceSHA:  testSHA,
		Name:       "Sessionless v1.0.0",
		Body:       notes,
		Assets: []Asset{
			{Name: NotesAssetName, ContentType: "text/markdown; charset=utf-8", Data: notes},
			{Name: ManifestAssetName, ContentType: "application/json", Data: manifest},
			{Name: ChecksumAssetName, ContentType: "text/plain; charset=utf-8",
				Data: []byte(hex.EncodeToString(hash[:]) + "  deployment-images.json\n")},
		},
	}
}

func draftRelease(state *releaseServer, request Request) *Release {
	return &Release{ID: 7, TagName: request.Tag, Name: request.Name, Body: string(request.Body),
		Draft: true, Prerelease: request.Prerelease,
		UploadURL: state.server.URL + "/repos/acme/sessionless/releases/7/assets{?name,label}"}
}

func asset(request *Request, name string) *Asset {
	for index := range request.Assets {
		if request.Assets[index].Name == name {
			return &request.Assets[index]
		}
	}
	panic("missing test asset " + name)
}

func setManifest(request *Request, data []byte) {
	asset(request, ManifestAssetName).Data = data
	sum := sha256.Sum256(data)
	asset(request, ChecksumAssetName).Data = []byte(hex.EncodeToString(sum[:]) + "  deployment-images.json\n")
}

func testManifest(t *testing.T) []byte {
	t.Helper()
	committedAt := time.Unix(1_700_000_000, 0).UTC()
	inputDigests := make(map[string]string)
	images := make(map[string]any)
	for index, component := range requiredComponents {
		manifestDigest := testDigest(index + 1)
		inputDigest := testDigest(index + 20)
		inputDigests[component] = inputDigest
		images[component] = map[string]string{
			"tagged_reference": "cr.yandex/registry1/" + component + ":" + testSHA,
			"reference":        "cr.yandex/registry1/" + component + "@" + manifestDigest,
			"manifest_digest":  manifestDigest, "config_digest": testDigest(index + 10), "input_digest": inputDigest,
		}
	}
	payload := map[string]any{
		"schema_version": 2,
		"source": map[string]string{"repository": "gitcode.com/urandon/sessionless", "sha": testSHA,
			"tree": strings.Repeat("b", 40), "committed_at": committedAt.Format(time.RFC3339),
			"source_date_epoch": fmt.Sprint(committedAt.Unix())},
		"build":  map[string]any{"platform": "linux/amd64", "contract_digest": testDigest(99), "input_digests": inputDigests},
		"images": images,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	return append(data, '\n')
}

func testDigest(seed int) string {
	return fmt.Sprintf("sha256:%064x", seed)
}

func remoteAsset(id int64, name string, body []byte) RemoteAsset {
	sum := sha256.Sum256(body)
	return RemoteAsset{ID: id, Name: name, State: "uploaded", Size: int64(len(body)), Digest: "sha256:" + hex.EncodeToString(sum[:])}
}

func TestAssetUploadURLUsesEncodedFixedName(t *testing.T) {
	state := newReleaseServer(t)
	client, err := NewClient(state.server.URL, "test-token", state.server.Client())
	if err != nil {
		t.Fatal(err)
	}
	got, err := client.safeUploadURL(state.server.URL+"/upload{?name,label}", "release-notes.md")
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := url.Parse(got)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Query().Get("name") != "release-notes.md" {
		t.Fatalf("upload URL=%s", got)
	}
}

func TestValidateRequestSortsAssetsDeterministically(t *testing.T) {
	assets, err := validateRequest(testRequest(t))
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(assets))
	for _, asset := range assets {
		names = append(names, asset.Name)
	}
	if !sort.StringsAreSorted(names) {
		t.Fatalf("asset names not sorted: %v", names)
	}
}
