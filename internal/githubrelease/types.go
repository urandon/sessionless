package githubrelease

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	ManifestAssetName = "deployment-images.json"
	ChecksumAssetName = "deployment-images.sha256"
	NotesAssetName    = "release-notes.md"
)

var (
	repositoryPattern = regexp.MustCompile(`^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$`)
	shaPattern        = regexp.MustCompile(`^[0-9a-f]{40}$`)
	digestPattern     = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
)

var ErrNotFound = errors.New("github release not found")

var requiredComponents = []string{"control-api", "reconciler", "telegram-sender", "web-bff", "worker-runtime"}

type HTTPStatusError struct {
	Method string
	Path   string
	Status int
}

func (e *HTTPStatusError) Error() string {
	return fmt.Sprintf("github %s %s returned HTTP %d", e.Method, e.Path, e.Status)
}

type Asset struct {
	Name        string
	ContentType string
	Data        []byte
}

func (a Asset) Digest() string {
	sum := sha256.Sum256(a.Data)
	return "sha256:" + hex.EncodeToString(sum[:])
}

type Request struct {
	Repository string
	Tag        string
	SourceSHA  string
	Name       string
	Body       []byte
	Prerelease bool
	Assets     []Asset
}

type Release struct {
	ID         int64  `json:"id"`
	TagName    string `json:"tag_name"`
	Name       string `json:"name"`
	Body       string `json:"body"`
	Draft      bool   `json:"draft"`
	Prerelease bool   `json:"prerelease"`
	Immutable  bool   `json:"immutable"`
	UploadURL  string `json:"upload_url"`
}

type RemoteAsset struct {
	ID          int64  `json:"id"`
	Name        string `json:"name"`
	State       string `json:"state"`
	ContentType string `json:"content_type"`
	Size        int64  `json:"size"`
	Digest      string `json:"digest"`
}

type Result struct {
	ReleaseID int64  `json:"release_id"`
	Tag       string `json:"tag"`
	SourceSHA string `json:"source_sha"`
	Status    string `json:"status"`
	Writes    int    `json:"writes"`
}

func validateRequest(request Request) ([]Asset, error) {
	if !repositoryPattern.MatchString(request.Repository) {
		return nil, errors.New("repository must have the form owner/name")
	}
	if strings.TrimSpace(request.Tag) == "" || strings.ContainsAny(request.Tag, "\r\n") {
		return nil, errors.New("tag must be non-empty and single-line")
	}
	if !shaPattern.MatchString(request.SourceSHA) {
		return nil, errors.New("source SHA must be a full lowercase commit SHA")
	}
	if strings.TrimSpace(request.Name) == "" || strings.ContainsAny(request.Name, "\r\n") {
		return nil, errors.New("release name must be non-empty and single-line")
	}
	if len(request.Body) == 0 {
		return nil, errors.New("release body must not be empty")
	}

	expected := map[string]struct{}{
		ManifestAssetName: {},
		ChecksumAssetName: {},
		NotesAssetName:    {},
	}
	if len(request.Assets) != len(expected) {
		return nil, fmt.Errorf("exactly three release assets are required, got %d", len(request.Assets))
	}
	assets := append([]Asset(nil), request.Assets...)
	sort.Slice(assets, func(i, j int) bool { return assets[i].Name < assets[j].Name })
	seen := make(map[string]struct{}, len(assets))
	for _, asset := range assets {
		if _, ok := expected[asset.Name]; !ok {
			return nil, fmt.Errorf("unexpected release asset %q", asset.Name)
		}
		if _, duplicate := seen[asset.Name]; duplicate {
			return nil, fmt.Errorf("duplicate release asset %q", asset.Name)
		}
		seen[asset.Name] = struct{}{}
		if asset.ContentType == "" || strings.ContainsAny(asset.ContentType, "\r\n") {
			return nil, fmt.Errorf("release asset %q has invalid content type", asset.Name)
		}
		if len(asset.Data) == 0 {
			return nil, fmt.Errorf("release asset %q must not be empty", asset.Name)
		}
		wantContentType := map[string]string{
			ManifestAssetName: "application/json",
			ChecksumAssetName: "text/plain; charset=utf-8",
			NotesAssetName:    "text/markdown; charset=utf-8",
		}[asset.Name]
		if asset.ContentType != wantContentType {
			return nil, fmt.Errorf("release asset %q has content type %q, expected %q", asset.Name, asset.ContentType, wantContentType)
		}
		if !digestPattern.MatchString(asset.Digest()) {
			return nil, fmt.Errorf("release asset %q has invalid digest", asset.Name)
		}
	}
	for _, asset := range assets {
		if asset.Name == NotesAssetName && string(asset.Data) != string(request.Body) {
			return nil, errors.New("release body must equal release-notes.md byte-for-byte")
		}
	}
	byName := make(map[string]Asset, len(assets))
	for _, asset := range assets {
		byName[asset.Name] = asset
	}
	wantChecksum := strings.TrimPrefix(byName[ManifestAssetName].Digest(), "sha256:") + "  " + ManifestAssetName + "\n"
	if string(byName[ChecksumAssetName].Data) != wantChecksum {
		return nil, errors.New("deployment-images.sha256 does not match deployment-images.json")
	}
	if err := validateManifest(byName[ManifestAssetName].Data, request.SourceSHA); err != nil {
		return nil, err
	}
	return assets, nil
}

type deploymentManifest struct {
	SchemaVersion int `json:"schema_version"`
	Source        struct {
		Repository      string `json:"repository"`
		SHA             string `json:"sha"`
		Tree            string `json:"tree"`
		CommittedAt     string `json:"committed_at"`
		SourceDateEpoch string `json:"source_date_epoch"`
	} `json:"source"`
	Build struct {
		Platform       string            `json:"platform"`
		ContractDigest string            `json:"contract_digest"`
		InputDigests   map[string]string `json:"input_digests"`
	} `json:"build"`
	Images map[string]struct {
		TaggedReference string `json:"tagged_reference"`
		Reference       string `json:"reference"`
		ManifestDigest  string `json:"manifest_digest"`
		ConfigDigest    string `json:"config_digest"`
		InputDigest     string `json:"input_digest"`
	} `json:"images"`
}

func validateManifest(data []byte, sourceSHA string) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var manifest deploymentManifest
	if err := decoder.Decode(&manifest); err != nil {
		return fmt.Errorf("deployment-images.json: %w", err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return fmt.Errorf("deployment-images.json: %w", err)
	}
	if manifest.SchemaVersion != 2 || manifest.Source.Repository != "gitcode.com/urandon/sessionless" ||
		manifest.Source.SHA != sourceSHA || !shaPattern.MatchString(manifest.Source.Tree) || manifest.Build.Platform != "linux/amd64" ||
		!digestPattern.MatchString(manifest.Build.ContractDigest) {
		return errors.New("deployment-images.json has invalid schema, source, or build identity")
	}
	committedAt, err := time.Parse(time.RFC3339, manifest.Source.CommittedAt)
	if err != nil {
		return errors.New("deployment-images.json has invalid committed_at")
	}
	epoch, err := strconv.ParseInt(manifest.Source.SourceDateEpoch, 10, 64)
	if err != nil || epoch <= 0 || committedAt.Unix() != epoch {
		return errors.New("deployment-images.json has inconsistent source_date_epoch")
	}
	if err := requireExactComponentKeys(manifest.Images); err != nil {
		return fmt.Errorf("deployment-images.json images: %w", err)
	}
	if err := requireExactStringKeys(manifest.Build.InputDigests); err != nil {
		return fmt.Errorf("deployment-images.json build input digests: %w", err)
	}
	registryPrefix := ""
	for _, component := range requiredComponents {
		image := manifest.Images[component]
		if !digestPattern.MatchString(image.ManifestDigest) || !digestPattern.MatchString(image.ConfigDigest) ||
			!digestPattern.MatchString(image.InputDigest) || image.InputDigest != manifest.Build.InputDigests[component] {
			return fmt.Errorf("deployment-images.json image %q has invalid digest contract", component)
		}
		suffix := "/" + component + "@" + image.ManifestDigest
		if !strings.HasPrefix(image.Reference, "cr.yandex/") || !strings.HasSuffix(image.Reference, suffix) {
			return fmt.Errorf("deployment-images.json image %q has invalid immutable reference", component)
		}
		prefix := strings.TrimSuffix(image.Reference, suffix)
		if registryPrefix == "" {
			registryPrefix = prefix
		} else if prefix != registryPrefix {
			return errors.New("deployment-images.json spans more than one registry")
		}
		wantTag := prefix + "/" + component + ":" + sourceSHA
		if image.TaggedReference != wantTag {
			return fmt.Errorf("deployment-images.json image %q has invalid source tag", component)
		}
	}
	return nil
}

func requireJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("contains more than one JSON value")
		}
		return err
	}
	return nil
}

func requireExactComponentKeys[T any](values map[string]T) error {
	if len(values) != len(requiredComponents) {
		return fmt.Errorf("expected exactly five components, got %d", len(values))
	}
	for _, component := range requiredComponents {
		if _, ok := values[component]; !ok {
			return fmt.Errorf("missing component %q", component)
		}
	}
	return nil
}

func requireExactStringKeys(values map[string]string) error {
	return requireExactComponentKeys(values)
}
