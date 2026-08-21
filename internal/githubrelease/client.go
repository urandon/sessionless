package githubrelease

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

const (
	apiVersion       = "2022-11-28"
	maxResponseBytes = 4 << 20
	maxPages         = 100
)

type API interface {
	FindRelease(context.Context, string, string) (Release, error)
	CreateDraft(context.Context, Request) (Release, error)
	PublishDraft(context.Context, string, int64) (Release, error)
	ListAssets(context.Context, string, int64) ([]RemoteAsset, error)
	UploadAsset(context.Context, string, Release, Asset) (RemoteAsset, error)
	DeleteAsset(context.Context, string, int64) error
}

type Client struct {
	baseURL    *url.URL
	token      string
	httpClient *http.Client
}

func NewClient(baseURL, token string, httpClient *http.Client) (*Client, error) {
	parsed, err := url.Parse(strings.TrimRight(baseURL, "/"))
	if err != nil || parsed.Scheme == "" || parsed.Host == "" || parsed.User != nil {
		return nil, errors.New("invalid GitHub API base URL")
	}
	if token == "" {
		return nil, errors.New("GitHub token must not be empty")
	}
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &Client{baseURL: parsed, token: token, httpClient: httpClient}, nil
}

func (c *Client) FindRelease(ctx context.Context, repository, tag string) (Release, error) {
	var found *Release
	for page := 1; page <= maxPages; page++ {
		var releases []Release
		path := fmt.Sprintf("/repos/%s/releases?per_page=100&page=%d", repository, page)
		if err := c.doJSON(ctx, http.MethodGet, path, nil, http.StatusOK, &releases); err != nil {
			return Release{}, err
		}
		for _, release := range releases {
			if release.TagName != tag {
				continue
			}
			if found != nil {
				return Release{}, fmt.Errorf("more than one GitHub release exists for tag %q", tag)
			}
			copy := release
			found = &copy
		}
		if len(releases) < 100 {
			break
		}
		if page == maxPages {
			return Release{}, errors.New("GitHub release pagination exceeded safety limit")
		}
	}
	if found == nil {
		return Release{}, ErrNotFound
	}
	return *found, nil
}

func (c *Client) CreateDraft(ctx context.Context, request Request) (Release, error) {
	payload := struct {
		TagName              string `json:"tag_name"`
		Name                 string `json:"name"`
		Body                 string `json:"body"`
		Draft                bool   `json:"draft"`
		Prerelease           bool   `json:"prerelease"`
		GenerateReleaseNotes bool   `json:"generate_release_notes"`
	}{request.Tag, request.Name, string(request.Body), true, request.Prerelease, false}
	var release Release
	err := c.doJSON(ctx, http.MethodPost, "/repos/"+request.Repository+"/releases", payload, http.StatusCreated, &release)
	return release, err
}

func (c *Client) PublishDraft(ctx context.Context, repository string, releaseID int64) (Release, error) {
	payload := struct {
		Draft bool `json:"draft"`
	}{false}
	var release Release
	err := c.doJSON(ctx, http.MethodPatch,
		fmt.Sprintf("/repos/%s/releases/%d", repository, releaseID), payload, http.StatusOK, &release)
	return release, err
}

func (c *Client) ListAssets(ctx context.Context, repository string, releaseID int64) ([]RemoteAsset, error) {
	var all []RemoteAsset
	for page := 1; page <= maxPages; page++ {
		var assets []RemoteAsset
		path := fmt.Sprintf("/repos/%s/releases/%d/assets?per_page=100&page=%d", repository, releaseID, page)
		if err := c.doJSON(ctx, http.MethodGet, path, nil, http.StatusOK, &assets); err != nil {
			return nil, err
		}
		all = append(all, assets...)
		if len(assets) < 100 {
			break
		}
		if page == maxPages {
			return nil, errors.New("GitHub release asset pagination exceeded safety limit")
		}
	}
	return all, nil
}

func (c *Client) UploadAsset(ctx context.Context, _ string, release Release, asset Asset) (RemoteAsset, error) {
	uploadURL, err := c.safeUploadURL(release.UploadURL, asset.Name)
	if err != nil {
		return RemoteAsset{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, uploadURL, bytes.NewReader(asset.Data))
	if err != nil {
		return RemoteAsset{}, err
	}
	c.setHeaders(req)
	req.Header.Set("Content-Type", asset.ContentType)
	var remote RemoteAsset
	if err := c.do(req, http.StatusCreated, &remote); err != nil {
		return RemoteAsset{}, err
	}
	return remote, nil
}

func (c *Client) DeleteAsset(ctx context.Context, repository string, assetID int64) error {
	path := fmt.Sprintf("/repos/%s/releases/assets/%d", repository, assetID)
	req, err := c.request(ctx, http.MethodDelete, path, nil)
	if err != nil {
		return err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNoContent || resp.StatusCode == http.StatusNotFound {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxResponseBytes))
		return nil
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxResponseBytes))
	return &HTTPStatusError{Method: req.Method, Path: req.URL.EscapedPath(), Status: resp.StatusCode}
}

func (c *Client) doJSON(ctx context.Context, method, path string, payload any, wantStatus int, output any) error {
	var body io.Reader
	if payload != nil {
		encoded, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		body = bytes.NewReader(encoded)
	}
	req, err := c.request(ctx, method, path, body)
	if err != nil {
		return err
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return c.do(req, wantStatus, output)
}

func (c *Client) request(ctx context.Context, method, path string, body io.Reader) (*http.Request, error) {
	relative, err := url.Parse(path)
	if err != nil {
		return nil, err
	}
	endpoint := c.baseURL.ResolveReference(relative)
	req, err := http.NewRequestWithContext(ctx, method, endpoint.String(), body)
	if err != nil {
		return nil, err
	}
	c.setHeaders(req)
	return req, nil
}

func (c *Client) setHeaders(req *http.Request) {
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("X-GitHub-Api-Version", apiVersion)
	req.Header.Set("User-Agent", "sessionless-release-publisher")
}

func (c *Client) do(req *http.Request, wantStatus int, output any) error {
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	limited := io.LimitReader(resp.Body, maxResponseBytes)
	if resp.StatusCode != wantStatus {
		_, _ = io.Copy(io.Discard, limited)
		return &HTTPStatusError{Method: req.Method, Path: req.URL.EscapedPath(), Status: resp.StatusCode}
	}
	decoder := json.NewDecoder(limited)
	if err := decoder.Decode(output); err != nil {
		return fmt.Errorf("decode GitHub response: %w", err)
	}
	return nil
}

func (c *Client) safeUploadURL(template, name string) (string, error) {
	raw := strings.TrimSuffix(template, "{?name,label}")
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" || parsed.User != nil || parsed.Fragment != "" {
		return "", errors.New("release returned an invalid upload URL")
	}
	allowedHost := parsed.Host == c.baseURL.Host
	if c.baseURL.Hostname() == "api.github.com" && parsed.Hostname() == "uploads.github.com" {
		allowedHost = true
	}
	if parsed.Scheme != c.baseURL.Scheme || !allowedHost {
		return "", errors.New("release returned an untrusted upload URL")
	}
	query := parsed.Query()
	query.Set("name", name)
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func statusCode(err error) int {
	var statusErr *HTTPStatusError
	if errors.As(err, &statusErr) {
		return statusErr.Status
	}
	return 0
}

func validateRemoteAsset(remote RemoteAsset, expected Asset) error {
	if remote.ID <= 0 || remote.Name != expected.Name || remote.State != "uploaded" ||
		remote.Size != int64(len(expected.Data)) || remote.Digest != expected.Digest() {
		return fmt.Errorf("release asset %q does not match expected immutable bytes", expected.Name)
	}
	return nil
}
