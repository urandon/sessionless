package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"gitcode.com/urandon/sessionless/internal/githubrelease"
)

const maxAssetBytes = 8 << 20

type assetPaths []string

func (paths *assetPaths) String() string { return fmt.Sprint([]string(*paths)) }
func (paths *assetPaths) Set(value string) error {
	*paths = append(*paths, value)
	return nil
}

func main() {
	if err := run(context.Background(), os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "github-release:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("github-release", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	var repository, tag, sourceSHA, title, notesPath, apiURL string
	var prerelease bool
	var paths assetPaths
	flags.StringVar(&repository, "repository", os.Getenv("GITHUB_REPOSITORY"), "GitHub owner/name")
	flags.StringVar(&tag, "tag", "", "existing verified release tag")
	flags.StringVar(&sourceSHA, "source-sha", "", "verified full source commit SHA")
	flags.StringVar(&title, "title", "", "release title (default: Sessionless TAG)")
	flags.StringVar(&notesPath, "notes", "", "deterministic release notes")
	flags.Var(&paths, "asset", "release asset path; repeat exactly three times")
	flags.StringVar(&apiURL, "api-url", envOr("GITHUB_API_URL", "https://api.github.com"), "GitHub API URL")
	flags.BoolVar(&prerelease, "prerelease", false, "publish as a prerelease")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("positional arguments are not supported")
	}
	if repository == "" || tag == "" || sourceSHA == "" || notesPath == "" {
		return errors.New("--repository (or GITHUB_REPOSITORY), --tag, --source-sha, and --notes are required")
	}
	if title == "" {
		title = "Sessionless " + tag
	}
	if len(paths) != 3 {
		return fmt.Errorf("exactly three --asset values are required, got %d", len(paths))
	}
	notes, err := readRegular(notesPath)
	if err != nil {
		return fmt.Errorf("read release notes: %w", err)
	}
	assets := make([]githubrelease.Asset, 0, len(paths))
	for _, path := range paths {
		data, err := readRegular(path)
		if err != nil {
			return fmt.Errorf("read release asset: %w", err)
		}
		name := filepath.Base(path)
		contentType, err := assetContentType(name)
		if err != nil {
			return err
		}
		assets = append(assets, githubrelease.Asset{Name: name, ContentType: contentType, Data: data})
	}
	client, err := githubrelease.NewClient(apiURL, os.Getenv("GITHUB_TOKEN"), nil)
	if err != nil {
		return err
	}
	publisher, err := githubrelease.NewPublisher(client)
	if err != nil {
		return err
	}
	result, err := publisher.Publish(ctx, githubrelease.Request{
		Repository: repository,
		Tag:        tag,
		SourceSHA:  sourceSHA,
		Name:       title,
		Body:       notes,
		Prerelease: prerelease,
		Assets:     assets,
	})
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(result)
}

func readRegular(path string) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > maxAssetBytes {
		return nil, fmt.Errorf("%s must be a non-empty regular file no larger than %d bytes", filepath.Base(path), maxAssetBytes)
	}
	return os.ReadFile(path)
}

func assetContentType(name string) (string, error) {
	switch name {
	case githubrelease.ManifestAssetName:
		return "application/json", nil
	case githubrelease.ChecksumAssetName:
		return "text/plain; charset=utf-8", nil
	case githubrelease.NotesAssetName:
		return "text/markdown; charset=utf-8", nil
	default:
		return "", fmt.Errorf("unexpected release asset %q", name)
	}
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
