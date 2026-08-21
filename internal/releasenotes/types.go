// Package releasenotes generates deterministic release notes from the local
// first-parent Git history.
package releasenotes

import "context"

const (
	gitCodeRepositoryURL = "https://gitcode.com/urandon/sessionless"
	maxSupplementBytes   = 256 * 1024
)

// Repository is the local Git evidence required by the generator.
type Repository interface {
	ResolveCommit(context.Context, string) (string, error)
	FirstParentHistory(context.Context, string) ([]Commit, error)
	VersionTags(context.Context) ([]VersionTag, error)
	BlobAt(context.Context, string, string) ([]byte, bool, error)
}

// Commit is one commit on the current release's first-parent lineage.
type Commit struct {
	SHA     string
	Parents []string
	Subject string
	Body    string
	Paths   []string
}

// VersionTag is a syntactically valid release tag and its peeled commit.
type VersionTag struct {
	Name   string
	Commit string
}

// Options binds generated notes to an exact tag and checked-out source.
type Options struct {
	Tag         string
	SourceSHA   string
	PreviousTag string
}

type category string

const (
	categoryFeatures category = "features"
	categoryFixes    category = "fixes"
	categoryInfra    category = "infrastructure and documentation"
	categoryOther    category = "other"
)

type entry struct {
	Title        string
	SHA          string
	MergeRequest int
	Issues       []int
	Category     category
}

type document struct {
	Tag         string
	SourceSHA   string
	PreviousTag string
	Supplement  string
	Entries     []entry
}
