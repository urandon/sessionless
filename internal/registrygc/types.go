package registrygc

import "time"

const (
	SchemaVersion         = 1
	ManifestSchemaVersion = 2
	ModeDryRun            = "dry-run"
	ModeDelete            = "delete"
	DecisionRetain        = "retain"
	DecisionDelete        = "delete"
)

var RequiredComponents = []string{
	"control-api", "reconciler", "telegram-sender", "web-bff", "worker-runtime",
}

// Inventory contains only non-secret Terraform state evidence. Live cloud
// state is always read from Yandex APIs while the deployment lock is held.
type Inventory struct {
	SchemaVersion         int                            `json:"schema_version"`
	Environment           string                         `json:"environment"`
	FolderID              string                         `json:"folder_id"`
	RegistryID            string                         `json:"registry_id"`
	Repositories          map[string]TerraformRepository `json:"repositories"`
	Containers            map[string]TerraformContainer  `json:"containers"`
	StableSlot            string                         `json:"stable_slot"`
	CandidateSlot         string                         `json:"candidate_slot"`
	LockEnvironment       string                         `json:"lock_environment"`
	LifecyclePolicyStatus map[string]string              `json:"lifecycle_policy_status"`
	Terraform             TerraformStateEvidence         `json:"terraform"`
}

type TerraformRepository struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type TerraformContainer struct {
	ContainerID string `json:"container_id"`
	RevisionID  string `json:"revision_id"`
	Component   string `json:"component"`
	Repository  string `json:"repository"`
	Slot        string `json:"slot"`
	SourceSHA   string `json:"source_sha"`
	ImageRef    string `json:"image_ref"`
}

type TerraformStateEvidence struct {
	StateLineage  string `json:"state_lineage"`
	StateSerial   uint64 `json:"state_serial"`
	OutputsDigest string `json:"outputs_digest"`
}

// LiveState is populated by the Yandex adapter. Returning it means all page
// tokens were drained successfully; partial API responses are never planned.
type LiveState struct {
	Containers   map[string]LiveContainer
	Repositories map[string]LiveRepository
}

type LiveContainer struct {
	ID        string
	Name      string
	Labels    map[string]string
	Revisions []LiveRevision
}

type LiveRevision struct {
	ID          string
	Status      string
	CreatedAt   time.Time
	ImageURL    string
	ImageDigest string
}

type LiveRepository struct {
	ID     string
	Name   string
	Images []RegistryImage
}

type RegistryImage struct {
	ID             string
	Digest         string
	CreatedAt      time.Time
	CompressedSize int64
	Tags           []string
}

type CloudImage struct {
	ID        string
	Name      string
	Digest    string
	CreatedAt time.Time
}

type DeploymentManifest struct {
	SchemaVersion int                      `json:"schema_version"`
	Source        ManifestSource           `json:"source"`
	Build         ManifestBuild            `json:"build"`
	Images        map[string]ManifestImage `json:"images"`
}

type ManifestSource struct {
	Repository      string `json:"repository"`
	SHA             string `json:"sha"`
	Tree            string `json:"tree"`
	CommittedAt     string `json:"committed_at"`
	SourceDateEpoch string `json:"source_date_epoch"`
}

type ManifestBuild struct {
	Platform       string            `json:"platform"`
	ContractDigest string            `json:"contract_digest"`
	InputDigests   map[string]string `json:"input_digests"`
}

type ManifestImage struct {
	TaggedReference string `json:"tagged_reference"`
	Reference       string `json:"reference"`
	ManifestDigest  string `json:"manifest_digest"`
	ConfigDigest    string `json:"config_digest"`
	InputDigest     string `json:"input_digest"`
}

type ProtectedDigests struct {
	SchemaVersion int                 `json:"schema_version"`
	Environment   string              `json:"environment"`
	RegistryID    string              `json:"registry_id"`
	Digests       map[string][]string `json:"digests"`
}

type Reason struct {
	Kind      string `json:"kind"`
	Reference string `json:"reference,omitempty"`
}

type Decision struct {
	Component      string    `json:"component"`
	Repository     string    `json:"repository"`
	ImageID        string    `json:"image_id"`
	Digest         string    `json:"digest"`
	CreatedAt      time.Time `json:"created_at"`
	AgeSeconds     int64     `json:"age_seconds"`
	CompressedSize int64     `json:"compressed_size"`
	Tags           []string  `json:"tags"`
	Decision       string    `json:"decision"`
	Reasons        []Reason  `json:"reasons"`
	Execution      string    `json:"execution"`
}

type ReportSource struct {
	Repository         string `json:"repository,omitempty"`
	Commit             string `json:"commit,omitempty"`
	WorkflowRunID      string `json:"workflow_run_id,omitempty"`
	WorkflowRunAttempt string `json:"workflow_run_attempt,omitempty"`
	WorkflowRunURL     string `json:"workflow_run_url,omitempty"`
}

type ReportSummary struct {
	Repositories          int   `json:"repositories"`
	Images                int   `json:"images"`
	Retained              int   `json:"retained"`
	DeleteCandidates      int   `json:"delete_candidates"`
	EstimatedReclaimBytes int64 `json:"estimated_reclaim_bytes"`
	Deleted               int   `json:"deleted"`
	AlreadyAbsent         int   `json:"already_absent"`
}

type Report struct {
	SchemaVersion       int           `json:"schema_version"`
	GeneratedAt         time.Time     `json:"generated_at"`
	Mode                string        `json:"mode"`
	Environment         string        `json:"environment"`
	FolderID            string        `json:"folder_id"`
	RegistryID          string        `json:"registry_id"`
	TerraformLineage    string        `json:"terraform_lineage"`
	TerraformSerial     uint64        `json:"terraform_serial"`
	TerraformDigest     string        `json:"terraform_outputs_digest"`
	SafetyWindowSeconds int64         `json:"safety_window_seconds"`
	Complete            bool          `json:"complete"`
	Status              string        `json:"status"`
	Source              ReportSource  `json:"source"`
	Summary             ReportSummary `json:"summary"`
	Decisions           []Decision    `json:"decisions"`
}

type PlanConfig struct {
	Now          time.Time
	SafetyWindow time.Duration
	Mode         string
	Source       ReportSource
}
