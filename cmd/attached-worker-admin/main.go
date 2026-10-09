// attached-worker-admin is an operator-only private-file handoff. It never
// starts a worker, enables a provider, or exports bootstrap material to stdout.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"os"
	"strings"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworker"
	onboarding "gitcode.com/urandon/sessionless/internal/attachedworkeronboarding"
	"gitcode.com/urandon/sessionless/internal/attachedworkeronboardingserver"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/idgen"
	"gitcode.com/urandon/sessionless/internal/ydbclient"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

type adminService interface {
	PrepareGrant(context.Context, domain.TenantID, domain.UserID, attachedworker.CreateEnrollmentRequest, string) (onboarding.GrantV1, error)
	PersistGrant(context.Context, onboarding.GrantV1) error
	Claim(context.Context, onboarding.ClaimV1) (onboarding.ReceiptV1, error)
	Head(context.Context, domain.TenantID, domain.UserID, domain.AttachedWorkerID) (domain.AttachedWorker, error)
	Rotate(context.Context, onboarding.RotationV1) (onboarding.ReceiptV1, error)
}

type adminClock struct{}

func (adminClock) Now() time.Time { return time.Now().UTC() }

type adminResult struct {
	Version     uint32                          `json:"version"`
	Code        string                          `json:"code"`
	WorkerID    domain.AttachedWorkerID         `json:"worker_id,omitempty"`
	ResourceID  domain.SubscriptionConnectionID `json:"resource_id,omitempty"`
	Entitlement domain.EntitlementState         `json:"entitlement,omitempty"`
	Quota       domain.ProviderQuotaState       `json:"quota,omitempty"`
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	open := func(ctx context.Context) (adminService, func(), error) {
		connection := os.Getenv("YDB_CONNECTION_STRING")
		if connection == "" {
			return nil, nil, errors.New("missing connection")
		}
		client, err := ydbclient.Open(ctx, connection)
		if err != nil {
			return nil, nil, err
		}
		closeClient := func() {
			closeCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
			defer stop()
			_ = client.Close(closeCtx)
		}
		store, err := ydbstore.New(client.DB, ydbstore.Options{})
		if err != nil {
			closeClient()
			return nil, nil, err
		}
		service, err := attachedworkeronboardingserver.New(attachedworker.Config{
			Clock: adminClock{}, IDs: idgen.New(), MaxEnrollmentTTL: 10 * time.Minute, EnrollmentRetention: 24 * time.Hour,
		}, store.OnboardingStore())
		if err != nil {
			closeClient()
			return nil, nil, err
		}
		return service, closeClient, nil
	}
	os.Exit(runAdmin(ctx, os.Args[1:], os.Stdin, os.Stdout, open, adminClock{}.Now))
}

type adminOpener func(context.Context) (adminService, func(), error)

func runAdmin(ctx context.Context, args []string, input io.Reader, output io.Writer, open adminOpener, now func() time.Time) int {
	if output == nil {
		return 1
	}
	fail := func(code string) int {
		_ = json.NewEncoder(output).Encode(adminResult{Version: 1, Code: code})
		return 1
	}
	if ctx == nil || ctx.Err() != nil || len(args) == 0 || input == nil || output == nil || open == nil || now == nil {
		return fail("invalid")
	}
	command := args[0]
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	tenant := flags.String("tenant", "", "exact active tenant")
	owner := flags.String("owner", "", "exact authenticated owner user ID")
	worker := flags.String("worker", "", "exact owned worker")
	display := flags.String("display-name", "", "bounded enrollment name")
	audience := flags.String("audience", "", "exact enrollment audience")
	origin := flags.String("origin", "", "canonical HTTPS control origin")
	grantPath := flags.String("grant-file", "", "private grant handoff file")
	claimPath := flags.String("claim-file", "", "private signed claim handoff")
	rotationPath := flags.String("rotation-file", "", "private signed rotation handoff")
	receiptPath := flags.String("receipt-file", "", "private server receipt or worker head")
	ttl := flags.Duration("ttl", 5*time.Minute, "short enrollment TTL")
	if flags.Parse(args[1:]) != nil || flags.NArg() != 0 || domain.TenantID(*tenant).Validate() != nil || domain.UserID(*owner).Validate() != nil {
		return fail("invalid")
	}
	// Reject unrelated flags so a stale command cannot silently ignore authority.
	allowed := map[string]bool{"tenant": true, "owner": true}
	switch command {
	case "enroll-create":
		for _, k := range []string{"display-name", "audience", "origin", "grant-file", "ttl"} {
			allowed[k] = true
		}
		if *grantPath == "" || *display == "" || *audience == "" || onboarding.ValidateOrigin(*origin) != nil || *ttl <= 0 || *ttl > 10*time.Minute {
			return fail("invalid")
		}
	case "enroll-claim":
		allowed["claim-file"] = true
		allowed["receipt-file"] = true
		if *claimPath == "" || *receiptPath == "" {
			return fail("invalid")
		}
	case "worker-head":
		allowed["worker"] = true
		allowed["receipt-file"] = true
		if domain.AttachedWorkerID(*worker).Validate() != nil || *receiptPath == "" {
			return fail("invalid")
		}
	case "identity-rotate":
		allowed["rotation-file"] = true
		allowed["receipt-file"] = true
		if *rotationPath == "" || *receiptPath == "" {
			return fail("invalid")
		}
	default:
		return fail("invalid")
	}
	valid := true
	flags.Visit(func(f *flag.Flag) { valid = valid && allowed[f.Name] })
	if !valid {
		return fail("invalid")
	}
	// Deliberate operator confirmation precedes opening any live dependencies.
	expected := "ONBOARD " + command + " " + *owner + " INTO " + *tenant
	confirmation, err := bufio.NewReader(io.LimitReader(input, 1025)).ReadString('\n')
	if (err != nil && !errors.Is(err, io.EOF)) || len(confirmation) > 1024 || strings.TrimSpace(confirmation) != expected {
		return fail("confirmation_required")
	}
	service, closeService, err := open(ctx)
	if err != nil || service == nil || closeService == nil {
		return fail("backend_unavailable")
	}
	defer closeService()
	result := adminResult{Version: 1, Code: "ready"}
	switch command {
	case "enroll-create":
		grant, readErr := onboarding.ReadGrant(*grantPath)
		if errors.Is(readErr, os.ErrNotExist) {
			grant, err = service.PrepareGrant(ctx, domain.TenantID(*tenant), domain.UserID(*owner), attachedworker.CreateEnrollmentRequest{DisplayName: *display, Audience: *audience, ExpiresAt: now().UTC().Add(*ttl)}, *origin)
			if err != nil {
				return fail("denied")
			}
			// Secret and IDs must be durable before the first database mutation.
			if onboarding.WriteGrant(*grantPath, grant) != nil {
				return fail("handoff_ambiguous")
			}
		} else if readErr != nil {
			return fail("handoff_invalid")
		}
		if grant.Enrollment.TenantID != domain.TenantID(*tenant) || grant.Enrollment.OwnerUserID != domain.UserID(*owner) || grant.Enrollment.DisplayName != *display || grant.Enrollment.Audience != *audience || grant.ControlPlaneOrigin != *origin {
			return fail("scope_conflict")
		}
		if service.PersistGrant(ctx, grant) != nil {
			return fail("denied")
		}
		result.WorkerID = grant.Enrollment.WorkerID
	case "enroll-claim":
		claim, e := onboarding.ReadClaim(*claimPath)
		if e != nil {
			return fail("handoff_invalid")
		}
		if claim.Enrollment.TenantID != domain.TenantID(*tenant) || claim.Enrollment.OwnerUserID != domain.UserID(*owner) {
			return fail("scope_conflict")
		}
		receipt, e := service.Claim(ctx, claim)
		if e != nil {
			return fail("denied")
		}
		if onboarding.WriteReceipt(*receiptPath, receipt) != nil {
			return fail("handoff_ambiguous")
		}
		result.WorkerID = receipt.Worker.ID
		result.ResourceID = receipt.ResourceID
		result.Entitlement = receipt.Entitlement
		result.Quota = receipt.Quota
	case "worker-head":
		head, e := service.Head(ctx, domain.TenantID(*tenant), domain.UserID(*owner), domain.AttachedWorkerID(*worker))
		if e != nil {
			return fail("denied")
		}
		if onboarding.WriteWorkerHead(*receiptPath, head) != nil {
			return fail("handoff_ambiguous")
		}
		result.WorkerID = head.ID
	case "identity-rotate":
		rotation, e := onboarding.ReadRotation(*rotationPath)
		if e != nil {
			return fail("handoff_invalid")
		}
		if rotation.Worker.TenantID != domain.TenantID(*tenant) || rotation.Worker.OwnerUserID != domain.UserID(*owner) {
			return fail("scope_conflict")
		}
		receipt, e := service.Rotate(ctx, rotation)
		if e != nil {
			return fail("denied")
		}
		if onboarding.WriteReceipt(*receiptPath, receipt) != nil {
			return fail("handoff_ambiguous")
		}
		result.WorkerID = receipt.Worker.ID
		result.ResourceID = receipt.ResourceID
		result.Entitlement = receipt.Entitlement
		result.Quota = receipt.Quota
	}
	if json.NewEncoder(output).Encode(result) != nil {
		return 1
	}
	return 0
}
