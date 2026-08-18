package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/s3store"
	"gitcode.com/urandon/sessionless/internal/sessionlifecycle"
	"gitcode.com/urandon/sessionless/internal/ydbclient"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "session lifecycle operation failed:", err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) != 2 {
		return errors.New("usage: session-delete request|plan|execute|hold|release-hold")
	}
	mode := os.Args[1]
	switch mode {
	case "request", "plan", "execute", "hold", "release-hold":
	default:
		return fmt.Errorf("unknown session lifecycle operation %q", mode)
	}
	tenantID := domain.TenantID(strings.TrimSpace(os.Getenv("SESSION_DELETE_TENANT_ID")))
	sessionID := domain.SessionID(strings.TrimSpace(os.Getenv("SESSION_DELETE_SESSION_ID")))
	operatorID := domain.UserID(strings.TrimSpace(os.Getenv("SESSION_DELETE_OPERATOR_ID")))
	if err := tenantID.Validate(); err != nil {
		return err
	}
	if err := sessionID.Validate(); err != nil {
		return err
	}
	if err := operatorID.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(os.Getenv("YDB_CONNECTION_STRING")) == "" {
		return errors.New("YDB_CONNECTION_STRING is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	client, err := ydbclient.Open(ctx, os.Getenv("YDB_CONNECTION_STRING"))
	if err != nil {
		return fmt.Errorf("open YDB: %w", err)
	}
	defer client.Close(context.Background())
	store, err := ydbstore.New(client.DB, ydbstore.Options{})
	if err != nil {
		return err
	}
	now := time.Now().UTC()

	switch mode {
	case "request":
		deletion, err := store.RequestSessionDeletion(ctx, domain.SessionDeletion{
			TenantID: tenantID, SessionID: sessionID, RequestedBy: operatorID,
			Reason: strings.TrimSpace(os.Getenv("SESSION_DELETE_REASON")),
			State:  domain.SessionDeletionRequested, RequestedAt: now,
		})
		return writeResult(deletion, err)
	case "hold":
		hold, err := store.PutSessionLegalHold(ctx, domain.SessionLegalHold{
			TenantID: tenantID, SessionID: sessionID, SetBy: operatorID,
			Reason: strings.TrimSpace(os.Getenv("SESSION_DELETE_REASON")),
			State:  domain.SessionLegalHoldActive, SetAt: now,
		})
		return writeResult(hold, err)
	case "release-hold":
		hold, err := store.ReleaseSessionLegalHold(ctx, tenantID, sessionID, operatorID, now)
		return writeResult(hold, err)
	}

	blobs, err := s3store.New(ctx, s3store.Config{
		Endpoint: os.Getenv("S3_ENDPOINT"), Region: os.Getenv("S3_REGION"), Bucket: os.Getenv("S3_BUCKET"),
		AccessKeyID: os.Getenv("S3_ACCESS_KEY_ID"), SecretAccessKey: os.Getenv("S3_SECRET_ACCESS_KEY"),
		ForcePathStyle:         envBool("S3_FORCE_PATH_STYLE"),
		IAMMetadataCredentials: envBool("S3_IAM_METADATA_CREDENTIALS"), IAMToken: os.Getenv("YC_TOKEN"),
	})
	if err != nil {
		return fmt.Errorf("open Object Storage: %w", err)
	}
	service, err := sessionlifecycle.New(store, blobs, 0, 0)
	if err != nil {
		return err
	}
	plan, err := service.Plan(ctx, tenantID, sessionID)
	if err != nil {
		return err
	}
	if plan.Deletion.RequestedBy != operatorID {
		return domain.ErrMembershipDenied
	}
	if mode == "plan" {
		return writeResult(plan, nil)
	}
	deletion, err := service.Execute(
		ctx, tenantID, sessionID, os.Getenv("CONFIRM_SESSION_DELETE"), now,
	)
	return writeResult(deletion, err)
}

func writeResult(value any, err error) error {
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(value)
}

func envBool(name string) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(name))) {
	case "1", "true", "yes":
		return true
	default:
		return false
	}
}
