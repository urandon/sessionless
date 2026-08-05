package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ydbclient"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

func main() {
	if len(os.Args) != 1 {
		fatal(errors.New("web bootstrap accepts no command-line arguments; use documented environment values and stdin confirmation"))
	}
	grant, err := grantFromEnvironment(time.Now().UTC())
	if err != nil {
		fatal(err)
	}
	expected := fmt.Sprintf("BOOTSTRAP %s INTO %s", grant.UserID, grant.TenantID)
	fmt.Fprintf(os.Stderr, "Type %q to create the audited cloud-dev membership: ", expected)
	confirmation, err := bufio.NewReader(os.Stdin).ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		fatal(fmt.Errorf("read confirmation: %w", err))
	}
	if strings.TrimSpace(confirmation) != expected {
		fatal(errors.New("confirmation did not match; no membership was changed"))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, err := ydbclient.Open(ctx, os.Getenv("YDB_CONNECTION_STRING"))
	if err != nil {
		fatal(fmt.Errorf("open YDB: %w", err))
	}
	defer client.Close(context.Background())
	store, err := ydbstore.New(client.DB, ydbstore.Options{})
	if err != nil {
		fatal(err)
	}
	membership, err := store.BootstrapDevelopmentMembership(ctx, grant)
	if err != nil {
		fatal(err)
	}
	if err := json.NewEncoder(os.Stdout).Encode(map[string]any{
		"status": "ready", "tenant_id": membership.TenantID, "user_id": membership.UserID,
		"role": membership.Role, "security_version": membership.SecurityVersion,
	}); err != nil {
		fatal(err)
	}
}

func grantFromEnvironment(now time.Time) (domain.DevelopmentBootstrapGrant, error) {
	role := domain.TenantMembershipRole(os.Getenv("WEB_BOOTSTRAP_ROLE"))
	grant := domain.DevelopmentBootstrapGrant{
		TenantID: domain.TenantID(os.Getenv("WEB_BOOTSTRAP_TENANT_ID")),
		UserID:   domain.UserID(os.Getenv("WEB_BOOTSTRAP_USER_ID")),
		Role:     role, Environment: os.Getenv("SESSIONLESS_ENVIRONMENT"),
		Operator: os.Getenv("WEB_BOOTSTRAP_OPERATOR"), Reason: os.Getenv("WEB_BOOTSTRAP_REASON"),
		GrantedAt: now,
	}
	if err := grant.Validate(); err != nil {
		return domain.DevelopmentBootstrapGrant{}, err
	}
	if os.Getenv("YDB_CONNECTION_STRING") == "" {
		return domain.DevelopmentBootstrapGrant{}, errors.New("YDB_CONNECTION_STRING is required")
	}
	return grant, nil
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "web bootstrap failed:", err)
	os.Exit(1)
}
