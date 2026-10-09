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
	subject, err := bootstrapSubjectFromEnvironment()
	if err != nil {
		fatal(err)
	}
	expected := fmt.Sprintf("BOOTSTRAP %s INTO %s", grant.UserID, grant.TenantID)
	if subject != nil {
		expected += " FOR " + subject.String()
	}
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
	var membership domain.TenantMembership
	if subject != nil {
		membership, err = store.BootstrapDevelopmentMembershipForSubject(ctx, grant, *subject)
	} else {
		membership, err = store.BootstrapDevelopmentMembership(ctx, grant)
	}
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

// Optional explicit provisioning is privileged cloud-dev bootstrap before
// first sign-in, not a browser claim or an account-link command.
func bootstrapSubjectFromEnvironment() (*domain.ExternalSubject, error) {
	p, s := os.Getenv("WEB_BOOTSTRAP_EXTERNAL_PROVIDER"), os.Getenv("WEB_BOOTSTRAP_EXTERNAL_SUBJECT")
	if p == "" && s == "" {
		return nil, nil
	}
	if p != "yandex" && p != "telegram" {
		return nil, errors.New("bootstrap external provider must be yandex or telegram")
	}
	subject := domain.ExternalSubject{Provider: domain.IdentityProvider(p), Subject: s}
	if subject.Validate() != nil {
		return nil, errors.New("bootstrap external subject is invalid")
	}
	if p == "yandex" {
		if s[0] == '0' {
			return nil, errors.New("Yandex bootstrap requires canonical numeric account id")
		}
		for _, c := range s {
			if c < '0' || c > '9' {
				return nil, errors.New("Yandex bootstrap requires numeric account id, not email or username")
			}
		}
	}
	return &subject, nil
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
