package ydbstore

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/telegramoidc"
	"gitcode.com/urandon/sessionless/internal/yandexoauth"
	"github.com/golang-jwt/jwt/v4"
)

const accountConnectedBaseV2 = "https://account.example.com"

type accountConnectedClockV2 struct{ f *identityProofSQLV2 }

func (c accountConnectedClockV2) Now() time.Time { return c.f.at }

type accountConnectedTransportV2 func(*http.Request) (*http.Response, error)

func (f accountConnectedTransportV2) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// Three exact existing SQL surfaces composed over ONE original snapshot. The
// extra membership LIST is ordinary-login DATA; session audit validation reuses
// its existing fixture verbatim. No unrecognized statement is accepted.
type accountConnectedSQLV2 struct {
	*identityProofSQLV2
	// One additional real-written operation suffices for the lost-cookie
	// predecessor recovery case. Original operation/child owners stay unchanged.
	extra, pendingExtra map[domain.SecretDigest]string
}
type accountConnectedConnV2 struct {
	*identityProofConnV2
	owner *accountConnectedSQLV2
}

func (f *accountConnectedSQLV2) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := f.identityProofSQLV2.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &accountConnectedConnV2{identityProofConnV2: conn.(*identityProofConnV2), owner: f}, nil
}

type accountConnectedTxV2 struct {
	original driver.Tx
	owner    *accountConnectedSQLV2
}

func (c *accountConnectedConnV2) BeginTx(ctx context.Context, options driver.TxOptions) (driver.Tx, error) {
	tx, err := c.identityProofConnV2.BeginTx(ctx, options)
	if err != nil {
		return nil, err
	}
	c.owner.base.mu.Lock()
	c.owner.pendingExtra = make(map[domain.SecretDigest]string, len(c.owner.extra))
	for key, value := range c.owner.extra {
		c.owner.pendingExtra[key] = value
	}
	c.owner.base.mu.Unlock()
	return &accountConnectedTxV2{original: tx, owner: c.owner}, nil
}
func (tx *accountConnectedTxV2) Commit() error {
	tx.owner.base.mu.Lock()
	before := tx.owner.base.commits
	tx.owner.base.mu.Unlock()
	err := tx.original.Commit()
	tx.owner.base.mu.Lock()
	defer tx.owner.base.mu.Unlock()
	if tx.owner.base.commits == before+1 {
		tx.owner.extra = tx.owner.pendingExtra
	}
	tx.owner.pendingExtra = nil
	return err
}
func (tx *accountConnectedTxV2) Rollback() error {
	err := tx.original.Rollback()
	tx.owner.base.mu.Lock()
	tx.owner.pendingExtra = nil
	tx.owner.base.mu.Unlock()
	return err
}
func (c *accountConnectedConnV2) QueryContext(ctx context.Context, statement string, args []driver.NamedValue) (driver.Rows, error) {
	if identityRowsV2Statement(statement) == identityOperationRowSQLV2 {
		values := identityRowsV2Args(args)
		if len(values) == 2 && values[1] != string(c.owner.digest) {
			c.owner.base.mu.Lock()
			defer c.owner.base.mu.Unlock()
			if err := c.owner.base.statement(ctx, statement, args, false); err != nil {
				return nil, err
			}
			digest, ok := values[1].(string)
			if !ok || domain.SecretDigest(digest).Validate("test.operation_digest") != nil || values[0] != identityRowsV2ExpectedBucket(digest) || c.owner.base.pending == nil {
				return nil, errors.New("additional operation lookup requires exact bounded key/transaction")
			}
			rows := &identityRowsV2Rows{fixture: c.owner.base, columns: []string{"value"}}
			if raw, found := c.owner.pendingExtra[domain.SecretDigest(digest)]; found {
				if len(raw) > domain.IdentityAuthMaxRecordBytesV2+1 {
					raw = raw[:domain.IdentityAuthMaxRecordBytesV2+1]
				}
				rows.values = [][]driver.Value{{raw}}
			}
			return rows, nil
		}
	}
	if identityRowsV2Statement(statement) != "SELECT record FROM tenant_memberships WHERE user_bucket = $1 AND user_id = $2 ORDER BY tenant_id LIMIT $3" {
		return c.identityProofConnV2.QueryContext(ctx, statement, args)
	}
	f := c.owner.identityProofSQLV2
	f.base.mu.Lock()
	defer f.base.mu.Unlock()
	values := identityRowsV2Args(args)
	if len(values) != 3 || values[0] != identityRowsV2ExpectedBucket("rows-user") || values[1] != "rows-user" || values[2] != int64(200) || f.base.pending != nil || ctx.Err() != nil {
		return nil, errors.New("ordinary membership LIST requires exact own key/bound outside transaction")
	}
	if f.base.queries+f.base.writes >= f.base.queryLimit {
		return nil, errors.New("fixture statement ceiling exceeded")
	}
	f.base.queries++
	f.base.statements = append(f.base.statements, identityRowsV2Statement(statement))
	var rows [][]driver.Value
	for _, raw := range f.base.durable.members {
		var member domain.TenantMembership
		if json.Unmarshal([]byte(raw), &member) != nil || member.UserID != "rows-user" {
			return nil, errors.New("foreign membership fixture")
		}
		rows = append(rows, []driver.Value{raw})
	}
	return &compactionSourceRows{rows: rows}, nil
}
func (c *accountConnectedConnV2) ExecContext(ctx context.Context, statement string, args []driver.NamedValue) (driver.Result, error) {
	if identityRowsV2Statement(statement) == identityOperationWriteSQLTestV2 {
		values := identityRowsV2Args(args)
		if len(values) == 11 && values[1] != string(c.owner.digest) {
			raw, ok := values[9].(string)
			operation, err := domain.DecodeIdentityOperationV2([]byte(raw))
			if !ok || err != nil {
				return c.identityProofConnV2.ExecContext(ctx, statement, args)
			}
			c.owner.base.mu.Lock()
			defer c.owner.base.mu.Unlock()
			if err := c.owner.base.statement(ctx, statement, args, true); err != nil {
				return nil, err
			}
			if operation.Pending == nil || c.owner.base.pending == nil {
				return nil, errors.New("additional operation must be original canonical pending write")
			}
			want := []any{identityRowsV2ExpectedBucket(string(operation.OperationDigest)), string(operation.OperationDigest), string(operation.Pending.Scope.BrowserBindingDigest), "", "", "", operation.CreatedAt, operation.ExpiresAt, time.Unix(0, 0).UTC(), raw, operation.DeleteAfter}
			if !reflect.DeepEqual(values, want) || len(c.owner.pendingExtra) != 0 {
				return nil, errors.New("additional operation exact scalar/canonical correspondence or fixture bound")
			}
			c.owner.pendingExtra[operation.OperationDigest] = raw
			return driver.RowsAffected(1), nil
		}
	}
	if identityRowsV2Statement(statement) == sessionServingAuditSQLTestV2 {
		return (&sessionServingConnV2{identityOperationConnV2: c.identityProofConnV2.identityOperationConnV2, f: &sessionServingSQLV2{identityOperationSQLV2: c.owner.identityOperationSQLV2}}).ExecContext(ctx, statement, args)
	}
	return c.identityProofConnV2.ExecContext(ctx, statement, args)
}

type accountConnectedPeerV2 struct {
	t                           *testing.T
	f                           *identityProofSQLV2
	key                         *rsa.PrivateKey
	authorization               map[domain.IdentityProvider]url.Values
	used                        map[string]bool
	tokens, infos, jwks, closed int
	deny                        bool
}
type accountConnectedBodyV2 struct {
	io.Reader
	owner *accountConnectedPeerV2
}

func (b accountConnectedBodyV2) Close() error { b.owner.closed++; return nil }
func (p *accountConnectedPeerV2) response(r *http.Request, body string) (*http.Response, error) {
	return &http.Response{StatusCode: 200, Header: make(http.Header), Body: accountConnectedBodyV2{strings.NewReader(body), p}, Request: r}, nil
}
func (p *accountConnectedPeerV2) RoundTrip(r *http.Request) (*http.Response, error) {
	provider := domain.IdentityProviderYandex
	if r.URL.Host == "oauth.telegram.org" {
		provider = domain.IdentityProviderTelegram
	}
	if p.f.base.pending != nil {
		p.t.Error("provider I/O occurred inside DB transaction")
	}
	if r.URL.String() == telegramoidc.DefaultJWKSURL {
		p.jwks++
		body, _ := json.Marshal(map[string]any{"keys": []any{map[string]string{"kid": "account-key", "kty": "RSA", "alg": "RS256", "use": "sig", "n": base64.RawURLEncoding.EncodeToString(p.key.N.Bytes()), "e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(p.key.E)).Bytes())}}})
		p.f.at = p.f.at.Add(time.Millisecond)
		return p.response(r, string(body))
	}
	if r.Method == http.MethodPost && (r.URL.String() == yandexoauth.DefaultTokenEndpoint || r.URL.String() == telegramoidc.DefaultTokenEndpoint) {
		p.tokens++
		if err := r.ParseForm(); err != nil {
			return nil, err
		}
		code := r.PostForm.Get("code")
		if code == "" || p.used[code] {
			return nil, errors.New("code retried")
		}
		p.used[code] = true
		q := p.authorization[provider]
		if q == nil {
			return nil, errors.New("no actual authorization URL")
		}
		challenge := sha256.Sum256([]byte(r.PostForm.Get("code_verifier")))
		if base64.RawURLEncoding.EncodeToString(challenge[:]) != q.Get("code_challenge") || r.PostForm.Get("redirect_uri") != q.Get("redirect_uri") {
			p.t.Error("actual exchange did not bind original PKCE/callback")
		}
		childRaw := p.f.children[domain.DigestSecret(q.Get("state"))]
		if len(childRaw) != 11 {
			return nil, errors.New("actual challenge absent before provider exchange")
		}
		child, err := domain.DecodeIdentityChallengeV2([]byte(childRaw[9].(string)))
		if err != nil || child.ConsumedAt == nil || child.Secrets != nil || childRaw[3] != "" || childRaw[4] != "" {
			p.t.Error("provider I/O preceded committed scalar/JSON scrub")
		}
		id, secret, ok := r.BasicAuth()
		wantID, wantSecret := "account-yandex", "secret-yandex"
		if provider == domain.IdentityProviderTelegram {
			wantID, wantSecret = "100000", "secret-telegram"
		}
		if !ok || id != wantID || secret != wantSecret {
			p.t.Error("provider client crossed")
		}
		if p.deny {
			return &http.Response{StatusCode: 400, Header: make(http.Header), Body: accountConnectedBodyV2{strings.NewReader(`{"error":"invalid_grant"}`), p}, Request: r}, nil
		}
		if provider == domain.IdentityProviderYandex {
			p.f.at = p.f.at.Add(time.Millisecond)
			return p.response(r, `{"access_token":"synthetic-access","token_type":"bearer","expires_in":3600}`)
		}
		claims := jwt.MapClaims{"iss": telegramoidc.DefaultIssuer, "aud": "100000", "sub": "909090", "iat": p.f.at.Unix(), "exp": p.f.at.Add(time.Hour).Unix(), "nonce": q.Get("nonce"), "auth_time": p.f.at.Unix()}
		token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
		token.Header["kid"] = "account-key"
		signed, err := token.SignedString(p.key)
		if err != nil {
			return nil, err
		}
		body, _ := json.Marshal(map[string]string{"id_token": signed})
		p.f.at = p.f.at.Add(time.Millisecond)
		return p.response(r, string(body))
	}
	if r.URL.String() == yandexoauth.DefaultInfoEndpoint+"?format=json" {
		p.infos++
		if r.Header.Get("Authorization") != "OAuth synthetic-access" {
			p.t.Error("account API credential crossed")
		}
		p.f.at = p.f.at.Add(time.Millisecond)
		return p.response(r, `{"id":"424242","client_id":"account-yandex"}`)
	}
	return nil, errors.New("unconfigured HTTP endpoint")
}

// Test-only raw composition. No BFF import, production activation, supplied
// proof, or authority decision is exposed by this fixture.
type IdentityAccountConnectedFixtureV2 struct {
	Store           *Store
	Service         ports.IdentityAccountServiceV2
	Yandex          *yandexoauth.Provider
	Telegram        *telegramoidc.Provider
	Clock           ports.Clock
	OriginalMembers map[string]string
	sql             *accountConnectedSQLV2
	f               *identityProofSQLV2
	peer            *accountConnectedPeerV2
	owner           *testing.T
}

type IdentityAccountConnectedReverseV2 struct {
	UserID    domain.UserID
	Subject   domain.ExternalSubject
	CreatedAt time.Time
}
type IdentityAccountConnectedSnapshotV2 struct {
	Forward, Sessions, Members          map[string]string
	Reverse                             map[string]IdentityAccountConnectedReverseV2
	Operation                           string
	AdditionalOperations                map[domain.SecretDigest]string
	Children                            map[domain.SecretDigest][]any
	Writes, Tokens, Infos, JWKS, Closed int
}

func NewIdentityAccountConnectedFixtureV2(t *testing.T, provider domain.IdentityProvider, prelinked bool) *IdentityAccountConnectedFixtureV2 {
	return newIdentityAccountConnectedFixtureV2(t, provider, prelinked, nil)
}

func newIdentityAccountConnectedFixtureV2(t *testing.T, provider domain.IdentityProvider, prelinked bool, override ports.IdentityProviderProverV1) *IdentityAccountConnectedFixtureV2 {
	t.Helper()
	f, store, _, _, config := newIdentityProofStorageV2(t, domain.IdentityOperationLinkV2)
	// All identity/client fixture DATA is selected BEFORE the actual operation.
	state, identity, session, _ := identityRowsV2Data(t)
	delete(state.forward, identityRowsV2SubjectKey(identity.Subject))
	delete(state.reverse, identityRowsV2ReverseKey(identity.UserID, identity.Subject))
	identity.Subject = domain.ExternalSubject{Provider: provider, Subject: "424242"}
	if provider == domain.IdentityProviderTelegram {
		identity.Subject.Subject = "909090"
	}
	session.Session.AuthenticatedSubject = identity.Subject
	state.forward[identityRowsV2SubjectKey(identity.Subject)] = identityRowsV2JSON(t, identity)
	state.reverse[identityRowsV2ReverseKey(identity.UserID, identity.Subject)] = identityRowsV2Projection{user: identity.UserID, subject: identity.Subject, created: identity.CreatedAt}
	state.sessions[string(session.Session.SessionDigest)] = identityRowsV2JSON(t, session)
	if prelinked {
		remaining := identity
		remaining.Subject = domain.ExternalSubject{Provider: domain.IdentityProviderTelegram, Subject: "909090"}
		if provider == domain.IdentityProviderTelegram {
			remaining.Subject = domain.ExternalSubject{Provider: domain.IdentityProviderYandex, Subject: "424242"}
		}
		state.forward[identityRowsV2SubjectKey(remaining.Subject)] = identityRowsV2JSON(t, remaining)
		state.reverse[identityRowsV2ReverseKey(remaining.UserID, remaining.Subject)] = identityRowsV2Projection{user: remaining.UserID, subject: remaining.Subject, created: remaining.CreatedAt}
	}
	f.base.durable = state.clone()
	config.Providers = []domain.IdentityProviderBindingV2{{Provider: domain.IdentityProviderYandex, Issuer: domain.YandexOAuthIssuer, ClientID: "account-yandex", CallbackURI: accountConnectedBaseV2 + "/auth/login/callback"}, {Provider: domain.IdentityProviderTelegram, Issuer: telegramoidc.DefaultIssuer, ClientID: "100000", CallbackURI: accountConnectedBaseV2 + "/auth/telegram/callback"}}
	store.webIdentityV2 = &config
	composition := &accountConnectedSQLV2{identityProofSQLV2: f, extra: make(map[domain.SecretDigest]string)}
	db := sql.OpenDB(composition)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	store.db = db
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	peer := &accountConnectedPeerV2{t: t, f: f, key: key, authorization: make(map[domain.IdentityProvider]url.Values), used: make(map[string]bool)}
	client := &http.Client{Transport: accountConnectedTransportV2(peer.RoundTrip)}
	yandex, err := yandexoauth.New(yandexoauth.Config{ClientID: "account-yandex", ClientSecret: "secret-yandex", RedirectURI: config.Providers[0].CallbackURI, HTTPClient: client})
	if err != nil {
		t.Fatal(err)
	}
	telegram, err := telegramoidc.New(telegramoidc.Config{ClientID: "100000", ClientSecret: "secret-telegram", RedirectURI: config.Providers[1].CallbackURI, HTTPClient: client})
	if err != nil {
		t.Fatal(err)
	}
	options := accountOptionsV2(config)
	options.Providers = []IdentityAccountProviderV2{{Binding: config.Providers[0], Prover: yandex}, {Binding: config.Providers[1], Prover: telegram}}
	if override != nil {
		for i := range options.Providers {
			if options.Providers[i].Binding.Provider == provider {
				options.Providers[i].Prover = override
			}
		}
	}
	service, err := NewIdentityAccountServiceV2(store, options)
	if err != nil {
		t.Fatal(err)
	}
	f.digest = domain.DigestSecret("op_" + base64.RawURLEncoding.EncodeToString(accountRandomV2()[:32]))
	return &IdentityAccountConnectedFixtureV2{Store: store, Service: service, Yandex: yandex, Telegram: telegram, Clock: accountConnectedClockV2{f}, f: f, peer: peer, sql: composition, owner: t, OriginalMembers: state.clone().members}
}

func (f *IdentityAccountConnectedFixtureV2) Advance(d time.Duration) { f.f.at = f.f.at.Add(d) }

// Authorization records only the raw URL query observed by the external test.
// Actual token/nonce/PKCE validation remains inside the real adapters.
func (f *IdentityAccountConnectedFixtureV2) Authorization(provider domain.IdentityProvider, values url.Values) {
	copied := make(url.Values, len(values))
	for key, value := range values {
		copied[key] = append([]string(nil), value...)
	}
	f.peer.authorization[provider] = copied
}

func (f *IdentityAccountConnectedFixtureV2) Snapshot() IdentityAccountConnectedSnapshotV2 {
	f.owner.Helper()
	ctx, cancel := context.WithTimeout(f.owner.Context(), 5*time.Second)
	defer cancel()
	conn, err := f.Store.db.Conn(ctx)
	if err != nil {
		f.owner.Fatal("raw snapshot pool acquire", err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			f.owner.Error("raw snapshot pool release", err)
		}
	}()
	f.f.base.mu.Lock()
	defer f.f.base.mu.Unlock()
	state := f.f.base.durable.clone()
	reverse := make(map[string]IdentityAccountConnectedReverseV2, len(state.reverse))
	for key, value := range state.reverse {
		reverse[key] = IdentityAccountConnectedReverseV2{UserID: value.user, Subject: value.subject, CreatedAt: value.created}
	}
	extra := make(map[domain.SecretDigest]string, len(f.sql.extra))
	for key, value := range f.sql.extra {
		extra[key] = value
	}
	return IdentityAccountConnectedSnapshotV2{Forward: state.forward, Sessions: state.sessions, Members: state.members, Reverse: reverse, Operation: f.f.operation, AdditionalOperations: extra, Children: cloneIdentityProofRowsV2(f.f.children), Writes: f.f.base.writes, Tokens: f.peer.tokens, Infos: f.peer.infos, JWKS: f.peer.jwks, Closed: f.peer.closed}
}
