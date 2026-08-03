// Package ydbclient owns construction of the YDB native driver and its
// database/sql adapter. Authentication is selected exclusively from the
// documented YDB_* environment variables.
package ydbclient

import (
	"context"
	"database/sql"
	"errors"
	"net/url"

	environ "github.com/ydb-platform/ydb-go-sdk-auth-environ"
	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

// Client keeps both layers alive. Call Close once during process shutdown.
type Client struct {
	DB     *sql.DB
	driver *ydb.Driver
}

// Open creates a data-query connection suitable for serializable OLTP
// transactions. Goose-specific scripting flags are stripped because they
// emulate transactions and must never be used by the operational store.
func Open(ctx context.Context, connectionString string) (*Client, error) {
	if connectionString == "" {
		return nil, errors.New("YDB_CONNECTION_STRING must not be empty")
	}
	dataDSN, err := DataDSN(connectionString)
	if err != nil {
		return nil, err
	}
	return open(ctx, dataDSN,
		// Keep OLTP on the stable Table service explicitly. Newer SDK releases
		// default database/sql to Query service, which is not enabled on every
		// YDB Serverless endpoint and can time out while creating a session.
		ydb.WithQueryService(false),
		ydb.WithAutoDeclare(),
		ydb.WithNumericArgs(),
	)
}

// OpenScripting creates the transaction-emulating database/sql connection
// required by Goose for non-transactional YDB schema statements.
func OpenScripting(ctx context.Context, connectionString string) (*Client, error) {
	if connectionString == "" {
		return nil, errors.New("YDB_CONNECTION_STRING must not be empty")
	}
	dataDSN, err := DataDSN(connectionString)
	if err != nil {
		return nil, err
	}
	return open(ctx, dataDSN,
		ydb.WithQueryService(false),
		ydb.WithDefaultQueryMode(ydb.ScriptingQueryMode),
		ydb.WithFakeTx(ydb.ScriptingQueryMode),
		ydb.WithAutoDeclare(),
		ydb.WithNumericArgs(),
	)
}

func open(ctx context.Context, connectionString string, options ...ydb.ConnectorOption) (*Client, error) {
	driver, err := ydb.Open(ctx, connectionString, environ.WithEnvironCredentials())
	if err != nil {
		return nil, err
	}
	connector, err := ydb.Connector(driver, options...)
	if err != nil {
		_ = driver.Close(ctx)
		return nil, err
	}
	return &Client{
		DB:     sql.OpenDB(connector),
		driver: driver,
	}, nil
}

func (client *Client) Close(ctx context.Context) error {
	if client == nil {
		return nil
	}
	var result error
	if client.DB != nil {
		result = client.DB.Close()
	}
	if client.driver != nil {
		result = errors.Join(result, client.driver.Close(ctx))
	}
	return result
}

func (client *Client) Table() table.Client {
	return client.driver.Table()
}

// DatabasePath returns the absolute YDB database path used by native scheme
// operations. Unlike SQL queries, DescribeTable does not resolve table names
// relative to the connection database.
func (client *Client) DatabasePath() string {
	if client == nil || client.driver == nil {
		return ""
	}
	return client.driver.Name()
}

// DataDSN removes database/sql behavior flags that are needed by Goose DDL
// scripting but would disable real transactions in the state store.
func DataDSN(connectionString string) (string, error) {
	parsed, err := url.Parse(connectionString)
	if err != nil {
		return "", err
	}
	query := parsed.Query()
	query.Del("go_query_mode")
	query.Del("go_fake_tx")
	query.Del("go_query_bind")
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}
