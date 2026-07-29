package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gitcode.com/urandon/sessionless/internal/ydbclient"
	"gitcode.com/urandon/sessionless/internal/ydbpartition"
)

func main() {
	if err := run(); err != nil {
		slog.Error("YDB partition inspection failed", "error", err)
		os.Exit(1)
	}
}

func run() error {
	connectionString := os.Getenv("YDB_CONNECTION_STRING")
	if connectionString == "" {
		return errors.New("YDB_CONNECTION_STRING is required")
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	ctx, timeout := context.WithTimeout(ctx, time.Minute)
	defer timeout()

	client, err := ydbclient.Open(ctx, connectionString)
	if err != nil {
		return err
	}
	defer client.Close(context.Background())
	report, err := ydbpartition.Inspect(ctx, client.Table(), client.DatabasePath())
	if err != nil {
		return err
	}
	output, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(output))
	if !report.Valid {
		return errors.New("YDB partitioning contract drift detected")
	}
	return nil
}
