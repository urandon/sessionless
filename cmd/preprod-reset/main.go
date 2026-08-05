package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"gitcode.com/urandon/sessionless/internal/preprodreset"
	"gitcode.com/urandon/sessionless/internal/s3store"
	"gitcode.com/urandon/sessionless/internal/ydbclient"
)

func main() {
	execute := flag.Bool("execute", false, "perform the guarded cloud-dev application reset")
	flag.Parse()
	if flag.NArg() != 0 {
		log.Fatal("preprod-reset accepts only the optional --execute flag")
	}
	target := preprodreset.Target{
		Environment:    os.Getenv("APP_ENV"),
		FolderID:       os.Getenv("CLOUD_DEV_FOLDER_ID"),
		YDBConnection:  os.Getenv("YDB_CONNECTION_STRING"),
		ArtifactBucket: os.Getenv("S3_BUCKET"),
		ObjectPrefix:   os.Getenv("SESSIONLESS_RESET_OBJECT_PREFIX"),
		Confirmation:   os.Getenv("CONFIRM_CLOUD_APP_RESET"),
	}
	plan, err := preprodreset.BuildPlan(target, *execute)
	if err != nil {
		log.Fatal(err)
	}
	if !*execute {
		writeJSON(plan)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	ydb, err := ydbclient.OpenScripting(ctx, target.YDBConnection)
	if err != nil {
		log.Fatalf("open guarded reset YDB target: %v", err)
	}
	defer ydb.Close(context.Background())
	objects, err := s3store.New(ctx, s3store.Config{
		Endpoint:               targetEnv("S3_ENDPOINT"),
		Region:                 targetEnv("S3_REGION"),
		Bucket:                 target.ArtifactBucket,
		IAMToken:               os.Getenv("YC_TOKEN"),
		IAMMetadataCredentials: os.Getenv("S3_IAM_METADATA_CREDENTIALS") == "1",
	})
	if err != nil {
		log.Fatalf("open guarded reset Object Storage target: %v", err)
	}
	result, err := preprodreset.Execute(ctx, target, ydb.DB, objects)
	if err != nil {
		log.Fatal(err)
	}
	writeJSON(result)
}

func targetEnv(name string) string {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		log.Fatalf("%s must be set", name)
	}
	return value
}

func writeJSON(value any) {
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		log.Fatal(err)
	}
	if _, err := fmt.Fprintln(os.Stdout, string(encoded)); err != nil {
		log.Fatal(err)
	}
}
