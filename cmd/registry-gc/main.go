package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gitcode.com/urandon/sessionless/internal/registrygc"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	var inventoryPath, manifestsDir, reportJSON, reportMarkdown string
	var protectedPath, mode, confirmation string
	var safetyWindow time.Duration
	flag.StringVar(&inventoryPath, "inventory", "", "Terraform evidence JSON file")
	flag.StringVar(&manifestsDir, "manifests-dir", "", "directory containing exactly three deployment manifests")
	flag.StringVar(&reportJSON, "report-json", "", "machine-readable report output")
	flag.StringVar(&reportMarkdown, "report-markdown", "", "human-readable report output")
	flag.StringVar(&protectedPath, "protected-digests", "", "optional repository-to-digests JSON file")
	flag.StringVar(&mode, "mode", registrygc.ModeDryRun, "dry-run or delete")
	flag.StringVar(&confirmation, "confirm", "", "exact live deletion confirmation")
	flag.DurationVar(&safetyWindow, "safety-window", 48*time.Hour, "minimum image age before deletion")
	flag.Parse()
	if inventoryPath == "" || manifestsDir == "" || reportJSON == "" || reportMarkdown == "" {
		return errors.New("--inventory, --manifests-dir, --report-json, and --report-markdown are required")
	}
	inventory, err := registrygc.LoadInventory(inventoryPath)
	if err != nil {
		return err
	}
	manifests, err := registrygc.LoadManifestDir(manifestsDir)
	if err != nil {
		return err
	}
	protected := registrygc.ProtectedDigests{
		SchemaVersion: registrygc.SchemaVersion, Environment: inventory.Environment,
		RegistryID: inventory.RegistryID, Digests: map[string][]string{},
	}
	if protectedPath != "" {
		protected, err = registrygc.LoadProtectedDigests(protectedPath)
		if err != nil {
			return err
		}
	}
	if mode == registrygc.ModeDelete {
		if confirmation == "" {
			confirmation = os.Getenv("REGISTRY_GC_CONFIRM")
		}
		expected := inventory.Environment + ":" + inventory.RegistryID
		if confirmation != expected {
			return fmt.Errorf("live deletion requires --confirm or REGISTRY_GC_CONFIRM=%s", expected)
		}
	}
	cloud, err := registrygc.NewYandexCloud(registrygc.YandexConfig{
		Token:                     os.Getenv("YC_TOKEN"),
		ContainerRegistryEndpoint: os.Getenv("YANDEX_CONTAINER_REGISTRY_API_ENDPOINT"),
		ServerlessEndpoint:        os.Getenv("YANDEX_SERVERLESS_CONTAINERS_API_ENDPOINT"),
		OperationEndpoint:         os.Getenv("YANDEX_OPERATION_API_ENDPOINT"),
	})
	if err != nil {
		return err
	}
	defer cloud.Close()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	report, runErr := registrygc.Run(ctx, registrygc.PlanConfig{
		Now: time.Now().UTC(), SafetyWindow: safetyWindow, Mode: mode,
		Source: registrygc.ReportSource{
			Repository: os.Getenv("GITHUB_REPOSITORY"), Commit: os.Getenv("GITHUB_SHA"),
			WorkflowRunID: os.Getenv("GITHUB_RUN_ID"), WorkflowRunAttempt: os.Getenv("GITHUB_RUN_ATTEMPT"),
			WorkflowRunURL: githubRunURL(),
		},
	}, inventory, manifests, protected, cloud)
	if report.SchemaVersion != 0 {
		if err := writeReports(reportJSON, reportMarkdown, report); err != nil {
			return errors.Join(runErr, err)
		}
	}
	return runErr
}

func githubRunURL() string {
	if value := os.Getenv("GITHUB_RUN_URL"); value != "" {
		return value
	}
	server, repository, runID := os.Getenv("GITHUB_SERVER_URL"), os.Getenv("GITHUB_REPOSITORY"), os.Getenv("GITHUB_RUN_ID")
	if server == "" || repository == "" || runID == "" {
		return ""
	}
	return server + "/" + repository + "/actions/runs/" + runID
}

func writeReports(jsonPath, markdownPath string, report registrygc.Report) error {
	jsonFile, err := os.Create(jsonPath)
	if err != nil {
		return err
	}
	jsonErr := registrygc.WriteJSON(jsonFile, report)
	jsonErr = errors.Join(jsonErr, jsonFile.Close())
	markdownFile, err := os.Create(markdownPath)
	if err != nil {
		return errors.Join(jsonErr, err)
	}
	markdownErr := registrygc.WriteMarkdown(markdownFile, report)
	markdownErr = errors.Join(markdownErr, markdownFile.Close())
	return errors.Join(jsonErr, markdownErr)
}
