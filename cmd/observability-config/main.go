// observability-config is offline authoring tooling, never a deploy command.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"gitcode.com/urandon/sessionless/internal/observabilityconfig"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "observability-config:", err)
		os.Exit(1)
	}
}

// run reports only redacted status; private artifacts are exclusively created in
// an owner-only directory. It neither discovers credentials nor contacts a cloud.
func run(args []string, stdout io.Writer) error {
	if len(args) == 0 {
		return errors.New("choose validate, render, compare or schema")
	}
	f := flag.NewFlagSet("observability-config", flag.ContinueOnError)
	f.SetOutput(io.Discard)
	root := f.String("root", "observability", "public YAML root")
	profile := f.String("profile", "cloud-dev", "profile name")
	out := f.String("out", "", "new private output directory (or compare file)")
	bindings := f.String("bindings", "", "explicit private binding YAML")
	ownership := f.String("ownership", "", "explicit private complete-list preflight YAML")
	nowValue := f.String("now", "", "explicit RFC3339 validation instant for bound export")
	expected := f.String("expected", "", "expected normalized.json")
	actual := f.String("actual", "", "operator-normalized actual readback JSON")
	if err := f.Parse(args[1:]); err != nil || f.NArg() != 0 {
		return errors.New("invalid flags or positional arguments")
	}
	switch args[0] {
	case "validate":
		if *out != "" || *bindings != "" || *ownership != "" || *nowValue != "" || *expected != "" || *actual != "" {
			return errors.New("validate accepts only root and profile")
		}
		c, err := observabilityconfig.Load(*root, *profile)
		if err != nil {
			return err
		}
		fmt.Fprintf(stdout, "VALID: schema=%d generator=%s input_sha256=%s; offline only\n", observabilityconfig.SchemaVersion, observabilityconfig.GeneratorVersion, c.InputDigest)
		return nil
	case "schema":
		if *out == "" || *bindings != "" || *ownership != "" || *nowValue != "" || *expected != "" || *actual != "" {
			return errors.New("schema requires only an output directory")
		}
		files := map[string]any{}
		for name, schema := range observabilityconfig.PublicSchemas() {
			files[name+".schema.json"] = schema
		}
		if err := writeBundle(*out, files); err != nil {
			return err
		}
		fmt.Fprintln(stdout, "SCHEMAS: closed structural schemas exported; semantic validator remains required")
		return nil
	case "render":
		if *out == "" || *expected != "" || *actual != "" {
			return errors.New("render requires output directory, not compare inputs")
		}
		c, err := observabilityconfig.Load(*root, *profile)
		if err != nil {
			return err
		}
		var b *observabilityconfig.BindingsDocument
		var o *observabilityconfig.OwnershipDocument
		var now time.Time
		if *bindings != "" || *ownership != "" || *nowValue != "" {
			if *bindings == "" || *ownership == "" || *nowValue == "" {
				return errors.New("bound render requires all three explicit private inputs")
			}
			b, err = observabilityconfig.ReadBindings(*bindings)
			if err != nil {
				return err
			}
			o, err = observabilityconfig.ReadOwnership(*ownership)
			if err != nil {
				return err
			}
			now, err = time.Parse(time.RFC3339, *nowValue)
			if err != nil {
				return errors.New("invalid explicit validation timestamp")
			}
		}
		bundle, err := observabilityconfig.Render(c, b, o, now)
		if err != nil {
			return err
		}
		files := map[string]any{"normalized.json": bundle.Envelope, "preview.json": struct {
			Metadata observabilityconfig.Metadata       `json:"metadata"`
			Panels   []observabilityconfig.PanelPreview `json:"panels"`
		}{bundle.Envelope.Metadata, bundle.Envelope.Panels}, "alerts-console.json": struct {
			Metadata observabilityconfig.Metadata      `json:"metadata"`
			Rules    []observabilityconfig.ConsoleRule `json:"rules"`
		}{bundle.Envelope.Metadata, bundle.Envelope.Console}}
		if bundle.Terraform != nil {
			files["dashboard.tf.json"] = bundle.Terraform
		}
		if err := writeBundle(*out, files); err != nil {
			return err
		}
		fmt.Fprintf(stdout, "EXPORTED: %s; no cloud access or apply authority\n", bundle.Envelope.Metadata.Mode)
		return nil
	case "compare":
		if *expected == "" || *actual == "" || *out == "" || *bindings != "" || *ownership != "" || *nowValue != "" {
			return errors.New("compare requires expected, actual and private output file")
		}
		e, err := readJSON(*expected)
		if err != nil {
			return err
		}
		a, err := readJSON(*actual)
		if err != nil {
			return err
		}
		drift, err := observabilityconfig.Compare(e, a)
		if err != nil {
			return err
		}
		data, err := observabilityconfig.Encode(drift)
		if err != nil {
			return err
		}
		if err := writeExclusive(*out, data); err != nil {
			return err
		}
		if !drift.Equal {
			return errors.New("DRIFT: reviewed reconciliation required; no changes applied")
		}
		fmt.Fprintln(stdout, "MATCH: normalized inputs equal; this is not native receipt or apply authority")
		return nil
	default:
		return errors.New("unknown operation")
	}
}
func readJSON(path string) ([]byte, error) {
	i, err := os.Lstat(path)
	if err != nil || !i.Mode().IsRegular() || i.Size() > 4*observabilityconfig.MaxDocumentBytes {
		return nil, errors.New("readback must be a bounded regular file")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, errors.New("cannot open readback")
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, 4*observabilityconfig.MaxDocumentBytes+1))
	if err != nil {
		return nil, errors.New("cannot read readback")
	}
	return b, nil
}
func writeBundle(dir string, files map[string]any) error {
	// Requiring a new directory prevents symlink traversal and partial replacement
	// of an existing reviewed output. A failed export preserves partial evidence.
	if err := os.Mkdir(dir, 0700); err != nil {
		return errors.New("output directory must be new, with an existing trusted parent")
	}
	for _, name := range []string{"queries.schema.json", "dashboards.schema.json", "alerts.schema.json", "profile.schema.json", "normalized.json", "preview.json", "alerts-console.json", "dashboard.tf.json"} {
		value, ok := files[name]
		if !ok {
			continue
		}
		data, err := observabilityconfig.Encode(value)
		if err != nil {
			return err
		}
		if err = writeExclusive(filepath.Join(dir, name), data); err != nil {
			return err
		}
	}
	return nil
}
func writeExclusive(path string, data []byte) error {
	parent, err := os.Lstat(filepath.Dir(path))
	if err != nil || !parent.IsDir() || parent.Mode().Perm()&0077 != 0 {
		return errors.New("output parent must be an owner-only directory")
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return errors.New("output must be a new file in a trusted private directory")
	}
	_, writeErr := f.Write(data)
	closeErr := f.Close()
	if writeErr != nil || closeErr != nil {
		return errors.New("cannot finish private output")
	}
	return nil
}
