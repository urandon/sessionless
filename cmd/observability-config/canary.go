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

// Canary flags are deliberately parsed separately from production operations.
func runCanary(args []string, stdout io.Writer) error {
	f := flag.NewFlagSet("observability-canary", flag.ContinueOnError)
	f.SetOutput(io.Discard)
	input := f.String("input", "observability/canary.yaml", "explicit synthetic policy YAML")
	out := f.String("out", "", "new private output directory")
	bindings := f.String("bindings", "", "owner-only planned canary binding YAML")
	nowValue := f.String("now", "", "explicit RFC3339 validation instant")
	if err := f.Parse(args[1:]); err != nil || f.NArg() != 0 {
		return errors.New("invalid canary flags or positional arguments")
	}
	used := map[string]bool{}
	f.Visit(func(v *flag.Flag) { used[v.Name] = true })
	switch args[0] {
	case "canary-schema":
		if !used["out"] || *out == "" || used["input"] || used["bindings"] || used["now"] {
			return errors.New("canary-schema accepts only output directory")
		}
		files := map[string]any{}
		for name, schema := range observabilityconfig.CanarySchemas() {
			files[name+".schema.json"] = schema
		}
		if err := writeCanaryBundle(*out, files); err != nil {
			return err
		}
		fmt.Fprintln(stdout, "CANARY SCHEMAS: closed structural schemas; semantic validation remains required; offline only")
		return nil
	case "canary-validate", "canary-render":
		if *input == "" {
			return errors.New("canary policy input required")
		}
		if args[0] == "canary-validate" && (used["out"] || used["bindings"] || used["now"]) {
			return errors.New("canary-validate accepts only input")
		}
		if args[0] == "canary-render" && (*out == "" || used["bindings"] != used["now"] || (used["bindings"] && (*bindings == "" || *nowValue == ""))) {
			return errors.New("canary-render requires new output and both or neither binding/time inputs")
		}
		c, err := observabilityconfig.LoadCanary(*input)
		if err != nil {
			return err
		}
		var b *observabilityconfig.CanaryBindingsDocument
		var now time.Time
		if used["bindings"] {
			b, err = observabilityconfig.ReadCanaryBindings(*bindings)
			if err != nil {
				return err
			}
			now, err = time.Parse(time.RFC3339, *nowValue)
			if err != nil {
				return errors.New("invalid explicit canary timestamp")
			}
		}
		bundle, err := observabilityconfig.RenderCanary(c, b, now)
		if err != nil {
			return err
		}
		if args[0] == "canary-validate" {
			fmt.Fprintf(stdout, "VALID CANARY: schema=1 generator=%s input_sha256=%s; PLAN_ONLY\n", observabilityconfig.CanaryGeneratorVersion, bundle.Envelope.Metadata.InputSHA256)
			return nil
		}
		if err := writeCanaryBundle(*out, map[string]any{"canary-normalized.json": bundle.Envelope, "canary-console.json": bundle.Console, "canary-plan.json": bundle.Plan}); err != nil {
			return err
		}
		fmt.Fprintln(stdout, "EXPORTED CANARY: PLAN_ONLY; no cloud access, sample writer or activation authority")
		return nil
	default:
		return errors.New("unknown canary operation")
	}
}

func writeCanaryBundle(dir string, files map[string]any) error {
	if err := os.Mkdir(dir, 0700); err != nil {
		return errors.New("canary output directory must be new with a trusted existing parent")
	}
	for _, name := range []string{"canary.schema.json", "canary-bindings.schema.json", "canary-normalized.json", "canary-console.json", "canary-plan.json"} {
		value, ok := files[name]
		if !ok {
			continue
		}
		data, err := observabilityconfig.Encode(value)
		if err != nil {
			return err
		}
		if err := writeExclusive(filepath.Join(dir, name), data); err != nil {
			return err
		}
	}
	return nil
}
