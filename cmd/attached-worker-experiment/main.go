// attached-worker-experiment prints an offline transport experiment draft.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"

	"gitcode.com/urandon/sessionless/internal/attachedworkerexperiment"
)

func run(args []string, output, diagnostic io.Writer) int {
	flags := flag.NewFlagSet("attached-worker-experiment", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	path := flags.String("manifest", "", "local content-free draft manifest")
	if err := flags.Parse(args); err != nil || *path == "" || flags.NArg() != 0 {
		fmt.Fprintln(diagnostic, "usage: attached-worker-experiment -manifest LOCAL_DRAFT_JSON")
		return 2
	}
	file, err := os.Open(*path)
	if err != nil {
		fmt.Fprintln(diagnostic, "cannot read transport experiment draft")
		return 1
	}
	defer file.Close()
	manifest, err := attachedworkerexperiment.ReadDraft(file)
	if err != nil {
		fmt.Fprintln(diagnostic, "invalid transport experiment draft")
		return 1
	}
	plan, err := attachedworkerexperiment.Prepare(manifest)
	if err != nil {
		fmt.Fprintln(diagnostic, "invalid transport experiment draft")
		return 1
	}
	encoder := json.NewEncoder(output)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(plan); err != nil {
		fmt.Fprintln(diagnostic, "cannot write transport experiment draft")
		return 1
	}
	return 0
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}
