package providercredential

import (
	"bytes"
	"context"
	"encoding/json"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestPiResponsePolicyExistingNativeParity(t *testing.T) {
	terminal := piPolicyJSON(t, piPolicyTerminal())
	badUsage := piPolicyResponse("completed")
	badUsage["usage"] = piPolicyUsage(9007199254740991, 1, 9007199254740991, 0, 0, 0)
	badTool := piPolicyResponse("completed")
	badTool["output"] = []any{map[string]any{"type": "function_call", "id": "tool"}}
	created := piPolicyResponse("in_progress")
	created["id"] = "other"
	var cases []struct {
		Request string `json:"request"`
		Body    string `json:"body"`
	}
	for _, body := range []string{
		piPolicyBody(t, piPolicyTerminal()),
		piPolicyBody(t, piPolicyLifecycle("response.incomplete", piPolicyResponse("incomplete"))),
		piPolicyBody(t, piPolicyLifecycle("response.queued", piPolicyResponse("queued")), piPolicyTerminal()),
		piPolicyBody(t, piPolicyAdded("message", "item_a", 0), piPolicyAdded("message", "item_a", 0), piPolicyTerminal()),
		piPolicyBody(t, piPolicyAdded("message", "item_a", 0), piPolicyAdded("message", "item_b", 0), piPolicyTerminal()),
		piPolicyBody(t, piPolicyAdded("message", "item_a", 0), piPolicyAdded("message", "item_a", 1), piPolicyTerminal()),
		piPolicyBody(t, piPolicyLifecycle("response.completed", badUsage)),
		piPolicyBody(t, piPolicyLifecycle("response.completed", badTool)),
		piPolicyBody(t, piPolicyLifecycle("response.created", created), piPolicyTerminal()),
		"data: " + terminal + "\n\r: trailing\n\n",
		"data: " + terminal + "\n\n: trailing\n\n",
		"data: " + terminal + "\n\rdata: " + terminal + "\n\n",
	} {
		cases = append(cases, struct {
			Request string `json:"request"`
			Body    string `json:"body"`
		}{piPolicyFixtureRequest, body})
	}
	stdin, err := json.Marshal(cases)
	if err != nil || len(stdin) > 65536 {
		t.Fatal("bounded parity fixture", err)
	}
	node, err := exec.LookPath("node")
	if err != nil {
		t.Fatal("documented Node runtime missing", err)
	}
	dir, err := filepath.Abs(".")
	if err != nil {
		t.Fatal(err)
	}
	script := filepath.Join(dir, "testdata/pi_response_policy_parity.mjs")
	native := filepath.Join(filepath.Dir(dir), "pisdktransport")
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, node, "--permission", "--allow-fs-read="+script, "--allow-fs-read="+filepath.Join(native, "policy.mjs"), "--allow-fs-read="+filepath.Join(native, "framing.mjs"), "--no-addons", "--disable-proto=throw", script)
	cmd.WaitDelay = time.Second
	cmd.Env = []string{"LANG=C", "LC_ALL=C"}
	cmd.Stdin = bytes.NewReader(stdin)
	stdout, stderr := piParityOutput{limit: 16384}, piParityOutput{limit: 4096}
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err = cmd.Run(); err != nil {
		t.Fatal("bounded native-policy parity", err, stderr.String())
	}
	var results [][]bool
	if err = json.Unmarshal(stdout.Bytes(), &results); err != nil || len(results) != len(cases) {
		t.Fatal("native fixture shape", err)
	}
	for i, c := range cases {
		q, x := piPolicyParsed(t, c.Request, c.Body)
		err := q.checkResponsePolicy(x)
		if len(results[i]) != 2 {
			t.Fatal("native chunk cohorts missing")
		}
		for _, native := range results[i] {
			if (err == nil) != native {
				t.Errorf("case%d parent=%v native=%v", i, err, native)
			}
		}
	}
}
