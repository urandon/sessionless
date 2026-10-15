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

type piParityOutput struct {
	bytes.Buffer
	limit int
}

func (b *piParityOutput) Write(p []byte) (int, error) {
	if len(p) > b.limit-b.Len() {
		n, _ := b.Buffer.Write(p[:b.limit-b.Len()])
		return n, ErrInvalid
	}
	return b.Buffer.Write(p)
}

func TestPiResponseScanExistingNativeFramerParity(t *testing.T) {
	var cases []string
	for _, sep := range []string{"\n", "\r", "\r\n"} {
		for _, body := range []string{"data: {}" + sep + sep, ": c" + sep + sep, sep + sep + sep + sep, "data: {}" + sep + sep + sep, "data: {}" + sep + sep + "data: {}" + sep + sep} {
			cases = append(cases, body)
		}
	}
	cases = append(cases, "data: {}\n\rdata: {}\n\n")
	stdin, err := json.Marshal(cases)
	if err != nil {
		t.Fatal(err)
	}
	node, err := exec.LookPath("node")
	if err != nil {
		t.Fatal("documented Node runtime missing", err)
	}
	dir, err := filepath.Abs(".")
	if err != nil {
		t.Fatal(err)
	}
	script := filepath.Join(dir, "testdata/pi_response_scan_framing.mjs")
	native := filepath.Join(filepath.Dir(dir), "pisdktransport/framing.mjs")
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, node, "--permission", "--allow-fs-read="+script, "--allow-fs-read="+native, "--no-addons", "--disable-proto=throw", script)
	cmd.WaitDelay = time.Second
	cmd.Env = []string{"LANG=C", "LC_ALL=C"} // no NODE_OPTIONS, HOME or credential inheritance
	cmd.Stdin = bytes.NewReader(stdin)
	stdout, stderr := piParityOutput{limit: 16384}, piParityOutput{limit: 4096}
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err = cmd.Run()
	if err != nil {
		t.Fatalf("bounded native framer parity: %v stderr=%s", err, stderr.String())
	}
	var results [][]struct {
		OK     bool   `json:"ok"`
		Frames uint64 `json:"frames"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &results); err != nil || len(results) != len(cases) {
		t.Fatal("native fixture shape", err)
	}
	for i, body := range cases {
		q := piScanner(t, piScanLimits())
		err := q.scan(t.Context(), piScanView(t, body), []byte("fixture-secret"))
		if len(results[i]) != 3 {
			t.Fatal("native chunk cohort missing")
		}
		for _, native := range results[i] {
			if (err == nil) != native.OK || q.frames != native.Frames {
				t.Errorf("case%d input=%q parentErr=%v frames=%d native=%+v", i, body, err, q.frames, native)
			}
		}
	}
}
