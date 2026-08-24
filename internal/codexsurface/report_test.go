package codexsurface

import (
	"bytes"
	"testing"
	"time"
)

func TestReportIsDeterministicAndContainsCodesOnly(t *testing.T) {
	report := NewReport(SurfaceSDK, "0.147.0", "0.147.0", []time.Duration{12 * time.Millisecond}, []Check{
		{Name: "z_check", Pass: true}, {Name: "a_check", Pass: false},
	}, []string{"z_finding", "a_finding"})
	first, err := report.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	second, err := report.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("public report is not byte-stable")
	}
	if report.Status != "no_go" || report.Checks[0].Name != "a_check" || report.FindingCodes[0] != "a_finding" {
		t.Fatalf("report = %#v", report)
	}
	for _, forbidden := range [][]byte{[]byte("prompt"), []byte("output"), []byte("token"), []byte("account"), []byte("@"), []byte("https://")} {
		if bytes.Contains(first, forbidden) {
			t.Fatalf("public report contains forbidden material %q", forbidden)
		}
	}
}

func TestReportRejectsUnstructuredOrIdentityBearingCodes(t *testing.T) {
	report := NewReport(SurfaceExec, "0.148.0-alpha.15", "", []time.Duration{time.Millisecond}, []Check{{Name: "contains@email", Pass: true}}, nil)
	if _, err := report.Marshal(); err == nil {
		t.Fatal("identity-bearing check name was accepted")
	}
}

func TestReportRejectsStatusThatContradictsChecks(t *testing.T) {
	report := NewReport(SurfaceExec, "0.148.0-alpha.15", "", []time.Duration{time.Millisecond}, []Check{{Name: "contract", Pass: false}}, nil)
	report.Status = "pass"
	if _, err := report.Marshal(); err == nil {
		t.Fatal("passing status with failed check was accepted")
	}
}
