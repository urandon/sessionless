package attachedworkerdaemon

import (
	"context"
	"strconv"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// Genuine original runner with synthetic parent/lifecycle only. These tests
// prove the full process comparison and pre-Issue ordering, not native/SDK,
// Source, model/tariff, filesystem or serving qualification.
func TestAPIOwnerPinnedPrefixAndExactInitArguments(t *testing.T) {
	type scenario struct {
		name string
		edit func(*Invocation, *apiTestParent)
		want bool
	}
	cases := []scenario{
		{name: "complete", want: true},
		{name: "longer pinned prefix", want: true, edit: func(i *Invocation, _ *apiTestParent) {
			i.Process.Arguments = []string{"--permission", "--no-addons", "/pinned/source/entry.mjs"}
		}},
		{name: "original stdin", edit: func(i *Invocation, _ *apiTestParent) { i.Process.Stdin = []byte("untrusted Init") }},
		{name: "missing original prefix", edit: func(i *Invocation, _ *apiTestParent) { i.Process.Arguments = nil }},
	}
	editData := func(name string, edit func(*PreparedAPIDataV1)) {
		cases = append(cases, scenario{name: name, edit: func(_ *Invocation, p *apiTestParent) { p.mutate = edit }})
	}
	editData("missing declared values", func(d *PreparedAPIDataV1) { d.InitArguments = [10]string{} })
	editData("missing suffix", func(d *PreparedAPIDataV1) {
		d.ParentProcess.Arguments = d.ParentProcess.Arguments[:len(d.ParentProcess.Arguments)-10]
		d.ChildProcess.Arguments = d.ChildProcess.Arguments[:len(d.ChildProcess.Arguments)-10]
	})
	editData("short suffix", func(d *PreparedAPIDataV1) {
		d.ParentProcess.Arguments = d.ParentProcess.Arguments[:len(d.ParentProcess.Arguments)-1]
		d.ChildProcess.Arguments = d.ChildProcess.Arguments[:len(d.ChildProcess.Arguments)-1]
	})
	editData("extra suffix", func(d *PreparedAPIDataV1) {
		d.ParentProcess.Arguments = append(d.ParentProcess.Arguments, "1")
		d.ChildProcess.Arguments = append(d.ChildProcess.Arguments, "1")
	})
	editData("changed pinned script", func(d *PreparedAPIDataV1) {
		d.ParentProcess.Arguments[0], d.ChildProcess.Arguments[0] = "/other/entry.mjs", "/other/entry.mjs"
	})
	editData("changed executable", func(d *PreparedAPIDataV1) {
		d.ParentProcess.Executable, d.ChildProcess.Executable = "/other/executable", "/other/executable"
	})
	editData("changed digest", func(d *PreparedAPIDataV1) {
		d.ParentProcess.ExecutableDigest[0] ^= 1
		d.ChildProcess.ExecutableDigest = d.ParentProcess.ExecutableDigest
	})
	editData("changed environment", func(d *PreparedAPIDataV1) {
		d.ParentProcess.Environment = append(d.ParentProcess.Environment, EnvironmentVariable{Name: "UNPINNED", Value: "1"})
		d.ChildProcess.Environment = append(d.ChildProcess.Environment, EnvironmentVariable{Name: "UNPINNED", Value: "1"})
	})
	editData("changed read roots", func(d *PreparedAPIDataV1) {
		d.ParentProcess.AdditionalReadRoots = []string{"/untrusted"}
		d.ChildProcess.AdditionalReadRoots = []string{"/untrusted", ""}
	})
	editData("parent stdin", func(d *PreparedAPIDataV1) {
		d.ParentProcess.Stdin, d.ChildProcess.Stdin = []byte("untrusted"), []byte("untrusted")
	})
	editData("child stdin", func(d *PreparedAPIDataV1) { d.ChildProcess.Stdin = []byte("untrusted") })
	editData("parent suffix not declared", func(d *PreparedAPIDataV1) {
		d.ParentProcess.Arguments[len(d.ParentProcess.Arguments)-1] = "1"
	})
	editData("child suffix not declared", func(d *PreparedAPIDataV1) {
		d.ChildProcess.Arguments[len(d.ChildProcess.Arguments)-1] = "1"
	})
	set := func(d *PreparedAPIDataV1, index int, value string) {
		d.InitArguments[index] = value
		d.ParentProcess.Arguments[len(d.ParentProcess.Arguments)-10+index] = value
		d.ChildProcess.Arguments[len(d.ChildProcess.Arguments)-10+index] = value
	}
	for _, value := range []string{"", "0", "01", "-1", "+1", " 1", "1 ", "1.0", "1e1", "١", "9007199254740992", "18446744073709551615", "--eval=1"} {
		editData("noncanonical value "+strconv.Quote(value), func(d *PreparedAPIDataV1) { set(d, 5, value) })
	}
	for _, index := range []int{0, 1, 2, 3, 7, 9} {
		editData("changed fixed value "+strconv.Itoa(index), func(d *PreparedAPIDataV1) { set(d, index, "1") })
	}
	editData("oversized Init", func(d *PreparedAPIDataV1) {
		set(d, 4, strconv.FormatUint(d.ParentRequest.Profile.Limits.ProcessFrameBytes, 10))
	})
	editData("oversized decoder copy", func(d *PreparedAPIDataV1) { set(d, 5, "9007199254740991") })
	editData("oversized decoder nodes", func(d *PreparedAPIDataV1) { set(d, 6, "9007199254740991") })
	editData("first over decoder copy", func(d *PreparedAPIDataV1) {
		set(d, 5, strconv.FormatUint(d.ParentRequest.Profile.Limits.PreSendAllocationBytes+1, 10))
	})
	editData("first over decoder nodes", func(d *PreparedAPIDataV1) {
		set(d, 6, strconv.FormatUint(d.ParentRequest.Profile.Limits.JSONNodes+1, 10))
	})
	editData("oversized profile", func(d *PreparedAPIDataV1) { set(d, 8, strconv.Itoa(domain.AttachedPiSDKCanonicalMaxBytesV1+1)) })
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			inv, parent, life := apiOwnerFixture(t)
			if tc.edit != nil {
				tc.edit(&inv, parent)
			}
			ctx, cancel := context.WithTimeout(t.Context(), time.Hour)
			t.Cleanup(cancel)
			calls := 0
			runner, err := NewPreparedAPIInvocationRunner(preparedExecutorFunc(func(_ context.Context, in PreparedInvocationV1) (AttemptResult, error) {
				calls++
				if err := authorizeSyntheticAPISend(in); err != nil {
					return AttemptResult{}, err
				}
				return AttemptResult{DescendantsReaped: true, BoundaryReleased: true, CleanupSucceeded: true}, nil
			}), life, parent)
			if err != nil {
				t.Fatal(err)
			}
			result, runErr := runner.Run(ctx, inv)
			if tc.want {
				if !result.Succeeded(runErr) || calls != 1 || life.finalizeCount != 1 || life.releaseCount != 1 {
					t.Fatalf("pinned prefix + Init denied: err=%v failure=%s execute=%d finalize=%d release=%d order=%v", runErr, result.FailureCode, calls, life.finalizeCount, life.releaseCount, parent.order)
				}
				return
			}
			if runErr == nil || calls != 0 || life.handle.HandleID != "" || result.CredentialReleaseRequired || life.releaseCount != 0 {
				t.Fatalf("unbound process crossed Issue: err=%v failure=%s execute=%d release=%d order=%v", runErr, result.FailureCode, calls, life.releaseCount, parent.order)
			}
			for _, step := range parent.order {
				if step == "pre_issue" || step == "issue" {
					t.Fatalf("unbound process reached %s before denial; order=%v", step, parent.order)
				}
			}
		})
	}
}
