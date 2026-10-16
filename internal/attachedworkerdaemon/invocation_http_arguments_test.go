package attachedworkerdaemon

import (
	"context"
	"strconv"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// Original runner with synthetic parent/lifecycle. This proves the explicit
// closed HTTP arm and exact manifest admission BEFORE Issue, not native fit.
func TestAPIOwnerSeparateHTTPArgumentsBeforeIssue(t *testing.T) {
	type scenario struct {
		name string
		edit func(*PreparedAPIDataV1)
		want bool
	}
	cases := []scenario{{name: "exact", want: true}}
	set := func(d *PreparedAPIDataV1, i int, value string) {
		d.HTTPInitArguments[i] = value
		d.ParentProcess.Arguments[len(d.ParentProcess.Arguments)-17+i] = value
		d.ChildProcess.Arguments[len(d.ChildProcess.Arguments)-17+i] = value
	}
	for i := range 11 {
		cases = append(cases, scenario{name: "wire field " + strconv.Itoa(i), edit: func(d *PreparedAPIDataV1) { set(d, i, "1") }})
	}
	for _, value := range []string{"", "0", "01", "-1", "+1", "1.0", "1e1", " 1", "١", "9007199254740992", "18446744073709551615"} {
		cases = append(cases, scenario{name: "decimal " + strconv.Quote(value), edit: func(d *PreparedAPIDataV1) { set(d, 12, value) }})
	}
	cases = append(cases,
		scenario{name: "both arms", edit: func(d *PreparedAPIDataV1) { d.InitArguments[0] = "1" }},
		scenario{name: "no HTTP declaration", edit: func(d *PreparedAPIDataV1) { d.HTTPInitArguments = [17]string{} }},
		scenario{name: "extra argv", edit: func(d *PreparedAPIDataV1) {
			d.ParentProcess.Arguments = append(d.ParentProcess.Arguments, "1")
			d.ChildProcess.Arguments = append(d.ChildProcess.Arguments, "1")
		}},
		scenario{name: "script", edit: func(d *PreparedAPIDataV1) {
			d.ParentProcess.Arguments[0] = "foreign"
			d.ChildProcess.Arguments[0] = "foreign"
		}},
		scenario{name: "child argv", edit: func(d *PreparedAPIDataV1) { d.ChildProcess.Arguments[len(d.ChildProcess.Arguments)-1] = "1" }},
		scenario{name: "init maximum", edit: func(d *PreparedAPIDataV1) {
			set(d, 11, strconv.FormatUint(d.ParentRequest.Profile.Limits.ProcessFrameBytes-112, 10))
		}},
		scenario{name: "copy maximum", edit: func(d *PreparedAPIDataV1) {
			set(d, 12, strconv.FormatUint(d.ParentRequest.Profile.Limits.PreSendAllocationBytes+1, 10))
		}},
		scenario{name: "node maximum", edit: func(d *PreparedAPIDataV1) {
			set(d, 13, strconv.FormatUint(d.ParentRequest.Profile.Limits.JSONNodes+1, 10))
		}},
		scenario{name: "depth", edit: func(d *PreparedAPIDataV1) { set(d, 14, "1") }},
		scenario{name: "profile maximum", edit: func(d *PreparedAPIDataV1) { set(d, 15, strconv.Itoa(domain.AttachedPiSDKCanonicalMaxBytesV1+1)) }},
		scenario{name: "request length", edit: func(d *PreparedAPIDataV1) { set(d, 16, "1") }},
	)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			inv, parent, life := apiOwnerFixture(t)
			parent.mutate = func(d *PreparedAPIDataV1) {
				l := d.ParentRequest.Profile.Limits
				values := [17]uint64{l.ProcessFrameBytes, l.ProcessFrameBytes - 113, min(l.NativeRequestBytes, l.ProcessFrameBytes-85-l.RequestHeaderBytes), min(l.RawFinalBytes, l.StructuredFinalBytes, l.ProcessFrameBytes-17), l.RequestHeaderBytes, l.ResponseHeaderBytes, l.ResponseBodyBytes, l.ResponseChunkBytes, maxSupervisorStdinBytes, l.ProcessStdoutBytes, l.ProcessEvents, 700, 600, 500, l.JSONDepth, 300, uint64(len(d.ParentRequest.Body))}
				prefix := len(d.ParentProcess.Arguments) - 10
				d.InitArguments = [10]string{}
				d.ParentProcess.Arguments = d.ParentProcess.Arguments[:prefix:prefix]
				d.ChildProcess.Arguments = d.ChildProcess.Arguments[:prefix:prefix]
				for i, v := range values {
					d.HTTPInitArguments[i] = strconv.FormatUint(v, 10)
				}
				d.ParentProcess.Arguments = append(d.ParentProcess.Arguments, d.HTTPInitArguments[:]...)
				d.ChildProcess.Arguments = append(d.ChildProcess.Arguments, d.HTTPInitArguments[:]...)
				if tc.edit != nil {
					tc.edit(d)
				}
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
			result, err := runner.Run(ctx, inv)
			if tc.want {
				if !result.Succeeded(err) || calls != 1 || life.finalizeCount != 1 || life.releaseCount != 1 {
					t.Fatal("exact HTTP original lifecycle denied", err, result.FailureCode, parent.order)
				}
			} else {
				if err == nil || calls != 0 || life.handle.HandleID != "" || life.releaseCount != 0 {
					t.Fatal("invalid HTTP manifest crossed Issue", err, calls, parent.order)
				}
				for _, step := range parent.order {
					if step == "pre_issue" || step == "issue" {
						t.Fatal("invalid HTTP manifest reached Source/Issue", parent.order)
					}
				}
			}
		})
	}
}
