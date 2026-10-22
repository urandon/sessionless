package nativeharnessbootstrap

import (
	"slices"
	"strings"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/nativeharnessinput"
)

// ArgumentsV3 lowers a selected exact native command and original-produced
// descriptor into the explicit V3 helper ABI. The V2 argument builder supplies
// only the unchanged helper/relay syntax and bounds: this function neither
// computes nor accepts a V2 installation policy for the resulting command.
// Original ownership, the seven sealed commands and execution attestation must
// be checked by the V3 caller/launcher; a descriptor value grants no authority.
func ArgumentsV3(workload attachedworkerdaemon.LaunchSpec, helpers attachedworkerdaemon.NativeLaunchV2, descriptor nativeharnessinput.NativeExecutionDescriptorV3) ([]string, []string, error) {
	if validateImageArgumentsV3(workload.Arguments, descriptor) != nil {
		return nil, nil, ErrConfig
	}
	raw, err := nativeharnessinput.Marshal(descriptor)
	if err != nil {
		return nil, nil, ErrConfig
	}
	boot, relay, err := Arguments(workload, helpers)
	if err != nil {
		return nil, nil, err
	}
	boot[0] = "bootstrap-v3"
	boot = append(boot, "--execution-descriptor-json", string(raw))
	if !argsValid(boot) {
		return nil, nil, ErrConfig
	}
	return boot, relay, nil
}

func parseBootstrapV3(args []string) (bootstrapPlan, nativeharnessinput.NativeExecutionDescriptorV3, error) {
	var descriptor nativeharnessinput.NativeExecutionDescriptorV3
	if !argsValid(args) || len(args) < 3 || args[0] != "bootstrap-v3" || args[len(args)-2] != "--execution-descriptor-json" {
		return bootstrapPlan{}, descriptor, ErrConfig
	}
	var err error
	descriptor, err = nativeharnessinput.Parse([]byte(args[len(args)-1]))
	if err != nil {
		return bootstrapPlan{}, descriptor, ErrConfig
	}
	static := slices.Clone(args[:len(args)-2])
	static[0] = "bootstrap"
	plan, err := parseBootstrap(static)
	if err != nil || validateImageArgumentsV3(plan.native.args, descriptor) != nil {
		return bootstrapPlan{}, descriptor, ErrConfig
	}
	return plan, descriptor, nil
}

func validateImageArgumentsV3(args []string, descriptor nativeharnessinput.NativeExecutionDescriptorV3) error {
	slots, err := nativeharnessinput.Slots(descriptor)
	if err != nil {
		return ErrConfig
	}
	var expected []string
	for _, slot := range slots {
		if slot.Ordinal != 0 {
			expected = append(expected, slot.Path)
		}
	}
	var actual []string
	for i := 0; i < len(args); i++ {
		switch {
		case args[i] == "--image":
			if i+1 >= len(args) {
				return ErrConfig
			}
			i++
			actual = append(actual, args[i])
		case strings.HasPrefix(args[i], "-i") || strings.HasPrefix(args[i], "--image="):
			return ErrConfig
		}
	}
	if !slices.Equal(actual, expected) {
		return ErrConfig
	}
	return nil
}
