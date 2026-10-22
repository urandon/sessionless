package nativeharnessbootstrap

import (
	"slices"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
)

// InstallationArgumentsV3 seals the seven complete static bootstrap commands.
// It reuses only the V2 helper syntax builder, not its installation digest or
// runtime acceptance. The per-execution descriptor pair is appended later by
// the original NativeExecutionV3 readset, never by a generic argv template.
func InstallationArgumentsV3(bundle attachedworkerdaemon.NativeLaunchV3) ([7][]string, []string, error) {
	var commands [7][]string
	if bundle.Version != 3 {
		return commands, nil, ErrConfig
	}
	helpers := attachedworkerdaemon.NativeLaunchV2{Version: 2, Bootstrap: bundle.Bootstrap, Relay: bundle.Relay, Companion: bundle.Companion,
		SocketDestination: bundle.SocketDestination, ProviderHosts: slices.Clone(bundle.ProviderHosts), Limits: bundle.Limits}
	var relay []string
	for i, args := range bundle.NativeArguments {
		workload := attachedworkerdaemon.LaunchSpec{Executable: bundle.Workload.Executable, ExecutableDigest: bundle.Workload.Digest, Arguments: slices.Clone(args)}
		boot, generatedRelay, err := Arguments(workload, helpers)
		if err != nil || len(boot) > 126 {
			return [7][]string{}, nil, ErrConfig
		}
		boot[0] = "bootstrap-v3"
		commands[i] = boot
		if i == 0 {
			relay = generatedRelay
		} else if !slices.Equal(relay, generatedRelay) {
			return [7][]string{}, nil, ErrConfig
		}
	}
	return commands, relay, nil
}

func ValidateInstallationV3(bundle attachedworkerdaemon.NativeLaunchV3) error {
	if bundle.Validate() != nil {
		return ErrConfig
	}
	commands, relay, err := InstallationArgumentsV3(bundle)
	if err != nil || !slices.Equal(bundle.RelayArguments, relay) {
		return ErrConfig
	}
	for i, command := range commands {
		if !slices.Equal(command, bundle.BootstrapArguments[i]) {
			return ErrConfig
		}
	}
	return nil
}
