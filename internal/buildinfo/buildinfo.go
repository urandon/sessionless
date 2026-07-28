// Package buildinfo exposes immutable build metadata to binaries and health endpoints.
package buildinfo

import "runtime"

// Values are populated with -ldflags in release builds.
var (
	Version = "dev"
	Commit  = "unknown"
	BuiltAt = "unknown"
)

// Info is the public build metadata contract shared by all components.
type Info struct {
	Component string `json:"component"`
	Version   string `json:"version"`
	Commit    string `json:"commit"`
	BuiltAt   string `json:"built_at"`
	GoVersion string `json:"go_version"`
}

// Current returns build metadata for component.
func Current(component string) Info {
	return Info{
		Component: component,
		Version:   Version,
		Commit:    Commit,
		BuiltAt:   BuiltAt,
		GoVersion: runtime.Version(),
	}
}
