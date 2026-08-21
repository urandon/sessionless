// Package releaseversion parses and compares the release tag format owned by
// Sessionless.
package releaseversion

import (
	"fmt"
	"regexp"
	"strconv"
)

var tagPattern = regexp.MustCompile(`^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(?:-rc\.(0|[1-9][0-9]*))?$`)

// Version is a stable release or release-candidate version. Sessionless does
// not accept arbitrary SemVer prerelease identifiers or build metadata.
type Version struct {
	Major uint64
	Minor uint64
	Patch uint64
	RC    *uint64
}

// ParseTag parses vMAJOR.MINOR.PATCH and the explicitly supported
// vMAJOR.MINOR.PATCH-rc.NUMBER prerelease form.
func ParseTag(tag string) (Version, error) {
	matches := tagPattern.FindStringSubmatch(tag)
	if matches == nil {
		return Version{}, fmt.Errorf("invalid release tag %q", tag)
	}
	parts := make([]uint64, 3)
	for index := range parts {
		value, err := strconv.ParseUint(matches[index+1], 10, 64)
		if err != nil {
			return Version{}, fmt.Errorf("invalid release tag %q: %w", tag, err)
		}
		parts[index] = value
	}
	version := Version{Major: parts[0], Minor: parts[1], Patch: parts[2]}
	if matches[4] != "" {
		value, err := strconv.ParseUint(matches[4], 10, 64)
		if err != nil {
			return Version{}, fmt.Errorf("invalid release tag %q: %w", tag, err)
		}
		version.RC = &value
	}
	return version, nil
}

// Compare returns -1, 0, or 1 when v sorts before, equals, or sorts after
// other according to the supported SemVer subset.
func (v Version) Compare(other Version) int {
	for _, pair := range [][2]uint64{{v.Major, other.Major}, {v.Minor, other.Minor}, {v.Patch, other.Patch}} {
		if pair[0] < pair[1] {
			return -1
		}
		if pair[0] > pair[1] {
			return 1
		}
	}
	if v.RC == nil && other.RC == nil {
		return 0
	}
	if v.RC == nil {
		return 1
	}
	if other.RC == nil {
		return -1
	}
	if *v.RC < *other.RC {
		return -1
	}
	if *v.RC > *other.RC {
		return 1
	}
	return 0
}
