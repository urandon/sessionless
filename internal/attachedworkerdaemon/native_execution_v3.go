package attachedworkerdaemon

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"unicode/utf8"

	"gitcode.com/urandon/sessionless/internal/nativeharnessinput"
)

// NativeInputFileV3 borrows a producer-held original file. This package never
// closes it or acquires source ownership from its pathname. The original
// prepared media/profile owner retains its complete cleanup lifetime.
type NativeInputFileV3 struct {
	Source string
	File   *os.File
}

// NativeInputMappingV3 is finite execution evidence, not a caller-selected
// destination or an authorization. Destinations derive only from the variant.
type NativeInputMappingV3 struct {
	Source, Destination string
	Size                uint64
	SHA256              string
	Device, Inode       uint64
}

// NativeExecutionV3 retains the original bounded input readset and its sealed
// command. It is not a Session child, claim, runner or admission decision: the
// connected Session/prepared/conduit owner must still supply that authority.
// Copies and source substitutions cannot replace this original readset.
type NativeExecutionV3 struct {
	self       *NativeExecutionV3
	bundle     NativeLaunchV3
	descriptor nativeharnessinput.NativeExecutionDescriptorV3
	arguments  []string
	files      []NativeInputFileV3
	identities []os.FileInfo
	mappings   []NativeInputMappingV3
	digest     ExecutableDigest
}

func NewNativeExecutionV3(ctx context.Context, bundle NativeLaunchV3, descriptor nativeharnessinput.NativeExecutionDescriptorV3, files []NativeInputFileV3) (*NativeExecutionV3, error) {
	if ctx == nil || ctx.Err() != nil || bundle.Validate() != nil {
		return nil, ErrSupervisorConfig
	}
	policy, err := bundle.InstallPolicyDigestV3()
	if err != nil || policy != bundle.PolicyDigest || descriptor.ScopeManifestDigest != hex.EncodeToString(bundle.ScopeDigest[:]) ||
		descriptor.InstallDigest != hex.EncodeToString(policy[:]) || descriptor.Catalog != bundle.Catalog || descriptor.Schema != bundle.Schema {
		return nil, ErrSupervisorConfig
	}
	raw, err := nativeharnessinput.Marshal(descriptor)
	slots, slotErr := nativeharnessinput.Slots(descriptor)
	if err != nil || slotErr != nil || len(files) != len(slots) {
		return nil, ErrSupervisorConfig
	}
	v := &NativeExecutionV3{bundle: bundle.Clone(), descriptor: descriptor, files: append([]NativeInputFileV3(nil), files...)}
	v.descriptor.Images = append([]nativeharnessinput.ImageV3{}, descriptor.Images...)
	v.arguments = append(append([]string(nil), bundle.BootstrapArguments[descriptor.Variant]...), "--execution-descriptor-json", string(raw))
	if len(v.arguments) > 128 {
		return nil, ErrSupervisorConfig
	}
	for i, file := range v.files {
		if ctx.Err() != nil || file.File == nil || !utf8.ValidString(file.Source) || !filepath.IsAbs(file.Source) || filepath.Clean(file.Source) != file.Source || file.Source == "/" || strings.ContainsAny(file.Source, ",\x00\r\n") {
			return nil, ErrSupervisorConfig
		}
		for _, artifact := range []NativeArtifactV2{bundle.Bootstrap, bundle.Relay, bundle.Companion, bundle.Workload} {
			if file.Source == artifact.Executable {
				return nil, ErrSupervisorConfig
			}
		}
		identity, err := file.File.Stat()
		device, inode, valid := nativeInputIdentityV3(identity)
		if err != nil || !valid || identity.Size() < 0 || uint64(identity.Size()) != slots[i].Size {
			return nil, ErrSupervisorConfig
		}
		for _, previous := range v.identities {
			if os.SameFile(previous, identity) {
				return nil, ErrSupervisorConfig
			}
		}
		v.identities = append(v.identities, identity)
		v.mappings = append(v.mappings, NativeInputMappingV3{Source: file.Source, Destination: slots[i].Path, Size: slots[i].Size, SHA256: slots[i].SHA256, Device: device, Inode: inode})
	}
	if v.verifySources(ctx) != nil {
		return nil, ErrSupervisorConfig
	}
	encoded, err := json.Marshal(struct {
		Descriptor nativeharnessinput.NativeExecutionDescriptorV3
		Arguments  []string
		Mappings   []NativeInputMappingV3
	}{v.descriptor, v.arguments, v.mappings})
	if err != nil {
		return nil, ErrSupervisorConfig
	}
	v.digest = ExecutableDigest(sha256.Sum256(append([]byte("sessionless.native-input.execution.v3\x00"), encoded...)))
	v.self = v
	return v, nil
}

func (v *NativeExecutionV3) ValidateV3(ctx context.Context, bundle NativeLaunchV3) error {
	if v == nil || v.self != v || ctx == nil || ctx.Err() != nil || !reflect.DeepEqual(v.bundle, bundle) || v.digest == (ExecutableDigest{}) {
		return ErrSupervisorConfig
	}
	return v.verifySources(ctx)
}

func (v *NativeExecutionV3) DescriptorV3() nativeharnessinput.NativeExecutionDescriptorV3 {
	if v == nil || v.self != v {
		return nativeharnessinput.NativeExecutionDescriptorV3{}
	}
	d := v.descriptor
	d.Images = append([]nativeharnessinput.ImageV3{}, d.Images...)
	return d
}

func (v *NativeExecutionV3) ArgumentsV3() []string {
	if v == nil || v.self != v {
		return nil
	}
	return append([]string(nil), v.arguments...)
}

func (v *NativeExecutionV3) MappingsV3() []NativeInputMappingV3 {
	if v == nil || v.self != v {
		return nil
	}
	return append([]NativeInputMappingV3(nil), v.mappings...)
}

func (v *NativeExecutionV3) DigestV3() ExecutableDigest {
	if v == nil || v.self != v {
		return ExecutableDigest{}
	}
	return v.digest
}

func (v *NativeExecutionV3) verifySources(ctx context.Context) error {
	for i, file := range v.files {
		if ctx.Err() != nil {
			return ErrSupervisorConfig
		}
		canonical, err := filepath.EvalSymlinks(file.Source)
		if err != nil || canonical != file.Source {
			return ErrSupervisorConfig
		}
		path, err := os.Lstat(file.Source)
		held, heldErr := file.File.Stat()
		if err != nil || heldErr != nil || !os.SameFile(path, v.identities[i]) || !os.SameFile(held, v.identities[i]) {
			return ErrSupervisorConfig
		}
		for _, actual := range []os.FileInfo{path, held} {
			device, inode, valid := nativeInputIdentityV3(actual)
			if !valid || device != v.mappings[i].Device || inode != v.mappings[i].Inode || actual.Size() < 0 || uint64(actual.Size()) != v.mappings[i].Size {
				return ErrSupervisorConfig
			}
		}
		hash := sha256.New()
		reader := io.NewSectionReader(file.File, 0, int64(v.mappings[i].Size)+1)
		var buffer [8192]byte
		var used uint64
		for {
			if ctx.Err() != nil {
				return ErrSupervisorConfig
			}
			n, readErr := reader.Read(buffer[:])
			used += uint64(n)
			if used > v.mappings[i].Size {
				return ErrSupervisorConfig
			}
			_, _ = hash.Write(buffer[:n])
			if readErr == io.EOF {
				break
			}
			if readErr != nil || n == 0 {
				return ErrSupervisorConfig
			}
		}
		if used != v.mappings[i].Size || hex.EncodeToString(hash.Sum(nil)) != v.mappings[i].SHA256 {
			return ErrSupervisorConfig
		}
		path, err = os.Lstat(file.Source)
		held, heldErr = file.File.Stat()
		if err != nil || heldErr != nil || !os.SameFile(path, v.identities[i]) || !os.SameFile(held, v.identities[i]) {
			return ErrSupervisorConfig
		}
		for _, actual := range []os.FileInfo{path, held} {
			_, _, valid := nativeInputIdentityV3(actual)
			if !valid || actual.Size() < 0 || uint64(actual.Size()) != v.mappings[i].Size {
				return ErrSupervisorConfig
			}
		}
	}
	if ctx.Err() != nil {
		return ErrSupervisorConfig
	}
	return nil
}
