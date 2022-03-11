//go:build !linux
// +build !linux

package cgutil

const (
	// DefaultCgroupParent does not apply to non-Linux operating systems.
	DefaultCgroupParent = ""
)

// UseV2 is always false on non-Linux systems.
//
// This is a read-only value.
var UseV2 = false

// NewCpusetManager creates a no-op CpusetManager for non-Linux operating systems.
func NewCpusetManager(string, hclog.Logger) CpusetManager {
	return new(NoopCpusetManager)
}

// FindCgroupMountpointDir returns nothing for non-Linux operating systems.
func FindCgroupMountpointDir() (string, error) {
	return "", nil
}
