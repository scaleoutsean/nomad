//go:build linux

package cgutil

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/hashicorp/go-hclog"
	"github.com/opencontainers/runc/libcontainer/cgroups"
	lcc "github.com/opencontainers/runc/libcontainer/configs"
)

// UseV2 indicates whether only cgroups.v2 is enabled. If cgroups.v2 is not
// enabled or is running in hybrid mode with cgroups.v1, Nomad will make use of
// cgroups.v1
//
// This is a read-only value.
var UseV2 = cgroups.IsCgroup2UnifiedMode()

// GetCgroupParent returns the mount point under the root cgroup in which Nomad
// will create cgroups. If parent is not set, an appropriate name for the version
// of cgroups will be used.
func GetCgroupParent(parent string) string {
	if UseV2 {
		return v2GetParent(parent)
	}
	return getParentV1(parent)
}

// CreateCPUSetManager creates a V1 or V2 CpusetManager depending on system configuration.
func CreateCPUSetManager(parent string, logger hclog.Logger) CpusetManager {
	if UseV2 {
		return NewCpusetManagerV2(v2GetParent(parent), logger.Named("cpuset.v2"))
	}
	return NewCpusetManagerV1(getParentV1(parent), logger.Named("cpuset.v1"))
}

func GetCPUsFromCgroup(group string) ([]uint16, error) {
	if UseV2 {
		return v2GetCPUsFromCgroup(v2GetParent(group))
	}
	return getCPUsFromCgroupV1(getParentV1(group))
}

func CgroupID(allocID, task string) string {
	if allocID == "" || task == "" {
		panic("empty alloc or task")
	}

	if UseV2 {
		return fmt.Sprintf("%s.%s.scope", allocID, task)
	}
	return fmt.Sprintf("%s.%s", task, allocID)
}

// ConfigureBasicCgroups will initialize cgroups for v1.
//
// Not used in cgroups.v2
func ConfigureBasicCgroups(cgroup string, config *lcc.Config) error {
	if UseV2 {
		return nil
	}

	// In V1 we must setup the freezer cgroup ourselves
	subsystem := "freezer"
	path, err := getCgroupPathHelperV1(subsystem, filepath.Join(DefaultCgroupV1Parent, cgroup))
	if err != nil {
		return fmt.Errorf("failed to find %s cgroup mountpoint: %v", subsystem, err)
	}
	if err = os.MkdirAll(path, 0755); err != nil {
		return err
	}
	config.Cgroups.Paths = map[string]string{
		subsystem: path,
	}
	return nil
}

// FindCgroupMountpointDir is used to find the cgroup mount point on a Linux
// system.
func FindCgroupMountpointDir() (string, error) {
	mount, err := cgroups.GetCgroupMounts(false)
	if err != nil {
		return "", err
	}
	// It's okay if the mount point is not discovered
	if len(mount) == 0 {
		return "", nil
	}
	return mount[0].Mountpoint, nil
}

// CopyCpuset copies the cpuset.cpus value from source into destination.
func CopyCpuset(source, destination string) error {
	correct, err := cgroups.ReadFile(source, "cpuset.cpus")
	if err != nil {
		return err
	}

	err = cgroups.WriteFile(destination, "cpuset.cpus", correct)
	if err != nil {
		return err
	}

	return nil
}
