package testutil

import (
	"os/exec"
	"runtime"
	"syscall"
	"testing"

	"github.com/opencontainers/runc/libcontainer/cgroups"
)

// RequireRoot skips tests unless:
// - running as root
func RequireRoot(t *testing.T) {
	if syscall.Geteuid() != 0 {
		t.Skip("Test requires root")
	}
}

// RequireConsul skips tests unless:
// - "consul" executable is detected on $PATH
func RequireConsul(t *testing.T) {
	_, err := exec.Command("consul", "version").CombinedOutput()
	if err != nil {
		t.Skipf("Test requires Consul: %v", err)
	}
}

// RequireVault skips tests unless:
// - "vault" executable is detected on $PATH
func RequireVault(t *testing.T) {
	_, err := exec.Command("vault", "version").CombinedOutput()
	if err != nil {
		t.Skipf("Test requires Vault: %v", err)
	}
}

// ExecCompatible skips tests unless:
// - running as root
// - running on Linux
// - support for cgroups is detected
func ExecCompatible(t *testing.T) {
	if runtime.GOOS != "linux" || syscall.Geteuid() != 0 {
		t.Skip("Test requires root on Linux")
	}

	if !CgroupsCompatible(t) {
		t.Skip("Test requires cgroup support")
	}
}

// JavaCompatible skips tests unless:
// - "java" executable is detected on $PATH
// - running as root
// - running on Linux
// - support for cgroups is detected
func JavaCompatible(t *testing.T) {
	_, err := exec.Command("java", "-version").CombinedOutput()
	if err != nil {
		t.Skipf("Test requires Java: %v", err)
	}

	if runtime.GOOS == "linux" || syscall.Geteuid() != 0 {
		t.Skip("Test requires root on Linux")
	}

	if !CgroupsCompatible(t) {
		t.Skip("Test requires cgroup support")
	}
}

// QemuCompatible skips tests unless:
// - "qemu-system-x86_64" executable is detected on $PATH (!windows)
// - "qemu-img" executable is detected on on $PATH (windows)
func QemuCompatible(t *testing.T) {
	// Check if qemu exists
	bin := "qemu-system-x86_64"
	if runtime.GOOS == "windows" {
		bin = "qemu-img"
	}
	_, err := exec.Command(bin, "--version").CombinedOutput()
	if err != nil {
		t.Skipf("Test requires QEMU (%s)", bin)
	}
}

// CgroupsCompatible returns true if either cgroups.v1 or cgroups.v2 is supported.
func CgroupsCompatible(t *testing.T) bool {
	return cgroupsCompatibleV1(t) || cgroupsCompatibleV2(t)
}

// CgroupsCompatibleV1 skips tests unless:
// - cgroup.v1 mount point is detected
func CgroupsCompatibleV1(t *testing.T) {
	if !cgroupsCompatibleV1(t) {
		t.Skipf("Test requires cgroup.v1 support")
	}
}

func cgroupsCompatibleV1(t *testing.T) bool {
	if cgroupsCompatibleV2(t) {
		t.Log("No cgroup.v1 mount point: running in cgroup.v2 mode")
		return false
	}
	mount, err := cgroups.GetCgroupMounts(false)
	if err != nil {
		t.Logf("Unable to detect cgroup.v1 mount point: %v", err)
		return false
	}
	if len(mount) == 0 {
		t.Logf("No cgroup.v1 mount point: empty path")
		return false
	}
	return true
}

// CgroupsCompatibleV2 skips tests unless:
// - cgroup.v2 unified mode is detected
func CgroupsCompatibleV2(t *testing.T) {
	if !cgroupsCompatibleV2(t) {
		t.Skip("Test requires cgroup.v2 support")
	}
}

func cgroupsCompatibleV2(t *testing.T) bool {
	if cgroups.IsCgroup2UnifiedMode() {
		return true
	}
	t.Logf("No cgroup.v2 unified mode support")
	return false
}

// MountCompatible skips tests unless:
// - not running as windows
// - running as root
func MountCompatible(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Test requires not using Windows")
	}

	if syscall.Geteuid() != 0 {
		t.Skip("Test requires root")
	}
}
