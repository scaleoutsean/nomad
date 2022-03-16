package cgutil

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/hashicorp/nomad/ci"
	"github.com/hashicorp/nomad/client/testutil"
	"github.com/hashicorp/nomad/helper/testlog"
	"github.com/hashicorp/nomad/helper/uuid"
	"github.com/opencontainers/runc/libcontainer/cgroups"
	"github.com/opencontainers/runc/libcontainer/cgroups/fs2"
	"github.com/stretchr/testify/require"
)

func TestUtil_SplitPath(t *testing.T) {
	ci.Parallel(t)

	try := func(input, expParent, expCgroup string) {
		parent, cgroup := SplitPath(input)
		require.Equal(t, expParent, parent)
		require.Equal(t, expCgroup, cgroup)
	}

	// foo, /bar
	try("foo/bar", "foo", "/bar")
	try("/foo/bar/", "foo", "/bar")
	try("/sys/fs/cgroup/foo/bar", "foo", "/bar")

	// foo, /bar/baz
	try("/foo/bar/baz/", "foo", "/bar/baz")
	try("foo/bar/baz", "foo", "/bar/baz")
	try("/sys/fs/cgroup/foo/bar/baz", "foo", "/bar/baz")
}

func TestUtil_GetCgroupParent(t *testing.T) {
	ci.Parallel(t)

	t.Run("v1", func(t *testing.T) {
		testutil.CgroupV1Compatible(t)
		t.Run("default", func(t *testing.T) {
			exp := "/nomad"
			parent := GetCgroupParent("")
			require.Equal(t, exp, parent)
		})

		t.Run("configured", func(t *testing.T) {
			exp := "/bar"
			parent := GetCgroupParent("/bar")
			require.Equal(t, exp, parent)
		})
	})

	t.Run("v2", func(t *testing.T) {
		testutil.CgroupV2Compatible(t)
		t.Run("default", func(t *testing.T) {
			exp := "nomad.slice"
			parent := GetCgroupParent("")
			require.Equal(t, exp, parent)
		})

		t.Run("configured", func(t *testing.T) {
			exp := "abc.slice"
			parent := GetCgroupParent("abc.slice")
			require.Equal(t, exp, parent)
		})
	})
}

func TestUtil_CreateCPUSetManager(t *testing.T) {
	ci.Parallel(t)

	logger := testlog.HCLogger(t)

	t.Run("v1", func(t *testing.T) {
		testutil.CgroupV1Compatible(t)
		parent := "/" + uuid.Short()
		manager := CreateCPUSetManager(parent, logger)
		err := manager.Init([]uint16{0})
		require.NoError(t, err)
		require.NoError(t, cgroups.RemovePath(filepath.Join(CgroupRoot, parent)))
	})

	t.Run("v2", func(t *testing.T) {
		testutil.CgroupV2Compatible(t)
		parent := uuid.Short() + ".slice"
		manager := CreateCPUSetManager(parent, logger)
		err := manager.Init([]uint16{0})
		require.NoError(t, err)
		require.NoError(t, cgroups.RemovePath(filepath.Join(CgroupRoot, parent)))
	})
}

func TestUtil_GetCPUsFromCgroup(t *testing.T) {
	ci.Parallel(t)

	t.Run("v2", func(t *testing.T) {
		testutil.CgroupV2Compatible(t)
		cpus, err := GetCPUsFromCgroup("system.slice") // thanks, systemd!
		require.NoError(t, err)
		require.NotEmpty(t, cpus)
	})
}

func create(t *testing.T, name string) {
	mgr, err := fs2.NewManager(nil, filepath.Join(CgroupRoot, name), v2isRootless)
	require.NoError(t, err)
	err = mgr.Apply(v2CreationPID)
	require.NoError(t, err)
}

func cleanup(t *testing.T, name string) {
	err := cgroups.RemovePath(filepath.Join(CgroupRoot, name))
	require.NoError(t, err)
}

func TestUtil_CopyCpuset(t *testing.T) {
	ci.Parallel(t)

	t.Run("v2", func(t *testing.T) {
		source := uuid.Short() + ".scope"
		create(t, source)
		defer cleanup(t, source)
		require.NoError(t, cgroups.WriteFile(filepath.Join(CgroupRoot, source), "cpuset.cpus", "0-1"))

		destination := uuid.Short() + ".scope"
		create(t, destination)
		defer cleanup(t, destination)

		err := CopyCpuset(
			filepath.Join(CgroupRoot, source),
			filepath.Join(CgroupRoot, destination),
		)
		require.NoError(t, err)

		value, readErr := cgroups.ReadFile(filepath.Join(CgroupRoot, destination), "cpuset.cpus")
		require.NoError(t, readErr)
		require.Equal(t, "0-1", strings.TrimSpace(value))
	})
}
