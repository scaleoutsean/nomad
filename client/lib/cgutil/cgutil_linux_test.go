package cgutil

import (
	"testing"

	"github.com/hashicorp/nomad/ci"
	"github.com/stretchr/testify/require"
)

func Test_SplitPath(t *testing.T) {
	ci.Parallel(t)

	try := func(input, expParent, expCgroup string) {
		parent, cgroup := SplitPath(input)
		require.Equal(t, expParent, parent)
		require.Equal(t, expCgroup, cgroup)
	}

	try("/foo/bar/baz/", "foo", "/bar/baz")
	try("foo/bar/baz", "foo", "/bar/baz")
	try("/sys/fs/cgroup/foo/bar/baz", "foo", "/bar/baz")
}
