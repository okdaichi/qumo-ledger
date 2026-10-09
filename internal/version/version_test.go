package version

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGet(t *testing.T) {
	info := Get()

	assert.Equal(t, "dev", info.Version, "an unstamped build reports the placeholder version")
	assert.Equal(t, runtime.Version(), info.Go)
	assert.NotEmpty(t, info.Commit)
	assert.NotEmpty(t, info.Date)
}

// Values stamped in at link time are the release's own account of itself, so
// the VCS stamps of whatever checkout happened to build it must not replace
// them.
func TestGet_StampedValuesWin(t *testing.T) {
	prevVersion, prevCommit, prevDate := version, commit, date
	t.Cleanup(func() { version, commit, date = prevVersion, prevCommit, prevDate })
	version, commit, date = "v1.2.3", "0123abc", "2026-01-02T03:04:05Z"

	info := Get()

	assert.Equal(t, "v1.2.3", info.Version)
	assert.Equal(t, "0123abc", info.Commit)
	assert.Equal(t, "2026-01-02T03:04:05Z", info.Date)
}
