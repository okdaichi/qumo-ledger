// Package version carries build information stamped in at link time.
package version

import "runtime/debug"

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

// Info describes the running build.
type Info struct {
	Version string
	Commit  string
	Date    string
	Go      string
}

// Get returns the current build information, falling back to the module's own
// VCS stamps when the binary was built without ldflags — which is what `go run`
// and `go install` produce.
func Get() Info {
	info := Info{Version: version, Commit: commit, Date: date, Go: "unknown"}

	build, ok := debug.ReadBuildInfo()
	if !ok {
		return info
	}
	info.Go = build.GoVersion

	for _, setting := range build.Settings {
		switch setting.Key {
		case "vcs.revision":
			if info.Commit == "none" {
				info.Commit = setting.Value
			}
		case "vcs.time":
			if info.Date == "unknown" {
				info.Date = setting.Value
			}
		}
	}

	return info
}
