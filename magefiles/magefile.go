//go:build mage

// Magefiles live in their own module so that build tooling never becomes a
// dependency of the library itself.
package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/magefile/mage/mg"
	"github.com/magefile/mage/sh"
)

const versionPkg = "github.com/okdaichi/qumo-ledger/internal/version"

// Default is the target run when none is given.
var Default = Help

// Help lists the available targets.
func Help() {
	fmt.Print(`qumo-ledger targets

  build     build bin/qumo-ledger
  test      run the test suite with the race detector
  cover     run tests and write coverage.out
  lint      run go vet and, if installed, golangci-lint
  tidy      tidy every module in the repo
  check     tidy, lint and test
  clean     remove build artifacts
`)
}

// Build compiles the CLI into bin/.
func Build() error {
	name := "qumo-ledger"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}

	return sh.RunV("go", "build", "-ldflags", versionLDFlags(), "-o", filepath.Join("bin", name), "./cmd/qumo-ledger")
}

// Test runs the suite under the race detector.
func Test() error {
	return sh.RunV("go", "test", "-race", "./...")
}

// Cover runs the suite and writes coverage.out.
func Cover() error {
	if err := sh.RunV("go", "test", "-coverprofile=coverage.out", "-covermode=atomic", "./..."); err != nil {
		return err
	}

	return sh.RunV("go", "tool", "cover", "-func=coverage.out")
}

// Lint runs go vet, then golangci-lint when it is on PATH.
func Lint() error {
	if err := sh.RunV("go", "vet", "./..."); err != nil {
		return err
	}

	if _, err := exec.LookPath("golangci-lint"); err != nil {
		fmt.Println("golangci-lint not found on PATH; skipping")
		return nil
	}

	return sh.RunV("golangci-lint", "run")
}

// Tidy tidies both the root module and the magefiles module.
func Tidy() error {
	if err := sh.RunV("go", "mod", "tidy"); err != nil {
		return err
	}

	return sh.RunWithV(map[string]string{}, "go", "mod", "tidy", "-C", "magefiles")
}

// Check runs the full pre-commit sequence.
func Check() {
	mg.SerialDeps(Tidy, Lint, Test)
}

// Clean removes build artifacts.
func Clean() error {
	for _, path := range []string{"bin", "coverage.out"} {
		if err := os.RemoveAll(path); err != nil {
			return err
		}
	}

	return nil
}

func versionLDFlags() string {
	return strings.Join([]string{
		"-s", "-w",
		"-X", versionPkg + ".version=" + gitDescribe(),
		"-X", versionPkg + ".commit=" + gitCommit(),
		"-X", versionPkg + ".date=" + time.Now().UTC().Format(time.RFC3339),
	}, " ")
}

func gitDescribe() string {
	return gitOutput("dev", "describe", "--tags", "--always", "--dirty")
}

func gitCommit() string {
	return gitOutput("none", "rev-parse", "--short", "HEAD")
}

func gitOutput(fallback string, args ...string) string {
	out, err := exec.Command("git", args...).Output()
	if err != nil {
		return fallback
	}

	return strings.TrimSpace(string(out))
}
