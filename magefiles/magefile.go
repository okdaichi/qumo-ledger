//go:build mage

// Magefiles live in their own module so that build tooling never becomes a
// dependency of the library itself.
package main

import (
	"fmt"
	"os"
	"os/exec"

	"github.com/magefile/mage/mg"
	"github.com/magefile/mage/sh"
)

// Default is the target run when none is given.
var Default = Help

// Help lists the available targets.
func Help() {
	fmt.Print(`qumo-ledger targets

  build     compile every package (the repo ships no binaries)
  test      run the test suite with the race detector
  cover     run tests and write coverage.out
  lint      run go vet and, if installed, golangci-lint
  tidy      tidy every module in the repo
  check     tidy, lint and test
  clean     remove build artifacts
`)
}

// Build compiles every package as a smoke check. qumo-ledger is a library: it
// ships no binaries, so there is nothing to link into bin/. Runnable examples
// live under examples/ and are run with `go run ./examples/<name>`.
func Build() error {
	return sh.RunV("go", "build", "./...")
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
	return os.RemoveAll("coverage.out")
}
