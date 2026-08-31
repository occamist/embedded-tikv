//go:build unix

package embeddedtikv

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

// TestProcessCommandLine covers the merged implementation: /proc where it exists, ps elsewhere.
// The reaper's safety depends entirely on this reporting the truth.
func TestProcessCommandLine(t *testing.T) {
	cmdline, ok := processCommandLine(os.Getpid())
	if !ok {
		t.Fatal("could not read our own command line")
	}

	// The test binary's own path must appear in its argv.
	if !strings.Contains(cmdline, os.Args[0]) {
		t.Errorf("command line %q does not contain %q", cmdline, os.Args[0])
	}
}

func TestProcessCommandLineReportsMissingProcess(t *testing.T) {
	// A process that has exited must not be reported as running, or the reaper could kill
	// whatever later inherits its PID.
	cmd := exec.Command("/bin/sh", "-c", "exit 0")
	if err := cmd.Run(); err != nil {
		t.Fatal(err)
	}

	if _, ok := processCommandLine(cmd.Process.Pid); ok {
		t.Errorf("reported a command line for exited pid %d", cmd.Process.Pid)
	}
}
