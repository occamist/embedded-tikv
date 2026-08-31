//go:build unix

package embeddedtikv

import (
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"sync"
)

const shellPath = "/bin/sh"

// watchdogScript supervises one server against the death of this process.
//
// The parent holds the write end of a pipe whose read end arrives here as fd 3, and the read
// blocks until one of two things happens.
//
// If the parent exits, the kernel closes the write end and the read reports EOF. That happens
// however the parent dies, SIGKILL included, which is what makes this work on platforms that
// have no parent-death signal.
//
// If instead the parent is shutting this server down deliberately, it writes a line first. The
// read then succeeds and the watchdog stands down without touching anything. Distinguishing the
// two explicitly matters: stopping a server that has *already* exited sends no signal, so the
// watchdog cannot infer "the parent is alive" from having survived one, and would otherwise
// delete the whole cluster's data directory out from under its still-running siblings.
//
// tikv-server and pd-server are third-party binaries and will never watch fd 3 themselves, so a
// shell holds the watch and then execs the server. Because it execs, the process we started
// keeps its PID and process group: the PID recorded in the registry is the server's, signals and
// exit status behave exactly as a direct exec, and the watcher is simply another member of the
// same process group.
//
// $$ is the invoking shell's PID and does not change inside the background subshell, so after
// the exec it names the server.
const watchdogScript = `d=$1; r=$2; l=$3; shift 3
{ if read _ <&3; then exit 0; fi
  kill -9 "$$" 2>/dev/null
  [ -n "$d" ] && rm -rf -- "$d"
  [ -n "$r" ] && rm -f -- "$r" "$l"
  kill -9 0 2>/dev/null
} &
exec "$@"`

var (
	shellOnce      sync.Once
	shellAvailable bool
	warnOnce       sync.Once
)

// haveShell reports whether the watchdog shell exists. Minimal container images sometimes ship
// without one, in which case servers are started directly and the parent-death guarantee falls
// back to the registry sweep, which is then the only mechanism.
func haveShell() bool {
	shellOnce.Do(func() {
		info, err := os.Stat(shellPath)
		shellAvailable = err == nil && !info.IsDir() && info.Mode()&0o111 != 0
	})

	return shellAvailable
}

// warnMissingWatchdog reports, once per process, that this host cannot supervise servers
// against the death of the test binary. Not an error — the cluster runs fine — but silence
// would make the leak look like a bug in this library rather than a property of the host.
func warnMissingWatchdog(logger io.Writer) {
	if haveShell() {
		return
	}

	warnOnce.Do(func() {
		const message = "embedded-tikv: no " + shellPath + ", so a killed run leaks servers " +
			"until the next Start or CleanOrphans"

		if logger != nil {
			fmt.Fprintln(logger, message)

			return
		}

		log.Println(message)
	})
}

// watchdogCleanup names the paths a watchdog removes once it has killed its server, so that a
// parent dying outright leaves nothing at all behind.
//
// dataPath must be empty when the caller supplied the directory: a caller's directory is theirs
// to keep. The record and lock are always this library's own.
type watchdogCleanup struct {
	dataPath   string
	recordPath string
	lockPath   string
}

// supervisedCommand builds the command to run a server under the parent-death watchdog. It
// returns the write end of the pipe, which the caller must hold open for as long as the server
// should live, and close once it has exited.
//
// The watchdog kills its own server first, then removes the data directory, and only then the
// registry record. That order matters: while the record survives, a later sweep can still find
// anything the watchdog failed to clean up.
//
// Every orderly shutdown path must call standDown before closing the pipe.
func supervisedCommand(binary string, args []string, cleanup watchdogCleanup) (*exec.Cmd, *os.File, error) {
	if !haveShell() {
		return exec.Command(binary, args...), nil, nil
	}

	reader, writer, err := os.Pipe()
	if err != nil {
		return nil, nil, fmt.Errorf("embedded-tikv: unable to create watchdog pipe: %w", err)
	}

	// $0 is a label, $1..$3 are the paths to clean up, and $4 onwards is the command to exec.
	argv := append([]string{
		"-c", watchdogScript, "embedded-tikv-watchdog",
		cleanup.dataPath, cleanup.recordPath, cleanup.lockPath,
		binary,
	}, args...)

	cmd := exec.Command(shellPath, argv...)
	cmd.ExtraFiles = []*os.File{reader}

	return cmd, writer, nil
}
