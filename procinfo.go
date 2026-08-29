//go:build unix

package embeddedtikv

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
)

var (
	procfsOnce sync.Once
	haveProcfs bool
)

func haveProcfsCached() bool {
	procfsOnce.Do(func() {
		_, err := os.Stat("/proc/self/cmdline")
		haveProcfs = err == nil
	})

	return haveProcfs
}

// processCommandLine returns the full argv of a running process, space separated.
//
// Linux exposes this through /proc, which costs one read and no subprocess. macOS and the BSDs
// have no /proc and fall back to ps, which is POSIX and reports the same thing. The fallback
// also covers a Linux container started without /proc mounted, so this is not merely a
// portability shim.
func processCommandLine(pid int) (string, bool) {
	if haveProcfsCached() {
		raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", pid))
		if err != nil {
			return "", false
		}

		return strings.ReplaceAll(strings.TrimRight(string(raw), "\x00"), "\x00", " "), true
	}

	output, err := exec.Command("ps", "-p", strconv.Itoa(pid), "-o", "args=").Output()
	if err != nil {
		return "", false
	}

	line := strings.TrimSpace(string(output))

	return line, line != ""
}

// processEntry pairs a process with its command line.
type processEntry struct {
	pid     int
	cmdline string
}

// enumerateProcesses lists the processes this user can see, with their command lines.
//
// The sweep needs this to catch servers that are running but not named in any record — a
// cluster killed between spawning a server and recording its PID would otherwise leave a
// process nothing knows about.
func enumerateProcesses() []processEntry {
	if haveProcfsCached() {
		return enumerateViaProcfs()
	}

	return enumerateViaPS()
}

func enumerateViaProcfs() []processEntry {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil
	}

	found := make([]processEntry, 0, len(entries))

	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}

		cmdline, ok := processCommandLine(pid)
		if !ok {
			continue
		}

		found = append(found, processEntry{pid: pid, cmdline: cmdline})
	}

	return found
}

func enumerateViaPS() []processEntry {
	output, err := exec.Command("ps", "-A", "-o", "pid=,args=").Output()
	if err != nil {
		return nil
	}

	var found []processEntry

	for _, line := range strings.Split(string(output), "\n") {
		line = strings.TrimSpace(line)

		pidText, cmdline, split := strings.Cut(line, " ")
		if !split {
			continue
		}

		pid, err := strconv.Atoi(pidText)
		if err != nil {
			continue
		}

		found = append(found, processEntry{pid: pid, cmdline: strings.TrimSpace(cmdline)})
	}

	return found
}
