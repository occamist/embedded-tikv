//go:build unix

package embeddedtikv

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"
)

// logTailBytes is how much of a server's output is quoted back in a start-up error.
const logTailBytes = 4096

// execFailureHint is appended to exec failures, where a missing dynamic loader makes execve
// report ENOENT for a binary that plainly exists. Stated unconditionally — probing for the
// loader guesses wrong on the very hosts that need the hint — and only here, since a server
// that started and then exited plainly managed to load.
const execFailureHint = "\n\nhint: the official builds are dynamically linked; set " +
	BinariesPathEnv + " to supply your own"

// process is one supervised server. Neither binary is given --log-file, so stdout and stderr
// carry everything and there is a single output path to capture, tee and quote back on failure.
type process struct {
	name    string
	cmd     *exec.Cmd
	logPath string
	logFile *os.File
	// watchdog is the write end of the parent-death pipe. Holding it open is what keeps the
	// server alive; the kernel closing it on our exit is what kills the server.
	watchdog *os.File
	done     chan struct{}
	waitErr  error
}

func startProcess(name, binary string, args []string, workDir, logPath string, logger io.Writer, cleanup watchdogCleanup) (*process, error) {
	logFile, err := os.Create(logPath)
	if err != nil {
		return nil, fmt.Errorf("embedded-tikv: unable to create log file %s: %w", logPath, err)
	}

	var output io.Writer = logFile
	if logger != nil {
		output = io.MultiWriter(logFile, logger)
	}

	cmd, watchdog, err := supervisedCommand(binary, args, cleanup)
	if err != nil {
		logFile.Close()

		return nil, err
	}

	cmd.Dir = workDir
	cmd.Stdout = output
	cmd.Stderr = output
	cmd.SysProcAttr = sysProcAttr()

	startErr := cmd.Start()

	// The child holds its own copy of the read end; ours must go, or the pipe would never
	// reach EOF and the watchdog would never fire.
	for _, extra := range cmd.ExtraFiles {
		extra.Close()
	}

	if startErr != nil {
		logFile.Close()

		if watchdog != nil {
			watchdog.Close()
		}

		return nil, fmt.Errorf("embedded-tikv: unable to start %s: %w%s", name, startErr, execFailureHint)
	}

	p := &process{
		name:     name,
		cmd:      cmd,
		logPath:  logPath,
		logFile:  logFile,
		watchdog: watchdog,
		done:     make(chan struct{}),
	}

	go func() {
		p.waitErr = cmd.Wait()
		close(p.done)
	}()

	return p, nil
}

// exited reports whether the server has already terminated.
func (p *process) exited() bool {
	select {
	case <-p.done:
		return true
	default:
		return false
	}
}

// exitReason describes how a server that has already exited terminated, for quoting back in a
// start-up error. Only meaningful once exited reports true.
//
// A server that exits cleanly during start-up has still failed, but cmd.Wait reports that as a
// nil error, which would reach the caller as "<nil>". The phrasing here is exec.ExitError's own,
// so that the clean case reads as the neighbouring value of the same field rather than as a
// separate kind of outcome: "exit status 0" beside "exit status 1".
func (p *process) exitReason() string {
	if p.waitErr == nil {
		return "exit status 0"
	}

	return p.waitErr.Error()
}

// stop asks the server to shut down cleanly, escalating to SIGKILL if it outstays timeout.
func (p *process) stop(timeout time.Duration) error {
	if p == nil {
		return nil
	}

	defer p.logFile.Close()

	// Stand the watchdog down first. It must never be left to infer intent from a signal: a
	// server that has already exited is stopped without one, and the watchdog would then read
	// the closing pipe as parent death and delete the whole cluster's data directory and
	// registry record while its siblings are still running.
	defer p.standDown()

	if p.exited() {
		return nil
	}

	if err := signalGroup(p.cmd.Process.Pid, syscall.SIGTERM); err != nil && !p.exited() {
		return fmt.Errorf("embedded-tikv: unable to signal %s: %w", p.name, err)
	}

	select {
	case <-p.done:
		return nil
	case <-time.After(timeout):
	}

	if err := signalGroup(p.cmd.Process.Pid, syscall.SIGKILL); err != nil && !p.exited() {
		return fmt.Errorf("embedded-tikv: unable to kill %s: %w", p.name, err)
	}

	select {
	case <-p.done:
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("embedded-tikv: %s did not exit after SIGKILL", p.name)
	}
}

// standDown tells the watchdog this shutdown is deliberate, then releases the pipe.
//
// The written line is what the watchdog's read consumes. A reader always sees buffered data
// before EOF, so writing and closing together is safe.
func (p *process) standDown() {
	if p == nil || p.watchdog == nil {
		return
	}

	_, _ = p.watchdog.Write([]byte("\n"))
	p.watchdog.Close()
	p.watchdog = nil
}

// logTail returns the end of the server's output, for quoting back in an error.
func (p *process) logTail() string {
	if p == nil {
		return ""
	}

	return logTail(p.logPath)
}

func logTail(path string) string {
	file, err := os.Open(path)
	if err != nil {
		return ""
	}

	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return ""
	}

	offset := int64(0)
	length := info.Size()

	if length > logTailBytes {
		offset = length - logTailBytes
		length = logTailBytes
	}

	buffer := make([]byte, length)
	if _, err := file.ReadAt(buffer, offset); err != nil && err != io.EOF {
		return ""
	}

	return strings.TrimSpace(string(buffer))
}
