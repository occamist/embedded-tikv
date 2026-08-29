//go:build unix

package embeddedtikv

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestWatchdogKillsServerWhenPipeCloses drives the mechanism directly. Closing the write end is
// exactly what the kernel does when the parent process dies, so this reproduces parent death
// without having to die.
func TestWatchdogKillsServerWhenPipeCloses(t *testing.T) {
	if !haveShell() {
		t.Skip("no /bin/sh, watchdog unavailable")
	}

	cleanup := filepath.Join(t.TempDir(), "cluster-data")
	if err := os.MkdirAll(cleanup, 0o755); err != nil {
		t.Fatal(err)
	}

	logPath := filepath.Join(t.TempDir(), "server.log")

	// A stand-in for a server: something that would outlive us if nothing killed it.
	proc, err := startProcess("fake-0", "/bin/sleep", []string{"120"}, t.TempDir(), logPath, nil, watchdogCleanup{dataPath: cleanup})
	if err != nil {
		t.Fatal(err)
	}

	if proc.watchdog == nil {
		t.Fatal("no watchdog pipe was created")
	}

	if proc.exited() {
		t.Fatal("server exited before the watchdog was tested")
	}

	// Simulate the parent dying.
	proc.watchdog.Close()

	select {
	case <-proc.done:
	case <-time.After(10 * time.Second):
		t.Fatal("watchdog did not kill the server after the parent pipe closed")
	}

	waitFor(t, 10*time.Second, func() bool {
		_, err := os.Stat(cleanup)
		return os.IsNotExist(err)
	}, "watchdog did not remove the data directory")
}

// TestWatchdogLeavesCallerOwnedDataPathAlone: a directory the caller chose is theirs, and must
// survive even when the watchdog fires.
func TestWatchdogSparesCallerOwnedDataPath(t *testing.T) {
	if !haveShell() {
		t.Skip("no /bin/sh, watchdog unavailable")
	}

	preserved := filepath.Join(t.TempDir(), "my-data")
	if err := os.MkdirAll(preserved, 0o755); err != nil {
		t.Fatal(err)
	}

	// An empty cleanup path is what Cluster passes when DataPath was supplied by the caller.
	proc, err := startProcess("fake-0", "/bin/sleep", []string{"120"}, t.TempDir(),
		filepath.Join(t.TempDir(), "server.log"), nil, watchdogCleanup{})
	if err != nil {
		t.Fatal(err)
	}

	proc.watchdog.Close()

	select {
	case <-proc.done:
	case <-time.After(10 * time.Second):
		t.Fatal("watchdog did not kill the server")
	}

	if _, err := os.Stat(preserved); err != nil {
		t.Errorf("watchdog removed a caller-owned directory: %v", err)
	}
}

// TestWatchdogDoesNotFireDuringOrderlyStop guards the other direction: a normal Stop must not
// look like parent death.
func TestWatchdogDoesNotFireDuringOrderlyStop(t *testing.T) {
	if !haveShell() {
		t.Skip("no /bin/sh, watchdog unavailable")
	}

	cleanup := filepath.Join(t.TempDir(), "cluster-data")
	if err := os.MkdirAll(cleanup, 0o755); err != nil {
		t.Fatal(err)
	}

	marker := filepath.Join(cleanup, "marker")
	if err := os.WriteFile(marker, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	proc, err := startProcess("fake-0", "/bin/sleep", []string{"120"}, t.TempDir(),
		filepath.Join(t.TempDir(), "server.log"), nil, watchdogCleanup{dataPath: cleanup})
	if err != nil {
		t.Fatal(err)
	}

	if err := proc.stop(5 * time.Second); err != nil {
		t.Fatal(err)
	}

	// Stop removes the data directory itself, in its own good time; the watchdog must not have
	// raced ahead and done it as though the process had died.
	if _, err := os.Stat(marker); err != nil {
		t.Errorf("watchdog fired during an orderly stop: %v", err)
	}
}

func waitFor(t *testing.T, limit time.Duration, condition func() bool, message string) {
	t.Helper()

	deadline := time.Now().Add(limit)

	for time.Now().Before(deadline) {
		if condition() {
			return
		}

		time.Sleep(20 * time.Millisecond)
	}

	t.Fatal(message)
}

// TestWatchdogRemovesRegistryRecord: a killed parent should leave nothing at all, not even the
// two small files a later sweep would otherwise have to clear.
func TestWatchdogRemovesRegistryRecord(t *testing.T) {
	if !haveShell() {
		t.Skip("no /bin/sh, watchdog unavailable")
	}

	config := DefaultConfig().CachePath(t.TempDir())

	record, err := openRecord(config)
	if err != nil {
		t.Fatal(err)
	}

	dataPath := filepath.Join(t.TempDir(), "cluster-data")
	if err := os.MkdirAll(dataPath, 0o755); err != nil {
		t.Fatal(err)
	}

	cleanup := watchdogCleanup{
		dataPath:   dataPath,
		recordPath: record.path,
		lockPath:   lockPathFor(record.path),
	}

	proc, err := startProcess("fake-0", "/bin/sleep", []string{"120"}, t.TempDir(),
		filepath.Join(t.TempDir(), "server.log"), nil, cleanup)
	if err != nil {
		t.Fatal(err)
	}

	proc.watchdog.Close() // as the kernel would, on parent death

	select {
	case <-proc.done:
	case <-time.After(10 * time.Second):
		t.Fatal("watchdog did not kill the server")
	}

	for _, path := range []string{dataPath, cleanup.recordPath, cleanup.lockPath} {
		waitFor(t, 10*time.Second, func() bool {
			_, err := os.Stat(path)
			return os.IsNotExist(err)
		}, "watchdog left "+path+" behind")
	}
}

// TestWatchdogDoesNotFireWhenStoppingAnAlreadyExitedServer is a regression test for a data-loss
// bug: stopping a server that had already crashed sent no signal, so the watchdog read the
// closing pipe as parent death and deleted the shared data directory and registry record while
// the rest of the cluster was still running.
func TestWatchdogDoesNotFireWhenStoppingAnAlreadyExitedServer(t *testing.T) {
	if !haveShell() {
		t.Skip("no /bin/sh, watchdog unavailable")
	}

	config := DefaultConfig().CachePath(t.TempDir())

	record, err := openRecord(config)
	if err != nil {
		t.Fatal(err)
	}

	// Shared by every server in the cluster, so deleting it destroys the others' state too.
	shared := filepath.Join(t.TempDir(), "shared-cluster-data")
	if err := os.MkdirAll(shared, 0o755); err != nil {
		t.Fatal(err)
	}

	cleanup := watchdogCleanup{
		dataPath:   shared,
		recordPath: record.path,
		lockPath:   lockPathFor(record.path),
	}

	// A server that exits by itself, as a crashing TiKV would.
	proc, err := startProcess("crashed-0", "/bin/sh", []string{"-c", "exit 1"}, t.TempDir(),
		filepath.Join(t.TempDir(), "server.log"), nil, cleanup)
	if err != nil {
		t.Fatal(err)
	}

	<-proc.done

	if err := proc.stop(5 * time.Second); err != nil {
		t.Fatal(err)
	}

	// Give the watchdog every chance to misbehave.
	time.Sleep(300 * time.Millisecond)

	if _, err := os.Stat(shared); err != nil {
		t.Errorf("stopping an already-exited server destroyed the shared data directory: %v", err)
	}

	if _, err := os.Stat(record.path); err != nil {
		t.Errorf("stopping an already-exited server destroyed the registry record: %v", err)
	}
}
