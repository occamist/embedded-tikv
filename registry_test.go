//go:build unix

package embeddedtikv

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// pidExists reports whether a PID is still present in the process table.
//
// EPERM means the process exists but is not ours to signal, which still counts as present, and
// a zombie counts too — the PID stays taken until its parent reaps it. So this answers "has it
// gone away", which is what these tests actually ask. Non-positive PIDs address process groups
// rather than processes and are never valid here.
func pidExists(pid int) bool {
	if pid <= 0 {
		return false
	}

	err := syscall.Kill(pid, 0)

	return err == nil || errors.Is(err, syscall.EPERM)
}

// blockedProcess starts a process whose command line contains binary and dataPath, and which
// blocks until killed. It stands in for an abandoned server without needing a real TiKV.
func blockedProcess(t *testing.T, binary, dataPath string) *exec.Cmd {
	t.Helper()

	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}

	// "read x" blocks in the shell itself, so the PID we record is the one that must die.
	// The trailing arguments mimic a real server's: the data directory always appears as a
	// path prefix, never as a bare argument, which is what makes a boundary-aware match safe.
	cmd := exec.Command("/bin/sh", "-c", "read x", binary,
		"--data-dir="+filepath.Join(dataPath, "tikv-0", "data"))
	cmd.Stdin = reader

	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		writer.Close()
		reader.Close()
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	})

	return cmd
}

func TestCleanOrphansReclaimsAbandonedCluster(t *testing.T) {
	config := DefaultConfig().CachePath(t.TempDir())

	dataPath := filepath.Join(t.TempDir(), "cluster-data")
	if err := os.MkdirAll(dataPath, 0o755); err != nil {
		t.Fatal(err)
	}

	binary := filepath.Join(t.TempDir(), "tikv-server")
	server := blockedProcess(t, binary, dataPath)

	record, err := openRecord(config)
	if err != nil {
		t.Fatal(err)
	}

	if err := record.setDataPath(dataPath, true); err != nil {
		t.Fatal(err)
	}

	if err := record.addServer("tikv-0", server.Process.Pid, binary); err != nil {
		t.Fatal(err)
	}

	// Simulate the owning process dying: the kernel would drop its lock exactly like this.
	record.lock.release()

	reclaimed, err := CleanOrphans(config)
	if err != nil {
		t.Fatal(err)
	}

	if reclaimed != 1 {
		t.Fatalf("CleanOrphans reclaimed %d clusters, want 1", reclaimed)
	}

	waitForExit(t, server)

	if _, err := os.Stat(dataPath); !os.IsNotExist(err) {
		t.Errorf("abandoned data directory %s was not removed", dataPath)
	}

	if _, err := os.Stat(record.path); !os.IsNotExist(err) {
		t.Errorf("record %s was not removed", record.path)
	}
}

func TestCleanOrphansLeavesLiveClusterAlone(t *testing.T) {
	config := DefaultConfig().CachePath(t.TempDir())

	dataPath := filepath.Join(t.TempDir(), "cluster-data")
	if err := os.MkdirAll(dataPath, 0o755); err != nil {
		t.Fatal(err)
	}

	binary := filepath.Join(t.TempDir(), "tikv-server")
	server := blockedProcess(t, binary, dataPath)

	// Lock stays held, exactly as it would while the owning test binary is running.
	record, err := openRecord(config)
	if err != nil {
		t.Fatal(err)
	}

	if err := record.setDataPath(dataPath, true); err != nil {
		t.Fatal(err)
	}

	defer record.close()

	if err := record.addServer("tikv-0", server.Process.Pid, binary); err != nil {
		t.Fatal(err)
	}

	reclaimed, err := CleanOrphans(config)
	if err != nil {
		t.Fatal(err)
	}

	if reclaimed != 0 {
		t.Fatalf("CleanOrphans reclaimed %d live clusters, want 0", reclaimed)
	}

	if !pidExists(server.Process.Pid) {
		t.Error("CleanOrphans killed a server belonging to a live cluster")
	}

	if _, err := os.Stat(dataPath); err != nil {
		t.Errorf("CleanOrphans removed a live cluster's data directory: %v", err)
	}
}

// TestCleanOrphansSparesUnrelatedProcess is the safety property that matters most: a recycled
// PID, or somebody's real TiKV started from the same binary, must never be killed.
func TestCleanOrphansSparesUnrelatedProcess(t *testing.T) {
	config := DefaultConfig().CachePath(t.TempDir())

	dataPath := filepath.Join(t.TempDir(), "cluster-data")
	if err := os.MkdirAll(dataPath, 0o755); err != nil {
		t.Fatal(err)
	}

	binary := filepath.Join(t.TempDir(), "tikv-server")

	// Same binary, a different data directory: this is somebody else's cluster.
	unrelated := blockedProcess(t, binary, filepath.Join(t.TempDir(), "someone-elses-data"))

	record, err := openRecord(config)
	if err != nil {
		t.Fatal(err)
	}

	if err := record.setDataPath(dataPath, true); err != nil {
		t.Fatal(err)
	}

	if err := record.addServer("tikv-0", unrelated.Process.Pid, binary); err != nil {
		t.Fatal(err)
	}

	record.lock.release()

	if _, err := CleanOrphans(config); err != nil {
		t.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond)

	if !pidExists(unrelated.Process.Pid) {
		t.Fatal("CleanOrphans killed a process that was not part of this cluster")
	}
}

func waitForExit(t *testing.T, cmd *exec.Cmd) {
	t.Helper()

	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("abandoned server was not killed")
	}
}

// TestRecordIsNeverVisibleWithoutItsLock pins the ordering invariant the reaper depends on:
// a record must not appear in the registry until its lock is already held. If it can, a reaper
// scanning at that instant takes the lock, reads an unparseable record, and deletes a cluster
// that is still starting up.
//
// This watches the registry from a second goroutine and flags any record file seen without a
// corresponding lock file.
func TestRecordIsNeverVisibleWithoutItsLock(t *testing.T) {
	config := DefaultConfig().CachePath(t.TempDir())

	dir, err := registryDir(config)
	if err != nil {
		t.Fatal(err)
	}

	stop := make(chan struct{})
	violations := make(chan string, 1)
	watching := make(chan struct{})

	go func() {
		defer close(watching)

		for {
			select {
			case <-stop:
				return
			default:
			}

			entries, err := os.ReadDir(dir)
			if err != nil {
				continue
			}

			for _, entry := range entries {
				name := entry.Name()
				if !strings.HasSuffix(name, ".json") || strings.HasPrefix(name, ".tmp-") {
					continue
				}

				record := filepath.Join(dir, name)

				if _, err := os.Stat(lockPathFor(record)); !os.IsNotExist(err) {
					continue
				}

				// The directory listing is a snapshot, so the record may simply have been
				// closed since. close removes the record before its lock, so a missing lock
				// is only a violation while the record is still there.
				if _, err := os.Stat(record); err != nil {
					continue
				}

				select {
				case violations <- name:
				default:
				}

				return
			}
		}
	}()

	for i := 0; i < 2000; i++ {
		dataPath := filepath.Join(t.TempDir(), "data")
		if err := os.MkdirAll(dataPath, 0o755); err != nil {
			t.Fatal(err)
		}

		record, err := openRecord(config)
		if err != nil {
			t.Fatalf("iteration %d: %v", i, err)
		}

		if err := record.setDataPath(dataPath, true); err != nil {
			t.Fatalf("iteration %d: %v", i, err)
		}

		record.close()
	}

	close(stop)
	<-watching

	select {
	case name := <-violations:
		t.Fatalf("record %s was visible in the registry before its lock existed", name)
	default:
	}
}

// TestCleanOrphansIgnoresRecordWithoutLock covers the other half of the fix: a record whose
// lock file is missing must not be reaped, and probing it must not manufacture a lock file.
func TestCleanOrphansIgnoresRecordWithoutLock(t *testing.T) {
	config := DefaultConfig().CachePath(t.TempDir())

	dir, err := registryDir(config)
	if err != nil {
		t.Fatal(err)
	}

	orphaned := filepath.Join(dir, "cluster-nolock.json")
	if err := os.WriteFile(orphaned, []byte(`{"data_path":"/nonexistent"}`), 0o644); err != nil {
		t.Fatal(err)
	}

	reclaimed, err := CleanOrphans(config)
	if err != nil {
		t.Fatal(err)
	}

	if reclaimed != 0 {
		t.Errorf("reclaimed %d records that had no lock, want 0", reclaimed)
	}

	if _, err := os.Stat(lockPathFor(orphaned)); !os.IsNotExist(err) {
		t.Error("probing a lockless record created a lock file, which would make a live cluster look dead")
	}
}

// newDataDir creates a directory named the way this library names them, so the sweep's
// data-directory passes recognise it.
func newDataDir(t *testing.T, id string) string {
	t.Helper()

	path := filepath.Join(os.TempDir(), dataDirPrefix+id)
	if err := os.MkdirAll(path, 0o755); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { os.RemoveAll(path) })

	return path
}

// TestCleanOrphansKillsUnrecordedServer covers the startup window: a cluster killed between
// spawning a server and recording its PID leaves a process no record names. Sweeping by data
// directory is what finds it.
func TestCleanOrphansKillsUnrecordedServer(t *testing.T) {
	config := DefaultConfig().CachePath(t.TempDir())

	record, err := openRecord(config)
	if err != nil {
		t.Fatal(err)
	}

	dataPath := newDataDir(t, record.id)

	if err := record.setDataPath(dataPath, true); err != nil {
		t.Fatal(err)
	}

	// Started, but deliberately never passed to addServer.
	unrecorded := blockedProcess(t, filepath.Join(t.TempDir(), "tikv-server"), dataPath)

	record.lock.release()

	if _, err := CleanOrphans(config); err != nil {
		t.Fatal(err)
	}

	waitForGone(t, unrecorded, "a server that was running but unrecorded survived the sweep")
}

// TestCleanOrphansReclaimsDataDirWithNoRecord covers a crash between claiming an identity and
// writing the record: the directory exists but nothing references it.
func TestCleanOrphansReclaimsDataDirWithNoRecord(t *testing.T) {
	config := DefaultConfig().CachePath(t.TempDir())

	dataPath := newDataDir(t, "orphan-no-record-test")

	// Backdate it past the grace period that protects clusters still starting up.
	old := time.Now().Add(-2 * orphanGracePeriod)
	if err := os.Chtimes(dataPath, old, old); err != nil {
		t.Fatal(err)
	}

	if _, err := CleanOrphans(config); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(dataPath); !os.IsNotExist(err) {
		t.Errorf("data directory with no record survived the sweep: %v", err)
	}
}

// TestCleanOrphansSparesYoungDataDirWithNoRecord is the safety half of the pass above: a
// cluster configured with a different CachePath is invisible to this registry, and briefly has
// no process referencing its directory either.
func TestCleanOrphansSparesYoungDataDirWithNoRecord(t *testing.T) {
	config := DefaultConfig().CachePath(t.TempDir())

	dataPath := newDataDir(t, "young-no-record-test")

	if _, err := CleanOrphans(config); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(dataPath); err != nil {
		t.Errorf("sweep removed a directory that could belong to a cluster still starting: %v", err)
	}
}

// TestCleanOrphansSparesDataDirInUse: no record, past the grace period, but a live process is
// using it. Age alone must not be enough.
func TestCleanOrphansSparesDataDirInUse(t *testing.T) {
	config := DefaultConfig().CachePath(t.TempDir())

	dataPath := newDataDir(t, "in-use-no-record-test")

	old := time.Now().Add(-2 * orphanGracePeriod)
	if err := os.Chtimes(dataPath, old, old); err != nil {
		t.Fatal(err)
	}

	live := blockedProcess(t, filepath.Join(t.TempDir(), "tikv-server"), dataPath)

	if _, err := CleanOrphans(config); err != nil {
		t.Fatal(err)
	}

	if !pidExists(live.Process.Pid) {
		t.Error("sweep killed a process using a directory it did not own")
	}

	if _, err := os.Stat(dataPath); err != nil {
		t.Errorf("sweep removed a directory still in use: %v", err)
	}
}

// TestCleanOrphansRemovesStaleLock covers a crash between claiming an identity and writing the
// record. The lock is tiny, but it would accumulate forever.
func TestCleanOrphansRemovesStaleLock(t *testing.T) {
	config := DefaultConfig().CachePath(t.TempDir())

	record, err := openRecord(config)
	if err != nil {
		t.Fatal(err)
	}

	lockPath := lockPathFor(record.path)

	// Simulate dying after the lock but before the record was written.
	record.lock.release()

	if err := os.Remove(record.path); err != nil {
		t.Fatal(err)
	}

	if _, err := CleanOrphans(config); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(lockPath); !os.IsNotExist(err) {
		t.Errorf("stale lock %s survived the sweep", lockPath)
	}
}

func waitForGone(t *testing.T, cmd *exec.Cmd, message string) {
	t.Helper()

	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal(message)
	}
}

// TestStopSweepsOrphansCreatedWhileRunning: the sweep on Start cannot help a cluster abandoned
// after this one started. Sweeping on Stop as well shortens how long it survives.
func TestStopSweepsOrphansCreatedWhileRunning(t *testing.T) {
	config := DefaultConfig().CachePath(t.TempDir())

	cluster := New(config)
	cluster.started = true // Stop's teardown tolerates a cluster that never really started

	// Somebody else's cluster dies after ours is already up, so Start's sweep never saw it.
	orphan, err := openRecord(config)
	if err != nil {
		t.Fatal(err)
	}

	dataPath := newDataDir(t, orphan.id)
	if err := orphan.setDataPath(dataPath, true); err != nil {
		t.Fatal(err)
	}

	orphan.lock.release()

	if err := cluster.Stop(); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(orphan.path); !os.IsNotExist(err) {
		t.Error("Stop did not sweep an orphan abandoned while this cluster was running")
	}

	if _, err := os.Stat(dataPath); !os.IsNotExist(err) {
		t.Error("Stop left the orphan's data directory behind")
	}
}
