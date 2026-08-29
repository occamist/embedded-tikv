//go:build unix

package embeddedtikv

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

const (
	// registryDirName holds one record per running cluster, under the shared cache directory
	// so that a sweep in any project on the machine can find clusters abandoned by any other.
	registryDirName = "run"

	recordPrefix = "cluster-"
	// dataDirPrefix ties a cluster's data directory to its record: the directory for record
	// cluster-<id> is <TMPDIR>/embedded-tikv-<id>. That link is what lets a sweep reclaim a
	// data directory whose record was lost or never finished being written.
	dataDirPrefix = "embedded-tikv-"
)

// orphanGracePeriod is how long a data directory with no lock at all must go untouched before a
// sweep will reclaim it.
//
// A cluster configured with a different CachePath keeps its records in a different registry and
// is therefore invisible here, and in the moment between creating its data directory and
// starting its first server, no process references it either. Neither the lock nor the process
// scan can see such a cluster, so young directories are left alone. A genuine orphan is simply
// reclaimed by a later sweep instead.
const orphanGracePeriod = 5 * time.Minute

// clusterRecord is what a running cluster leaves behind so it can be reclaimed if its owning
// process dies without calling Stop.
type clusterRecord struct {
	Created      time.Time      `json:"created"`
	OwnerPID     int            `json:"owner_pid"`
	DataPath     string         `json:"data_path"`
	OwnsDataPath bool           `json:"owns_data_path"`
	Servers      []serverRecord `json:"servers"`

	// Runtime state, not serialised.
	id   string
	path string
	lock *fileLock
}

type serverRecord struct {
	Name   string `json:"name"`
	PID    int    `json:"pid"`
	Binary string `json:"binary"`
}

func registryDir(cfg Config) (string, error) {
	cache, err := cacheDirectory(cfg)
	if err != nil {
		return "", err
	}

	dir := filepath.Join(cache, registryDirName)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("embedded-tikv: unable to create registry %s: %w", dir, err)
	}

	return dir, nil
}

func lockPathFor(recordPath string) string {
	return strings.TrimSuffix(recordPath, ".json") + ".lock"
}

func recordPathFor(lockPath string) string {
	return strings.TrimSuffix(lockPath, ".lock") + ".json"
}

// defaultDataDir is where a cluster owned by this library keeps its state. The name embeds the
// record id so the two can always be matched up again.
func defaultDataDir(id string) string {
	return filepath.Join(os.TempDir(), dataDirPrefix+id)
}

// openRecord claims an identity for a new cluster and takes the lock that marks it live.
//
// The lock is held for as long as the process runs. Because the kernel drops it on process exit
// — clean, panicking, or killed — a sweep that can take the lock knows the owner is gone.
//
// The lock is created before anything else exists, so neither a record nor a data directory is
// ever visible without a held lock behind it. A sweep observing either can therefore trust what
// the lock tells it.
func openRecord(cfg Config) (*clusterRecord, error) {
	dir, err := registryDir(cfg)
	if err != nil {
		return nil, err
	}

	lockFile, err := os.CreateTemp(dir, recordPrefix+"*.lock")
	if err != nil {
		return nil, fmt.Errorf("embedded-tikv: unable to create cluster lock: %w", err)
	}

	lockPath := lockFile.Name()
	lockFile.Close()

	lock, err := acquireFileLock(lockPath, true, true)
	if err != nil {
		os.Remove(lockPath)

		return nil, err
	}

	record := &clusterRecord{
		Created:  time.Now(),
		OwnerPID: os.Getpid(),
		id:       strings.TrimSuffix(strings.TrimPrefix(filepath.Base(lockPath), recordPrefix), ".lock"),
		path:     recordPathFor(lockPath),
		lock:     lock,
	}

	return record, record.save()
}

// setDataPath records where the cluster keeps its state, once that is known.
func (r *clusterRecord) setDataPath(path string, owned bool) error {
	if r == nil {
		return nil
	}

	r.DataPath = path
	r.OwnsDataPath = owned

	return r.save()
}

// addServer records a started server and flushes immediately, so a crash part-way through
// startup still leaves every running process accounted for.
func (r *clusterRecord) addServer(name string, pid int, binary string) error {
	if r == nil {
		return nil
	}

	r.Servers = append(r.Servers, serverRecord{Name: name, PID: pid, Binary: binary})

	return r.save()
}

func (r *clusterRecord) save() error {
	encoded, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}

	// Written via temp-and-rename so a sweep never observes a half-written record.
	temp, err := os.CreateTemp(filepath.Dir(r.path), ".tmp-*")
	if err != nil {
		return fmt.Errorf("embedded-tikv: unable to write cluster record: %w", err)
	}

	if _, err := temp.Write(encoded); err != nil {
		temp.Close()
		os.Remove(temp.Name())

		return fmt.Errorf("embedded-tikv: unable to write cluster record: %w", err)
	}

	if err := temp.Close(); err != nil {
		os.Remove(temp.Name())

		return err
	}

	return os.Rename(temp.Name(), r.path)
}

// close releases the lock and deletes the record, marking the cluster cleanly stopped.
func (r *clusterRecord) close() {
	if r == nil {
		return
	}

	r.lock.release()
	os.Remove(r.path)
	os.Remove(lockPathFor(r.path))
}

// abandon releases the lock but leaves the record in place, for a teardown that could not
// confirm every server was gone. Deleting it would strip away the only thing a later sweep
// could use to find the survivor.
func (r *clusterRecord) abandon() {
	if r == nil {
		return
	}

	r.lock.release()
}

// CleanOrphans reclaims clusters whose owning process died without calling Stop: it kills any
// servers still running and removes the data directories they left behind. It returns the
// number of clusters reclaimed.
//
// Start calls this automatically unless disabled with Config.ReapOrphans(false); it is exported
// for suites that would rather sweep explicitly, for example from TestMain.
//
// It is conservative by construction and will not touch a live cluster. Nothing is reclaimed
// unless its lock can be acquired, which the kernel permits only once the owning process has
// exited, and nothing is killed unless the process still names that cluster's own unique data
// directory on its command line.
//
// The sweep runs three passes, because a record alone cannot describe every way a cluster can
// be abandoned:
//
//  1. Records, which is the ordinary case.
//  2. Data directories whose record was lost or never finished being written. Without this a
//     crash during startup could strand a directory that nothing referenced.
//  3. Locks with no record, left by a crash between claiming an identity and writing it.
func CleanOrphans(config ...Config) (int, error) {
	cfg := DefaultConfig()
	if len(config) > 0 {
		cfg = config[0]
	}

	dir, err := registryDir(cfg)
	if err != nil {
		return 0, err
	}

	reclaimed := sweepRecords(dir)
	reclaimed += sweepDataDirectories(dir)

	sweepStaleLocks(dir)

	return reclaimed, nil
}

func sweepRecords(dir string) int {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0
	}

	reclaimed := 0

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}

		if reclaimRecord(filepath.Join(dir, entry.Name())) {
			reclaimed++
		}
	}

	return reclaimed
}

// reclaimRecord reclaims a single record, reporting whether it did.
func reclaimRecord(path string) bool {
	// Taking the lock is the liveness test: if the owner were alive it would still hold it.
	// The lock file is never created here — a record without one is malformed, and inventing
	// a fresh lock would make a live cluster look abandoned.
	lock, err := acquireFileLock(lockPathFor(path), false, false)
	if err != nil || lock == nil {
		return false
	}

	defer lock.release()

	raw, err := os.ReadFile(path)
	if err != nil {
		return false
	}

	var record clusterRecord
	if err := json.Unmarshal(raw, &record); err != nil {
		// An unreadable record is still worth clearing away, but nothing may be killed on
		// the strength of it.
		os.Remove(path)
		os.Remove(lockPathFor(path))

		return false
	}

	for _, server := range record.Servers {
		if processIsServer(server.PID, server.Binary, record.DataPath) {
			_ = signalGroup(server.PID, syscall.SIGKILL)
		}
	}

	// Servers started but not yet recorded are invisible above. Sweeping by data directory
	// catches them, and is only safe for directories this library named, whose paths cannot
	// coincide with anything else on the machine.
	if record.OwnsDataPath {
		killProcessesUsing(record.DataPath)
	}

	if record.OwnsDataPath && record.DataPath != "" {
		os.RemoveAll(record.DataPath)
	}

	os.Remove(path)
	os.Remove(lockPathFor(path))

	return true
}

// sweepDataDirectories reclaims data directories whose record is missing. A cluster killed
// between taking its lock and writing its record would otherwise strand one.
func sweepDataDirectories(registry string) int {
	entries, err := os.ReadDir(os.TempDir())
	if err != nil {
		return 0
	}

	reclaimed := 0

	for _, entry := range entries {
		if !entry.IsDir() || !strings.HasPrefix(entry.Name(), dataDirPrefix) {
			continue
		}

		id := strings.TrimPrefix(entry.Name(), dataDirPrefix)
		dataPath := filepath.Join(os.TempDir(), entry.Name())
		lockPath := filepath.Join(registry, recordPrefix+id+".lock")

		// A record still exists for this directory: pass 1 owns it, whether or not it was
		// able to reclaim it.
		if _, err := os.Stat(recordPathFor(lockPath)); err == nil {
			continue
		}

		if _, err := os.Stat(lockPath); err == nil {
			// The lock exists, so liveness is knowable: leave it to the lock.
			lock, err := acquireFileLock(lockPath, false, false)
			if err != nil || lock == nil {
				continue
			}

			lock.release()
		} else {
			// No lock at all — the registry was wiped, or belongs to a different CachePath.
			// The only evidence left is age and whether anything is still using it.
			info, err := entry.Info()
			if err != nil || time.Since(info.ModTime()) < orphanGracePeriod {
				continue
			}

			if usedByLiveProcess(dataPath) {
				continue
			}
		}

		killProcessesUsing(dataPath)
		os.RemoveAll(dataPath)
		os.Remove(lockPath)

		reclaimed++
	}

	return reclaimed
}

// sweepStaleLocks removes locks with no record, left by a crash between claiming an identity
// and writing it. They are tiny, but they accumulate forever otherwise.
func sweepStaleLocks(dir string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".lock") {
			continue
		}

		lockPath := filepath.Join(dir, entry.Name())

		if _, err := os.Stat(recordPathFor(lockPath)); err == nil {
			continue
		}

		lock, err := acquireFileLock(lockPath, false, false)
		if err != nil || lock == nil {
			continue
		}

		lock.release()
		os.Remove(lockPath)
	}
}

// processIsServer reports whether pid is still running the exact server this record describes.
//
// Both the binary path and the cluster's data directory must appear in the command line. The
// data directory is unique per cluster, which is what makes this safe: no process outside this
// cluster can match, so an unrelated TiKV — including a production one started from the same
// binary — can never be mistaken for an orphan.
func processIsServer(pid int, binary, dataPath string) bool {
	if pid <= 0 || binary == "" || dataPath == "" {
		return false
	}

	cmdline, ok := processCommandLine(pid)
	if !ok {
		return false
	}

	return strings.Contains(cmdline, binary) && cmdlineReferences(cmdline, dataPath)
}

// cmdlineReferences reports whether a command line names this data directory.
//
// The match requires the trailing separator. Record ids come from os.CreateTemp, whose random
// component is a variable-length decimal, so one id can be a strict prefix of another —
// /tmp/embedded-tikv-123 against /tmp/embedded-tikv-1234. A plain substring test would see a
// live cluster as part of the orphan next door and kill it. Every path this library passes to a
// server is a child of the data directory, so the separator is always present.
func cmdlineReferences(cmdline, dataPath string) bool {
	return dataPath != "" && strings.Contains(cmdline, dataPath+string(os.PathSeparator))
}

// ownsDataDirName reports whether a path is one this library generated. Sweeping by data
// directory is only safe for these: the name is unique and cannot collide with a caller's own
// directory, let alone an unrelated process's arguments.
func ownsDataDirName(dataPath string) bool {
	return dataPath != "" && strings.HasPrefix(filepath.Base(dataPath), dataDirPrefix)
}

// usedByLiveProcess reports whether any visible process references this data directory.
func usedByLiveProcess(dataPath string) bool {
	if !ownsDataDirName(dataPath) {
		return true // unknown, so assume in use and leave it alone
	}

	for _, entry := range enumerateProcesses() {
		if cmdlineReferences(entry.cmdline, dataPath) {
			return true
		}
	}

	return false
}

// killProcessesUsing kills every process whose command line references this data directory.
// This is the backstop for servers that were started but never recorded.
func killProcessesUsing(dataPath string) {
	if !ownsDataDirName(dataPath) {
		return
	}

	self := os.Getpid()

	for _, entry := range enumerateProcesses() {
		if entry.pid == self || !cmdlineReferences(entry.cmdline, dataPath) {
			continue
		}

		_ = signalGroup(entry.pid, syscall.SIGKILL)
	}
}
