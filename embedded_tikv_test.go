//go:build unix

package embeddedtikv

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// stubBinaries writes a directory of fake servers so the failure paths can be exercised without
// downloading or running real ones.
func stubBinaries(t *testing.T, script string) string {
	t.Helper()

	directory := t.TempDir()

	for _, name := range []string{binaryPD, binaryTiKV} {
		if err := os.WriteFile(filepath.Join(directory, name), []byte(script), 0o755); err != nil {
			t.Fatal(err)
		}
	}

	return directory
}

func TestStartRejectsZeroTiKVCount(t *testing.T) {
	cluster := New(DefaultConfig().TiKVCount(0))

	if err := cluster.Start(); err == nil {
		t.Fatal("expected TiKVCount(0) to be rejected")
	}
}

func TestStopBeforeStart(t *testing.T) {
	if err := New().Stop(); err != ErrClusterNotStarted {
		t.Fatalf("Stop returned %v, want ErrClusterNotStarted", err)
	}
}

func TestStartSurfacesServerOutputOnFailure(t *testing.T) {
	directory := stubBinaries(t, "#!/bin/sh\necho 'boom: config was rejected' >&2\nexit 1\n")

	cluster := New(DefaultConfig().
		BinariesPath(directory).
		DataPath(t.TempDir()))

	err := cluster.Start()
	if err == nil {
		t.Fatal("expected a start failure")
	}

	// A bare timeout would be useless; the server's own complaint is the diagnosis.
	if !strings.Contains(err.Error(), "boom: config was rejected") {
		t.Errorf("error does not include the server output:\n%v", err)
	}

	if !strings.Contains(err.Error(), "pd-0") {
		t.Errorf("error does not name the failing server:\n%v", err)
	}
}

func TestStartWritesConfigFilesServersCanRead(t *testing.T) {
	// A server that exits immediately is enough to get the config written to disk.
	directory := stubBinaries(t, "#!/bin/sh\nexit 0\n")
	dataPath := t.TempDir()

	cluster := New(DefaultConfig().
		BinariesPath(directory).
		DataPath(dataPath).
		PDConfig(map[string]any{"replication.max-replicas": 3}).
		TiKVConfig(map[string]any{"storage.api-version": 2}))

	// Expected to fail: the stub never becomes ready. The configs are the artefact under test.
	_ = cluster.Start()

	pdConfig, err := os.ReadFile(filepath.Join(dataPath, "pd", "pd.toml"))
	if err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(string(pdConfig), "max-replicas = 3") {
		t.Errorf("PDConfig override missing from pd.toml:\n%s", pdConfig)
	}

	if !strings.Contains(string(pdConfig), "low-space-ratio = 1.0") {
		t.Errorf("pd.toml must keep low-space-ratio a float, or PD refuses to load it:\n%s", pdConfig)
	}
}

func TestIsPortConflict(t *testing.T) {
	for _, message := range []string{
		"listen tcp 127.0.0.1:2379: bind: address already in use",
		"Address already in use (os error 98)",
	} {
		if !isPortConflict(errString(message)) {
			t.Errorf("%q should be treated as a port conflict", message)
		}
	}

	if isPortConflict(errString("permission denied")) {
		t.Error("unrelated failures must not be retried as port conflicts")
	}

	if isPortConflict(nil) {
		t.Error("nil is not a port conflict")
	}
}

type errString string

func (e errString) Error() string { return string(e) }
