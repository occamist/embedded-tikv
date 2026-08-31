//go:build unix

package embeddedtikv

import (
	"strings"
	"testing"

	"github.com/BurntSushi/toml"
)

// TestWriteTOMLProducesTablesFromDottedKeys asserts the decoded structure rather than exact
// bytes: the encoder owns formatting, we own the dotted-path expansion.
func TestWriteTOMLProducesTablesFromDottedKeys(t *testing.T) {
	var out strings.Builder

	err := writeTOML(&out, map[string]any{
		"storage.reserve-space":           0,
		"storage.block-cache.capacity":    "128MB",
		"schedule.patrol-region-interval": "100ms",
		"election-interval":               "500ms",
	})
	if err != nil {
		t.Fatalf("writeTOML returned %v", err)
	}

	var got map[string]any
	if _, err := toml.Decode(out.String(), &got); err != nil {
		t.Fatalf("emitted invalid TOML: %v\n%s", err, out.String())
	}

	storage, ok := got["storage"].(map[string]any)
	if !ok {
		t.Fatalf("storage is %T, want a table", got["storage"])
	}

	if cache, ok := storage["block-cache"].(map[string]any); !ok || cache["capacity"] != "128MB" {
		t.Errorf("storage.block-cache.capacity did not nest correctly: %#v", storage)
	}

	// PD's own config mixes bare keys with tables, and bare keys must precede every table
	// header or they would be swallowed into one.
	if got["election-interval"] != "500ms" {
		t.Errorf("top-level key lost: %#v", got["election-interval"])
	}
}

// TestWriteTOMLKeepsFloatsFloats guards the bug that broke PD on the first run: a TOML number
// with no decimal point is an integer, and PD refuses to load one into a float field.
func TestWriteTOMLKeepsFloatsFloats(t *testing.T) {
	var out strings.Builder

	if err := writeTOML(&out, map[string]any{"schedule.low-space-ratio": 1.0}); err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(out.String(), "low-space-ratio = 1.0") {
		t.Errorf("float lost its decimal point:\n%s", out.String())
	}

	var got struct {
		Schedule struct {
			LowSpaceRatio float64 `toml:"low-space-ratio"`
		} `toml:"schedule"`
	}

	if _, err := toml.Decode(out.String(), &got); err != nil {
		t.Fatalf("a float field would not decode: %v", err)
	}
}

// TestWriteTOMLEscapesHostileInput covers what the hand-rolled writer got wrong: Go string
// escaping is not TOML escaping, and keys that are not bare identifiers must be quoted.
func TestWriteTOMLEscapesHostileInput(t *testing.T) {
	for name, config := range map[string]map[string]any{
		"control character":  {"server.labels.ctl": "bell\x07here"},
		"key needing quotes": {"server.labels.weird key": "a"},
		"quote in value":     {"server.labels.q": `he said "hi"`},
	} {
		var out strings.Builder
		if err := writeTOML(&out, config); err != nil {
			t.Errorf("%s: writeTOML returned %v", name, err)

			continue
		}

		var got map[string]any
		if _, err := toml.Decode(out.String(), &got); err != nil {
			t.Errorf("%s: emitted invalid TOML: %v\n%s", name, err, out.String())
		}
	}
}

func TestWriteTOMLReportsConflictingPaths(t *testing.T) {
	var out strings.Builder

	// "storage.reserve-space" cannot be both a value and a table.
	err := writeTOML(&out, map[string]any{
		"storage.reserve-space":      0,
		"storage.reserve-space.deep": 1,
	})
	if err == nil {
		t.Fatal("expected conflicting paths to be reported")
	}

	if !strings.Contains(err.Error(), "conflict") && !strings.Contains(err.Error(), "also used as a table") {
		t.Errorf("error does not explain the conflict: %v", err)
	}
}

func TestMergeConfigOverridesWithoutMutating(t *testing.T) {
	defaults := map[string]any{"storage.reserve-space": 0, "rocksdb.max-open-files": 256}
	overrides := map[string]any{"rocksdb.max-open-files": 1024, "storage.api-version": 2}

	merged := mergeConfig(defaults, overrides)

	if merged["rocksdb.max-open-files"] != 1024 {
		t.Errorf("override not applied: %v", merged["rocksdb.max-open-files"])
	}

	if merged["storage.reserve-space"] != 0 {
		t.Errorf("default not preserved: %v", merged["storage.reserve-space"])
	}

	if merged["storage.api-version"] != 2 {
		t.Errorf("new key not added: %v", merged["storage.api-version"])
	}

	if defaults["rocksdb.max-open-files"] != 256 {
		t.Error("mergeConfig mutated the defaults map")
	}
}

func TestDefaultPDConfigTightensElection(t *testing.T) {
	config := defaultPDConfig(1)

	// A single-member PD otherwise idles through a full election timeout before campaigning,
	// which measured as a median of 1.65s and up to 3.15s of pure waiting.
	if config["election-interval"] != "500ms" || config["tick-interval"] != "50ms" {
		t.Errorf("election tuning missing: %v / %v", config["election-interval"], config["tick-interval"])
	}
}

func TestDefaultTiKVConfigTouchesOnlyTheLivenessHeartbeat(t *testing.T) {
	config := defaultTiKVConfig()

	// The store heartbeat gates PD's state_name, which is what waitStoresUp waits for.
	if config["raftstore.pd-store-heartbeat-tick-interval"] != "500ms" {
		t.Errorf("store heartbeat not tuned: %v", config["raftstore.pd-store-heartbeat-tick-interval"])
	}

	// The region heartbeat drives PD's region view and scheduling decisions, so it is
	// deliberately left at TiKV's default. Overriding it changes cluster behaviour under test
	// for no gain, since it does not affect how quickly a cluster becomes usable.
	if _, overridden := config["raftstore.pd-heartbeat-tick-interval"]; overridden {
		t.Error("region heartbeat must be left at TiKV's default")
	}
}

func TestDefaultTiKVConfigSkipsLeaderEvictionOnShutdown(t *testing.T) {
	// Stop tears the whole cluster down, so there is never a peer to hand leaders to. This
	// bounds only that phase; the rest of the shutdown sequence still runs.
	if got := defaultTiKVConfig()["server.graceful-shutdown-timeout"]; got != "0s" {
		t.Errorf("graceful-shutdown-timeout = %v, want 0s", got)
	}
}

func TestDefaultPDConfigReplicaCount(t *testing.T) {
	// Fewer than three stores can never reach quorum on three replicas.
	for _, count := range []int{1, 2} {
		if got := defaultPDConfig(count)["replication.max-replicas"]; got != 1 {
			t.Errorf("defaultPDConfig(%d) max-replicas = %v, want 1", count, got)
		}
	}

	if _, set := defaultPDConfig(3)["replication.max-replicas"]; set {
		t.Error("defaultPDConfig(3) should leave PD's own default in place")
	}
}
