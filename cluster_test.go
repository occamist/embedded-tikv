//go:build unix

package embeddedtikv

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/tikv/client-go/v2/rawkv"
)

// These tests run genuine pd-server and tikv-server processes. On a cold cache the first one
// downloads several hundred megabytes, so `go test -short` skips them.
func skipUnlessIntegration(t *testing.T) {
	t.Helper()

	if testing.Short() {
		t.Skip("skipping test that starts real TiKV servers")
	}
}

func TestClusterLifecycle(t *testing.T) {
	skipUnlessIntegration(t)

	cluster := New()

	if err := cluster.Start(); err != nil {
		t.Fatal(err)
	}

	dataPath := cluster.DataPath()

	if got := len(cluster.Endpoints()); got != 1 {
		t.Fatalf("Endpoints returned %d entries, want 1", got)
	}

	if got := len(cluster.StoreAddrs()); got != 1 {
		t.Fatalf("StoreAddrs returned %d entries, want 1", got)
	}

	// Start's contract is that the cluster is usable on return, not merely running.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var stores pdStoresResponse
	if err := getJSON(ctx, &http.Client{Timeout: 5 * time.Second}, cluster.ClientURLs()[0]+"/pd/api/v1/stores", &stores); err != nil {
		t.Fatal(err)
	}

	if stores.Count != 1 || stores.Stores[0].Store.StateName != "Up" {
		t.Fatalf("PD reports %d stores, first state %q; want 1 store Up", stores.Count, stores.Stores[0].Store.StateName)
	}

	if err := cluster.Start(); err != ErrClusterAlreadyStarted {
		t.Errorf("second Start returned %v, want ErrClusterAlreadyStarted", err)
	}

	if err := cluster.Stop(); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(dataPath); !os.IsNotExist(err) {
		t.Errorf("temporary data directory %s survived Stop", dataPath)
	}

	if err := cluster.Stop(); err != ErrClusterNotStarted {
		t.Errorf("second Stop returned %v, want ErrClusterNotStarted", err)
	}
}

// TestClusterStartsFromCacheWithoutNetwork guards the property that makes the library usable
// day to day: after the first run, starting a cluster must not touch the network.
func TestClusterStartsFromCacheWithoutNetwork(t *testing.T) {
	skipUnlessIntegration(t)

	// Port 1 is not listening, so any attempt to reach the mirror fails immediately.
	cluster := New(DefaultConfig().MirrorURL("http://127.0.0.1:1"))

	if err := cluster.Start(); err != nil {
		t.Fatal(err)
	}

	if err := cluster.Stop(); err != nil {
		t.Fatal(err)
	}
}

func TestClusterKeepsExplicitDataPath(t *testing.T) {
	skipUnlessIntegration(t)

	dataPath := t.TempDir()

	cluster := New(DefaultConfig().DataPath(dataPath))

	if err := cluster.Start(); err != nil {
		t.Fatal(err)
	}

	if err := cluster.Stop(); err != nil {
		t.Fatal(err)
	}

	// An explicitly chosen directory belongs to the caller, logs included.
	if _, err := os.Stat(dataPath); err != nil {
		t.Errorf("explicit data directory was removed: %v", err)
	}
}

// TestClusterWithMultipleStores covers the TiKVCount > 1 path, where PD keeps its own
// three-replica default rather than the single-replica override.
func TestClusterWithMultipleStores(t *testing.T) {
	skipUnlessIntegration(t)

	cluster := New(DefaultConfig().TiKVCount(3).StartTimeout(180 * time.Second))

	if err := cluster.Start(); err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := cluster.Stop(); err != nil {
			t.Error(err)
		}
	}()

	if got := len(cluster.StoreAddrs()); got != 3 {
		t.Fatalf("StoreAddrs returned %d entries, want 3", got)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var stores pdStoresResponse
	if err := getJSON(ctx, &http.Client{Timeout: 5 * time.Second}, cluster.ClientURLs()[0]+"/pd/api/v1/stores", &stores); err != nil {
		t.Fatal(err)
	}

	up := 0

	for _, store := range stores.Stores {
		if store.Store.StateName == "Up" {
			up++
		}
	}

	if up != 3 {
		t.Fatalf("PD reports %d stores Up, want 3", up)
	}
}

// TestConcurrentClusters is the scenario the library exists for: independent tests each holding
// their own cluster at the same time.
func TestConcurrentClusters(t *testing.T) {
	skipUnlessIntegration(t)

	const clusters = 3

	var (
		waiting   sync.WaitGroup
		mutex     sync.Mutex
		endpoints []string
	)

	for i := 0; i < clusters; i++ {
		waiting.Add(1)

		go func() {
			defer waiting.Done()

			cluster := New()

			if err := cluster.Start(); err != nil {
				t.Error(err)

				return
			}

			defer func() {
				if err := cluster.Stop(); err != nil {
					t.Error(err)
				}
			}()

			mutex.Lock()
			endpoints = append(endpoints, cluster.Endpoints()...)
			mutex.Unlock()
		}()
	}

	waiting.Wait()

	if len(endpoints) != clusters {
		t.Fatalf("started %d clusters, want %d", len(endpoints), clusters)
	}

	seen := map[string]bool{}

	for _, endpoint := range endpoints {
		if seen[endpoint] {
			t.Errorf("two clusters were given the same PD endpoint %s", endpoint)
		}

		seen[endpoint] = true
	}
}

// TestAbandonedChildLeavesNothingBehind starts a real cluster in a child process that exits
// without calling Stop, and checks that nothing survives it.
//
// It asserts the end state rather than which mechanism produced it. The child's exit closes its
// watchdog pipes, so the watchdogs begin cleaning up immediately; asserting that an intermediate
// state is still observable — that the data directory is briefly still there, or that a later
// sweep reclaims exactly one cluster — would be a race against them.
func TestAbandonedChildLeavesNothingBehind(t *testing.T) {
	skipUnlessIntegration(t)

	const (
		cacheEnv  = "EMBEDDED_TIKV_TEST_ABANDON_CACHE"
		binEnv    = "EMBEDDED_TIKV_TEST_ABANDON_BIN"
		reportEnv = "EMBEDDED_TIKV_TEST_ABANDON_REPORT"
	)

	// Child mode: start a cluster, report where it lives, then die without cleaning up.
	if cache := os.Getenv(cacheEnv); cache != "" {
		cluster := New(DefaultConfig().
			CachePath(cache).
			BinariesPath(os.Getenv(binEnv)).
			ReapOrphans(false))

		if err := cluster.Start(); err != nil {
			t.Fatal(err)
		}

		if err := os.WriteFile(os.Getenv(reportEnv), []byte(cluster.DataPath()), 0o644); err != nil {
			t.Fatal(err)
		}

		os.Exit(0)
	}

	// Reuse the already-populated default cache so the child does not download anything.
	installed, err := resolveBinaries(DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}

	cache := t.TempDir()
	report := filepath.Join(t.TempDir(), "datapath")

	child := exec.Command(os.Args[0], "-test.run=^TestAbandonedChildLeavesNothingBehind$")
	child.Env = append(os.Environ(),
		cacheEnv+"="+cache,
		binEnv+"="+filepath.Dir(installed.tikv),
		reportEnv+"="+report)

	if output, err := child.CombinedOutput(); err != nil {
		t.Fatalf("child failed: %v\n%s", err, output)
	}

	abandoned, err := os.ReadFile(report)
	if err != nil {
		t.Fatal(err)
	}

	dataPath := string(abandoned)

	waitForCondition(t, 30*time.Second, func() bool {
		_, err := os.Stat(dataPath)
		return os.IsNotExist(err)
	}, "data directory "+dataPath+" outlived the child that abandoned it")

	// A sweep afterwards must be safe, and must leave the registry empty either way.
	if _, err := CleanOrphans(DefaultConfig().CachePath(cache)); err != nil {
		t.Fatal(err)
	}

	entries, err := os.ReadDir(filepath.Join(cache, registryDirName))
	if err == nil && len(entries) != 0 {
		t.Errorf("registry still holds %d files after the child was abandoned and swept", len(entries))
	}
}

// TestWatchdogReclaimsClusterWhenParentIsKilled kills a parent outright and checks that its
// cluster goes with it.
//
// This exercises the shipped configuration on every platform: with a watchdog in place no
// Pdeathsig is requested, so the watchdog is the only thing standing between a SIGKILLed parent
// and a pair of abandoned servers here exactly as it is on macOS.
func TestWatchdogReclaimsClusterWhenParentIsKilled(t *testing.T) {
	skipUnlessIntegration(t)

	if !haveShell() {
		t.Skip("no /bin/sh, watchdog unavailable")
	}

	const (
		modeEnv   = "EMBEDDED_TIKV_TEST_WATCHDOG"
		binEnv    = "EMBEDDED_TIKV_TEST_WATCHDOG_BIN"
		cacheEnv  = "EMBEDDED_TIKV_TEST_WATCHDOG_CACHE"
		reportEnv = "EMBEDDED_TIKV_TEST_WATCHDOG_REPORT"
	)

	// Child: start a real cluster, report it, then wait to be killed.
	if os.Getenv(modeEnv) != "" {
		cluster := New(DefaultConfig().
			CachePath(os.Getenv(cacheEnv)).
			BinariesPath(os.Getenv(binEnv)).
			ReapOrphans(false))

		if err := cluster.Start(); err != nil {
			t.Fatal(err)
		}

		report := struct {
			DataPath string `json:"data_path"`
			PIDs     []int  `json:"pids"`
		}{DataPath: cluster.DataPath()}

		for _, server := range cluster.record.Servers {
			report.PIDs = append(report.PIDs, server.PID)
		}

		encoded, err := json.Marshal(report)
		if err != nil {
			t.Fatal(err)
		}

		if err := os.WriteFile(os.Getenv(reportEnv), encoded, 0o644); err != nil {
			t.Fatal(err)
		}

		select {} // wait to be killed
	}

	installed, err := resolveBinaries(DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}

	report := filepath.Join(t.TempDir(), "report.json")
	cachePath := t.TempDir()

	child := exec.Command(os.Args[0], "-test.run=^TestWatchdogReclaimsClusterWhenParentIsKilled$", "-test.timeout=0")
	child.Env = append(os.Environ(),
		modeEnv+"=1",
		binEnv+"="+filepath.Dir(installed.tikv),
		cacheEnv+"="+cachePath,
		reportEnv+"="+report)

	if err := child.Start(); err != nil {
		t.Fatal(err)
	}

	defer func() {
		_ = child.Process.Kill()
		_, _ = child.Process.Wait()
	}()

	var started struct {
		DataPath string `json:"data_path"`
		PIDs     []int  `json:"pids"`
	}

	deadline := time.Now().Add(120 * time.Second)

	for time.Now().Before(deadline) {
		raw, err := os.ReadFile(report)
		if err == nil && json.Unmarshal(raw, &started) == nil && len(started.PIDs) > 0 {
			break
		}

		time.Sleep(50 * time.Millisecond)
	}

	if len(started.PIDs) != 2 {
		t.Fatalf("child reported %d servers, want 2 (pd and tikv)", len(started.PIDs))
	}

	for _, pid := range started.PIDs {
		if !pidExists(pid) {
			t.Fatalf("server %d was not running before the parent was killed", pid)
		}
	}

	// The parent dies outright: no deferred cleanup, no signal handler, nothing.
	if err := child.Process.Kill(); err != nil {
		t.Fatal(err)
	}

	_, _ = child.Process.Wait()

	for _, pid := range started.PIDs {
		pid := pid
		waitForCondition(t, 30*time.Second, func() bool { return !pidExists(pid) },
			fmt.Sprintf("server %d outlived its killed parent", pid))
	}

	waitForCondition(t, 30*time.Second, func() bool {
		_, err := os.Stat(started.DataPath)
		return os.IsNotExist(err)
	}, "data directory "+started.DataPath+" outlived its killed parent")

	// The watchdog clears the registry last, so a killed parent leaves nothing at all — not
	// even the record and lock a later sweep would otherwise have to collect.
	waitForCondition(t, 30*time.Second, func() bool {
		entries, err := os.ReadDir(filepath.Join(cachePath, registryDirName))
		return err == nil && len(entries) == 0
	}, "registry entries outlived the killed parent")
}

func waitForCondition(t *testing.T, limit time.Duration, condition func() bool, message string) {
	t.Helper()

	deadline := time.Now().Add(limit)

	for time.Now().Before(deadline) {
		if condition() {
			return
		}

		time.Sleep(50 * time.Millisecond)
	}

	t.Fatal(message)
}

// TestChecksumMatchesAcrossClusters starts two independent clusters, writes the same three keys
// to each, and asks TiKV for its own checksum of both.
//
// It is a stronger statement than starting and stopping: not just that a write came back, but
// that two separately started clusters ended up holding byte-identical data. A mismatch would
// mean the library had leaked state between clusters, or that a cluster started with data
// already in it.
func TestChecksumMatchesAcrossClusters(t *testing.T) {
	skipUnlessIntegration(t)

	ctx := context.Background()

	pairs := []struct{ key, value []byte }{
		{[]byte("checksum/a"), []byte("alpha")},
		{[]byte("checksum/b"), []byte("beta")},
		{[]byte("checksum/c"), []byte("gamma")},
	}

	// Both clusters stay up for the whole test, so this also exercises two running at once.
	checksums := make([]rawkv.RawChecksum, 2)

	for i := range checksums {
		cluster := New()

		if err := cluster.Start(); err != nil {
			t.Fatalf("cluster %d: %v", i, err)
		}

		t.Cleanup(func() {
			if err := cluster.Stop(); err != nil {
				t.Error(err)
			}
		})

		client, err := rawkv.NewClientWithOpts(ctx, cluster.Endpoints())
		if err != nil {
			t.Fatalf("cluster %d: %v", i, err)
		}

		t.Cleanup(func() { client.Close() })

		for _, pair := range pairs {
			if err := client.Put(ctx, pair.key, pair.value); err != nil {
				t.Fatalf("cluster %d: put %s: %v", i, pair.key, err)
			}
		}

		// An empty end key means "to the end of the keyspace", so this covers everything the
		// cluster holds rather than just the range we wrote.
		checksums[i], err = client.Checksum(ctx, []byte(""), []byte(""))
		if err != nil {
			t.Fatalf("cluster %d: checksum: %v", i, err)
		}
	}

	if checksums[0].TotalKvs != uint64(len(pairs)) {
		t.Errorf("cluster 0 holds %d keys, want %d — a fresh cluster should hold only what we wrote",
			checksums[0].TotalKvs, len(pairs))
	}

	if checksums[0] != checksums[1] {
		t.Errorf("two clusters given identical writes disagree:\n  cluster 0: %+v\n  cluster 1: %+v",
			checksums[0], checksums[1])
	}
}
