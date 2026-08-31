//go:build unix

package embeddedtikv

import (
	"context"
	"net"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
)

// tikvInstance is one tikv-server in a cluster.
type tikvInstance struct {
	name       string
	host       string
	dir        string
	configPath string
	logPath    string
	port       int
	statusPort int
	proc       *process
}

func (t *tikvInstance) addr() string {
	return net.JoinHostPort(t.host, strconv.Itoa(t.port))
}

func (t *tikvInstance) statusAddr() string {
	return net.JoinHostPort(t.host, strconv.Itoa(t.statusPort))
}

func (t *tikvInstance) args(pdEndpoints []string) []string {
	return []string{
		"--addr=" + t.addr(),
		"--advertise-addr=" + t.addr(),
		"--status-addr=" + t.statusAddr(),
		"--pd-endpoints=" + strings.Join(pdEndpoints, ","),
		"--config=" + t.configPath,
		"--data-dir=" + filepath.Join(t.dir, "data"),
	}
}

// defaultTiKVConfig keeps a test-scale TiKV inside test-scale resources. Every entry here is
// load-bearing: TiKV's own defaults are tuned for a dedicated production node.
func defaultTiKVConfig() map[string]any {
	config := map[string]any{
		// TiKV reserves 5GiB of disk by default and refuses to start without it.
		"storage.reserve-space":      0,
		"storage.reserve-raft-space": 0,
		// The block cache defaults to 45% of system memory, which is untenable once a test
		// suite runs several clusters at once.
		"storage.block-cache.capacity": "128MB",
		// Keep well clear of the open-file limit, which is commonly low on macOS and in CI.
		"rocksdb.max-open-files": 256,
		"raftdb.max-open-files":  256,
		// PD marks a newly registered store Down until its first store heartbeat arrives, and
		// waitStoresUp gates on that. At the stock 10s interval this alone adds ~10s to Start.
		//
		// This is the only heartbeat this library touches. It governs how often TiKV reports
		// liveness and capacity to PD; it has no effect on the data path. The region heartbeat
		// (raftstore.pd-heartbeat-tick-interval), which does drive PD's region view and
		// scheduling decisions, is deliberately left at TiKV's default.
		"raftstore.pd-store-heartbeat-tick-interval": "500ms",
		// This does not disable graceful shutdown. It bounds one phase of it: handing region
		// leaders to a surviving peer before exiting. Stop tears the whole cluster down, so
		// there is never a surviving peer to hand them to, at any store count — TiKV just
		// waits out the timeout and exits anyway. Measured at 20.6s per Stop with one store,
		// and worse with three, where each store waits in turn.
		//
		// The rest of the shutdown sequence is untouched and still runs in full: raftstore
		// stop, batch-system drain, RocksDB flush, "Storage stopped".
		"server.graceful-shutdown-timeout": "0s",
	}

	return config
}

type pdStoresResponse struct {
	Count  int `json:"count"`
	Stores []struct {
		Store struct {
			ID        uint64 `json:"id"`
			StateName string `json:"state_name"`
		} `json:"store"`
	} `json:"stores"`
}

// waitStoresUp blocks until PD reports the expected number of stores in the Up state.
//
// A started tikv-server is not the same as a usable one: it has to register with PD and have
// its first region become available before a client write can succeed. `tiup playground` has no
// equivalent check in tikv-slim mode because it has no client to satisfy; a test library does.
func waitStoresUp(ctx context.Context, client *http.Client, pdClientURL string, want int, guard func() error) error {
	return poll(ctx, func() (bool, error) {
		if err := guard(); err != nil {
			return false, err
		}

		var stores pdStoresResponse
		if err := getJSON(ctx, client, pdClientURL+"/pd/api/v1/stores", &stores); err != nil {
			return false, nil
		}

		up := 0

		for _, store := range stores.Stores {
			if store.Store.StateName == "Up" {
				up++
			}
		}

		return up >= want, nil
	})
}
