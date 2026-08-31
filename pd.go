//go:build unix

package embeddedtikv

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"strconv"
	"time"
)

// pdInstance is the single pd-server backing a cluster.
type pdInstance struct {
	name       string
	host       string
	dir        string
	configPath string
	logPath    string
	peerPort   int
	clientPort int
	proc       *process
}

// clientURL is the address TiKV and clients talk to PD on.
func (p *pdInstance) clientURL() string {
	return "http://" + net.JoinHostPort(p.host, strconv.Itoa(p.clientPort))
}

func (p *pdInstance) endpoint() string {
	return net.JoinHostPort(p.host, strconv.Itoa(p.clientPort))
}

func (p *pdInstance) peerURL() string {
	return "http://" + net.JoinHostPort(p.host, strconv.Itoa(p.peerPort))
}

func (p *pdInstance) args() []string {
	return []string{
		"--name=" + p.name,
		"--config=" + p.configPath,
		"--data-dir=" + filepath.Join(p.dir, "data"),
		"--peer-urls=" + p.peerURL(),
		"--advertise-peer-urls=" + p.peerURL(),
		"--client-urls=" + p.clientURL(),
		"--advertise-client-urls=" + p.clientURL(),
		// A single-member cluster still has to be told it is bootstrapping rather than joining.
		fmt.Sprintf("--initial-cluster=%s=%s", p.name, p.peerURL()),
	}
}

// defaultPDConfig mirrors the overrides `tiup playground` applies, which are what make a
// small local cluster usable rather than merely running.
func defaultPDConfig(tikvCount int) map[string]any {
	config := map[string]any{
		// PD is a single etcd member here, but it still waits out a full election timeout
		// before campaigning: measured at a median of 1.65s and as much as 3.15s, entirely
		// idle. Shrinking the tick and election intervals removes both the delay and its
		// variance. A lone member cannot lose an election, so there is nothing to trade away.
		"election-interval": "500ms",
		"tick-interval":     "50ms",
		// Scan regions aggressively so a fresh cluster settles in seconds, not minutes.
		"schedule.patrol-region-interval": "100ms",
		// Developer machines and CI runners are routinely low on disk; without this PD
		// declares the only store low on space and stops scheduling to it.
		"schedule.low-space-ratio": 1.0,
	}

	// Three replicas cannot reach quorum on fewer than three stores, so regions would stay
	// unavailable forever. Above that, leave PD's default alone.
	if tikvCount < 3 {
		config["replication.max-replicas"] = 1
	}

	return config
}

type pdMembersResponse struct {
	Header struct {
		ClusterID uint64 `json:"cluster_id"`
	} `json:"header"`
}

// waitPDReady blocks until PD reports a bootstrapped cluster, which is the point at which
// tikv-server can be pointed at it. guard is consulted on every tick so that a server which
// dies during start-up surfaces its own error instead of a timeout.
func waitPDReady(ctx context.Context, client *http.Client, clientURL string, guard func() error) error {
	return poll(ctx, func() (bool, error) {
		if err := guard(); err != nil {
			return false, err
		}

		var members pdMembersResponse
		if err := getJSON(ctx, client, clientURL+"/pd/api/v1/members", &members); err != nil {
			return false, nil
		}

		return members.Header.ClusterID != 0, nil
	})
}

func getJSON(ctx context.Context, client *http.Client, url string, into any) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}

	response, err := client.Do(request)
	if err != nil {
		return err
	}

	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("%s returned %s", url, response.Status)
	}

	return json.NewDecoder(response.Body).Decode(into)
}

// poll runs check every 100ms until it reports done, the context expires, or it errors.
func poll(ctx context.Context, check func() (bool, error)) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		done, err := check()
		if err != nil {
			return err
		}

		if done {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
