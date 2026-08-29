//go:build unix

// Package embeddedtikv runs a real TiKV cluster from a Go test.
//
// A cluster is one pd-server and one or more tikv-server processes, started on ephemeral ports
// against a temporary data directory, using official binaries downloaded once and cached under
// ~/.embedded-tikv. Start returns when the cluster is genuinely writable, so a client built on
// Endpoints can be used immediately:
//
//	cluster := embeddedtikv.New()
//	if err := cluster.Start(); err != nil {
//		t.Fatal(err)
//	}
//	defer cluster.Stop()
//
//	client, err := rawkv.NewClientWithOpts(ctx, cluster.Endpoints())
//
// The configuration mirrors `tiup playground --mode tikv-slim --without-monitor`: no TiDB, no
// monitoring, and no multi-host deployment.
package embeddedtikv

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// startAttempts bounds retries when a server loses a race for one of its ports. Ports are
// allocated by binding to :0 and closing, so another process on the machine can always take one
// in the gap before the server binds it.
const startAttempts = 3

// shutdownTimeout is how long each server is given to exit on SIGTERM before being killed.
const shutdownTimeout = 10 * time.Second

// Cluster is the lifecycle handle for one embedded TiKV cluster. Start and Stop are safe to
// call concurrently; independent Cluster values are fully independent.
type Cluster struct {
	// mu makes Start and Stop safe to call from more than one goroutine, so that a caller can
	// stop the cluster from a signal handler or a context watcher while the test that owns it
	// is still running.
	mu      sync.Mutex
	config  Config
	client  *http.Client
	started bool

	dataPath     string
	ownsDataPath bool
	binaries     binaries

	pd     *pdInstance
	tikvs  []*tikvInstance
	record *clusterRecord
}

// New creates a Cluster. Called without arguments it uses DefaultConfig; otherwise the first
// Config is used.
func New(config ...Config) *Cluster {
	settings := DefaultConfig()
	if len(config) > 0 {
		settings = config[0]
	}

	return &Cluster{
		config: settings,
		client: &http.Client{Timeout: 5 * time.Second},
	}
}

// Start downloads the binaries if they are not already cached, boots PD and TiKV, and waits
// until every store reports Up. It returns only when the cluster can serve reads and writes.
//
// On failure everything already started is torn down, and the error carries the tail of the
// responsible server's log.
func (c *Cluster) Start() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.start()
}

func (c *Cluster) start() error {
	if c.started {
		return ErrClusterAlreadyStarted
	}

	if c.config.tikvCount < 1 {
		return fmt.Errorf("embedded-tikv: TiKVCount must be at least 1, got %d", c.config.tikvCount)
	}

	// Reclaim anything left behind by a previous run that was killed before it could stop.
	// Best effort: a cluster that cannot sweep is still perfectly able to start.
	if c.config.reapOrphans {
		_, _ = CleanOrphans(c.config)
	}

	// Downloading is deliberately outside StartTimeout: a cold cache pulls hundreds of
	// megabytes, and that should not be mistaken for a cluster that failed to come up.
	installed, err := resolveBinaries(c.config)
	if err != nil {
		return err
	}

	warnMissingWatchdog(c.config.logger)

	c.binaries = installed

	var lastErr error

	for attempt := 0; attempt < startAttempts; attempt++ {
		lastErr = c.startOnce()
		if lastErr == nil {
			c.started = true

			return nil
		}

		c.teardown()

		if !isPortConflict(lastErr) {
			return lastErr
		}
	}

	return fmt.Errorf("embedded-tikv: cluster did not start after %d attempts: %w", startAttempts, lastErr)
}

func (c *Cluster) startOnce() error {
	// The record is claimed first: its id names the data directory, so a directory can never
	// exist without a lock that says whether its owner is still alive.
	record, err := openRecord(c.config)
	if err != nil {
		return err
	}

	c.record = record

	if err := c.prepareDataPath(record.id); err != nil {
		return err
	}

	if err := record.setDataPath(c.dataPath, c.ownsDataPath); err != nil {
		return err
	}

	// Two ports for PD (peer, client) and two per TiKV (service, status).
	ports, err := freePorts(c.config.host, 2+2*c.config.tikvCount)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.config.startTimeout)
	defer cancel()

	if err := c.startPD(ports[0], ports[1]); err != nil {
		return err
	}

	if err := waitPDReady(ctx, c.client, c.pd.clientURL(), c.aliveGuard); err != nil {
		return c.startupError("pd did not become ready", err)
	}

	for i := 0; i < c.config.tikvCount; i++ {
		if err := c.startTiKV(i, ports[2+2*i], ports[3+2*i]); err != nil {
			return err
		}
	}

	if err := waitStoresUp(ctx, c.client, c.pd.clientURL(), c.config.tikvCount, c.aliveGuard); err != nil {
		return c.startupError("tikv stores did not come up", err)
	}

	return nil
}

func (c *Cluster) startPD(peerPort, clientPort int) error {
	dir := filepath.Join(c.dataPath, "pd")

	pd := &pdInstance{
		name:       "pd-0",
		host:       c.config.host,
		dir:        dir,
		configPath: filepath.Join(dir, "pd.toml"),
		logPath:    filepath.Join(c.dataPath, "pd.log"),
		peerPort:   peerPort,
		clientPort: clientPort,
	}

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("embedded-tikv: unable to create %s: %w", dir, err)
	}

	settings := mergeConfig(defaultPDConfig(c.config.tikvCount), c.config.pdConfig)
	if err := writeConfigFile(pd.configPath, settings); err != nil {
		return err
	}

	proc, err := startProcess(pd.name, c.binaries.pd, pd.args(), dir, pd.logPath, c.config.logger, c.watchdogCleanupPaths())
	if err != nil {
		return err
	}

	pd.proc = proc
	c.pd = pd

	return c.record.addServer(pd.name, proc.cmd.Process.Pid, c.binaries.pd)
}

func (c *Cluster) startTiKV(index, port, statusPort int) error {
	name := fmt.Sprintf("tikv-%d", index)
	dir := filepath.Join(c.dataPath, name)

	tikv := &tikvInstance{
		name:       name,
		host:       c.config.host,
		dir:        dir,
		configPath: filepath.Join(dir, "tikv.toml"),
		logPath:    filepath.Join(c.dataPath, name+".log"),
		port:       port,
		statusPort: statusPort,
	}

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("embedded-tikv: unable to create %s: %w", dir, err)
	}

	settings := mergeConfig(defaultTiKVConfig(), c.config.tikvConfig)
	if err := writeConfigFile(tikv.configPath, settings); err != nil {
		return err
	}

	proc, err := startProcess(tikv.name, c.binaries.tikv, tikv.args([]string{c.pd.clientURL()}), dir, tikv.logPath, c.config.logger, c.watchdogCleanupPaths())
	if err != nil {
		return err
	}

	tikv.proc = proc
	c.tikvs = append(c.tikvs, tikv)

	return c.record.addServer(tikv.name, proc.cmd.Process.Pid, c.binaries.tikv)
}

// watchdogCleanupPaths lists what a server's watchdog should remove if this process dies: the
// temporary data directory, where this library created it, and the registry record, so that a
// killed run leaves nothing at all rather than two small files for a later sweep.
//
// A caller-supplied DataPath belongs to the caller and is never removed.
func (c *Cluster) watchdogCleanupPaths() watchdogCleanup {
	cleanup := watchdogCleanup{}

	if c.ownsDataPath {
		cleanup.dataPath = c.dataPath
	}

	if c.record != nil {
		cleanup.recordPath = c.record.path
		cleanup.lockPath = lockPathFor(c.record.path)
	}

	return cleanup
}

// aliveGuard reports an error as soon as any server exits, so readiness polling fails fast with
// the real reason rather than running out the clock.
func (c *Cluster) aliveGuard() error {
	for _, proc := range c.processes() {
		if proc.exited() {
			return fmt.Errorf("%s exited during startup: %v\n--- %s output ---\n%s", proc.name, proc.waitErr, proc.name, proc.logTail())
		}
	}

	return nil
}

func (c *Cluster) startupError(what string, cause error) error {
	// A guard failure already carries the server's output; a timeout does not, so attach the
	// most likely culprit's log.
	if strings.Contains(cause.Error(), "--- ") {
		return fmt.Errorf("embedded-tikv: %s: %w", what, cause)
	}

	tail := ""
	if procs := c.processes(); len(procs) > 0 {
		last := procs[len(procs)-1]
		tail = fmt.Sprintf("\n--- %s output ---\n%s", last.name, last.logTail())
	}

	return fmt.Errorf("embedded-tikv: %s: %w%s", what, cause, tail)
}

func (c *Cluster) processes() []*process {
	procs := make([]*process, 0, 1+len(c.tikvs))

	if c.pd != nil && c.pd.proc != nil {
		procs = append(procs, c.pd.proc)
	}

	for _, tikv := range c.tikvs {
		if tikv.proc != nil {
			procs = append(procs, tikv.proc)
		}
	}

	return procs
}

// Stop shuts the cluster down and, unless DataPath was set explicitly, removes its data
// directory.
func (c *Cluster) Stop() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.started {
		return ErrClusterNotStarted
	}

	err := c.teardown()
	c.started = false

	// Sweeping here costs microseconds and reclaims anything abandoned by another process
	// while this cluster was running. It cannot help the process that died — that is what
	// the sweep on Start is for — but it shortens how long somebody else's orphan survives.
	if c.config.reapOrphans {
		_, _ = CleanOrphans(c.config)
	}

	return err
}

// teardown stops every server and clears per-attempt state. It is used both by Stop and to
// clean up after a failed start, so it must tolerate partially constructed clusters.
func (c *Cluster) teardown() error {
	var firstErr error

	// TiKV first: shutting stores down before PD avoids a burst of failed heartbeats. The
	// stores are independent, so they are stopped in parallel rather than paying each exit
	// in turn.
	var (
		stopping sync.WaitGroup
		stopMu   sync.Mutex
	)

	for _, tikv := range c.tikvs {
		stopping.Add(1)

		go func(inst *tikvInstance) {
			defer stopping.Done()

			if err := inst.proc.stop(shutdownTimeout); err != nil {
				stopMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				stopMu.Unlock()
			}
		}(tikv)
	}

	stopping.Wait()

	if c.pd != nil {
		if err := c.pd.proc.stop(shutdownTimeout); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	c.tikvs = nil
	c.pd = nil

	if c.ownsDataPath && c.dataPath != "" {
		if err := os.RemoveAll(c.dataPath); err != nil && firstErr == nil {
			firstErr = err
		}

		c.dataPath = ""
	}

	// Released last: while this record exists and its lock is held, another process must treat
	// the cluster as live and leave it alone.
	//
	// If teardown could not confirm every server was gone, the record is kept rather than
	// deleted — it is the only handle a later sweep would have on the survivor.
	if firstErr != nil {
		c.record.abandon()
	} else {
		c.record.close()
	}

	c.record = nil

	return firstErr
}

// prepareDataPath creates the data directory, discarding any state left by a previous attempt
// so a retry never inherits a half-bootstrapped PD.
func (c *Cluster) prepareDataPath(id string) error {
	if c.dataPath == "" {
		if c.config.dataPath != "" {
			c.dataPath = c.config.dataPath
			c.ownsDataPath = false
		} else {
			// Named after the record id rather than randomly, so a sweep can pair the two
			// even when the record itself is gone.
			c.dataPath = defaultDataDir(id)
			c.ownsDataPath = true
		}
	}

	if err := os.MkdirAll(c.dataPath, 0o755); err != nil {
		return fmt.Errorf("embedded-tikv: unable to create data directory %s: %w", c.dataPath, err)
	}

	names := []string{"pd"}
	for i := 0; i < c.config.tikvCount; i++ {
		names = append(names, fmt.Sprintf("tikv-%d", i))
	}

	for _, name := range names {
		if err := os.RemoveAll(filepath.Join(c.dataPath, name)); err != nil {
			return fmt.Errorf("embedded-tikv: unable to clear %s: %w", name, err)
		}
	}

	return nil
}

func writeConfigFile(path string, settings map[string]any) error {
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("embedded-tikv: unable to write config %s: %w", path, err)
	}

	defer file.Close()

	if err := writeTOML(file, settings); err != nil {
		return fmt.Errorf("embedded-tikv: unable to write config %s: %w", path, err)
	}

	return nil
}

// isPortConflict reports whether a start failure looks like a lost race for a port, which is
// worth retrying, as opposed to a real misconfiguration, which is not.
func isPortConflict(err error) bool {
	if err == nil {
		return false
	}

	message := strings.ToLower(err.Error())

	return strings.Contains(message, "address already in use") ||
		strings.Contains(message, "address in use") ||
		strings.Contains(message, "addrinuse")
}

// Endpoints returns the addresses a TiKV client connects to, in the host:port form its
// pdAddrs argument expects. Clients talk to PD rather than to the stores directly, so these are
// PD's client addresses; StoreAddrs returns the stores.
func (c *Cluster) Endpoints() []string {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.pd == nil {
		return nil
	}

	return []string{c.pd.endpoint()}
}

// ClientURLs returns the same addresses as http:// URLs, for PD's REST API.
func (c *Cluster) ClientURLs() []string {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.pd == nil {
		return nil
	}

	return []string{c.pd.clientURL()}
}

// StoreAddrs returns the service address of every tikv-server in the cluster. Clients reach
// the stores through PD and rarely need these; Endpoints is what you pass to a client.
func (c *Cluster) StoreAddrs() []string {
	c.mu.Lock()
	defer c.mu.Unlock()

	addrs := make([]string, 0, len(c.tikvs))
	for _, tikv := range c.tikvs {
		addrs = append(addrs, tikv.addr())
	}

	return addrs
}

// DataPath returns the directory holding cluster state and server logs. It is useful for
// inspecting pd.log or tikv-0.log when a test fails.
func (c *Cluster) DataPath() string {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.dataPath
}
