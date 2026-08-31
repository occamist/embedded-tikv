//go:build unix

package embeddedtikv

import (
	"io"
	"time"
)

// TiKVVersion is the release tag shared by the tikv-server and pd-server binaries.
type TiKVVersion string

// Predefined TiKV versions, one per upstream long-term-support (LTS) line.
// https://docs.pingcap.com/tidb/stable/ if LTS versions get patch updates, do update below
// and double-check against `curl -s "https://api.github.com/repos/tikv/tikv/releases?per_page=100" | grep -o '"tag_name": "[^"]*"' | sort -V`
const (
	V8_5 = TiKVVersion("v8.5.8")
	V8_1 = TiKVVersion("v8.1.2")
	V7_5 = TiKVVersion("v7.5.7")
	V7_1 = TiKVVersion("v7.1.6")
	V6_5 = TiKVVersion("v6.5.12")
)

// DefaultMirrorURL is the TiUP mirror that publishes official TiKV and PD builds.
const DefaultMirrorURL = "https://tiup-mirrors.pingcap.com"

// BinariesPathEnv names an environment variable holding a directory that already contains
// tikv-server and pd-server. Setting it suppresses all downloading, which is what CI images and
// air-gapped machines want.
const BinariesPathEnv = "EMBEDDED_TIKV_BINARIES"

// Config holds the runtime configuration for one embedded cluster. It is an immutable builder:
// every method returns a copy, so a Config value is safe to keep as a shared template.
type Config struct {
	version      TiKVVersion
	tikvCount    int
	host         string
	dataPath     string
	binariesPath string
	cachePath    string
	mirrorURL    string
	startTimeout time.Duration
	logger       io.Writer
	pdConfig     map[string]any
	tikvConfig   map[string]any
	reapOrphans  bool
}

// DefaultConfig returns the configuration used when New is called with no arguments:
// one PD and one TiKV of the latest supported LTS release, on 127.0.0.1, with random ports,
// a temporary data directory removed on Stop, and binaries cached under ~/.embedded-tikv.
func DefaultConfig() Config {
	return Config{
		version:      V8_5,
		tikvCount:    1,
		host:         "127.0.0.1",
		mirrorURL:    DefaultMirrorURL,
		startTimeout: 120 * time.Second,
		reapOrphans:  true,
	}
}

// Version sets the release tag used for both tikv-server and pd-server.
func (c Config) Version(version TiKVVersion) Config {
	c.version = version
	return c
}

// TiKVCount sets how many tikv-server processes to run. There is always exactly one PD.
//
// A count below 3 leaves PD's replication.max-replicas at 1, since a smaller cluster can never
// reach quorum on three replicas and its regions would stay permanently unavailable.
func (c Config) TiKVCount(count int) Config {
	c.tikvCount = count
	return c
}

// Host sets the address the cluster listens on. Defaults to 127.0.0.1.
func (c Config) Host(host string) Config {
	c.host = host
	return c
}

// DataPath sets the directory holding PD and TiKV state plus process logs.
//
// When set, the directory survives Stop, which is useful for inspecting a failure. It is not
// reused across runs: every Start clears the pd/ and tikv-N/ subdirectories first, because PD
// records its peer URL in its data directory and each start allocates fresh ports.
//
// When unset, a temporary directory is created and removed on Stop.
func (c Config) DataPath(path string) Config {
	c.dataPath = path
	return c
}

// BinariesPath points at a directory already containing tikv-server and pd-server,
// bypassing the download entirely. Equivalent to the EMBEDDED_TIKV_BINARIES environment
// variable, which this takes precedence over.
func (c Config) BinariesPath(path string) Config {
	c.binariesPath = path
	return c
}

// CachePath sets where downloaded binaries are kept between runs.
// Defaults to ~/.embedded-tikv, shared by every project on the machine.
func (c Config) CachePath(path string) Config {
	c.cachePath = path
	return c
}

// MirrorURL overrides the TiUP mirror binaries are fetched from, for use with an internal
// mirror or a test double.
func (c Config) MirrorURL(url string) Config {
	c.mirrorURL = url
	return c
}

// StartTimeout bounds how long Start waits for the cluster to become writable.
// It excludes the time spent downloading binaries on a cold cache.
func (c Config) StartTimeout(timeout time.Duration) Config {
	c.startTimeout = timeout
	return c
}

// Logger receives a copy of everything PD and TiKV write to stdout and stderr.
// Output always goes to log files under the data directory regardless.
func (c Config) Logger(logger io.Writer) Config {
	c.logger = logger
	return c
}

// PDConfig merges extra pd-server settings over the defaults, keyed by dotted TOML path,
// for example {"replication.max-replicas": 3}.
func (c Config) PDConfig(config map[string]any) Config {
	c.pdConfig = config
	return c
}

// ReapOrphans controls whether Start first reclaims clusters abandoned by processes that died
// without calling Stop. Enabled by default. See CleanOrphans for the safety rules; disable it
// only if you want to sweep explicitly instead.
func (c Config) ReapOrphans(reap bool) Config {
	c.reapOrphans = reap
	return c
}

// TiKVConfig merges extra tikv-server settings over the defaults, keyed by dotted TOML path,
// for example {"storage.api-version": 2}.
func (c Config) TiKVConfig(config map[string]any) Config {
	c.tikvConfig = config
	return c
}
