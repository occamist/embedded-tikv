//go:build unix

package embeddedtikv

import (
	"archive/tar"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"time"
)

const (
	componentPD   = "pd"
	componentTiKV = "tikv"

	binaryPD   = "pd-server"
	binaryTiKV = "tikv-server"
)

// binaries locates the two executables a cluster runs.
type binaries struct {
	pd   string
	tikv string
}

// resolveBinaries returns paths to pd-server and tikv-server, downloading them if needed.
//
// Resolution order: an explicit BinariesPath, then EMBEDDED_TIKV_BINARIES, then the version
// cache under CachePath. Only the last can trigger a download.
func resolveBinaries(cfg Config) (binaries, error) {
	if dir := suppliedBinariesDir(cfg); dir != "" {
		return suppliedBinaries(dir)
	}

	platformKey, slug, err := platformFor(runtime.GOOS, runtime.GOARCH)
	if err != nil {
		return binaries{}, err
	}

	cacheDir, err := cacheDirectory(cfg)
	if err != nil {
		return binaries{}, err
	}

	binDir := filepath.Join(cacheDir, "bin", fmt.Sprintf("%s-%s", cfg.version, slug))

	found := binaries{
		pd:   filepath.Join(binDir, binaryPD),
		tikv: filepath.Join(binDir, binaryTiKV),
	}

	if isExecutable(found.pd) && isExecutable(found.tikv) {
		return found, nil
	}

	if err := os.MkdirAll(binDir, 0o755); err != nil {
		return binaries{}, fmt.Errorf("embedded-tikv: unable to create binary cache %s: %w", binDir, err)
	}

	err = withFileLock(binDir+".lock", func() error {
		mirror := newMirror(cfg.mirrorURL)

		for _, want := range []struct {
			component string
			path      string
		}{
			{componentPD, found.pd},
			{componentTiKV, found.tikv},
		} {
			if isExecutable(want.path) {
				continue
			}

			entry, err := mirror.resolve(want.component, string(cfg.version), platformKey)
			if err != nil {
				return err
			}

			if err := mirror.download(entry, want.path); err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		return binaries{}, err
	}

	return found, nil
}

func suppliedBinariesDir(cfg Config) string {
	if cfg.binariesPath != "" {
		return cfg.binariesPath
	}

	return os.Getenv(BinariesPathEnv)
}

func suppliedBinaries(dir string) (binaries, error) {
	supplied := binaries{
		pd:   filepath.Join(dir, binaryPD),
		tikv: filepath.Join(dir, binaryTiKV),
	}

	for _, binary := range []string{supplied.pd, supplied.tikv} {
		if !isExecutable(binary) {
			return binaries{}, fmt.Errorf("embedded-tikv: %s is missing or not executable; a directory given via BinariesPath or %s must contain both %s and %s", binary, BinariesPathEnv, binaryPD, binaryTiKV)
		}
	}

	return supplied, nil
}

func cacheDirectory(cfg Config) (string, error) {
	if cfg.cachePath != "" {
		return cfg.cachePath, nil
	}

	home, err := os.UserHomeDir()
	if err != nil {
		return ".embedded-tikv", nil
	}

	return filepath.Join(home, ".embedded-tikv"), nil
}

func isExecutable(path string) bool {
	info, err := os.Stat(path)

	return err == nil && !info.IsDir() && info.Mode()&0o111 != 0
}

// platformFor maps a Go OS/arch pair onto a TiUP mirror platform.
//
// TiKV and PD publish binaries for linux and darwin on amd64 and arm64, and nothing else.
// Rejecting an unsupported host here keeps the failure legible, rather than surfacing as a 404
// partway through a download.
//
// Windows needs no special case: the package is //go:build unix and does not compile there,
// because TiKV publishes no Windows binaries at all (tikv/tikv#9103). What this still catches
// is the other Unixes — FreeBSD, OpenBSD, illumos and so on — which do compile but have no
// published build.
func platformFor(goos, goarch string) (key, slug string, err error) {
	if goos != "linux" && goos != "darwin" {
		return "", "", fmt.Errorf("embedded-tikv: unsupported operating system %q, TiKV publishes binaries for linux and darwin only", goos)
	}

	if goarch != "amd64" && goarch != "arm64" {
		return "", "", fmt.Errorf("embedded-tikv: unsupported architecture %q, TiKV publishes binaries for amd64 and arm64 only", goarch)
	}

	return goos + "/" + goarch, goos + "-" + goarch, nil
}

// mirrorEntry is one platform/version record from a TiUP component manifest.
type mirrorEntry struct {
	Entry  string `json:"entry"`
	URL    string `json:"url"`
	Length int64  `json:"length"`
	Yanked bool   `json:"yanked"`
	Hashes struct {
		SHA256 string `json:"sha256"`
	} `json:"hashes"`
}

type snapshotManifest struct {
	Signed struct {
		Meta map[string]struct {
			Version int `json:"version"`
		} `json:"meta"`
	} `json:"signed"`
}

type componentManifest struct {
	Signed struct {
		Platforms map[string]map[string]mirrorEntry `json:"platforms"`
	} `json:"signed"`
}

// mirror reads the TiUP mirror's manifests to turn a component and version into a download URL
// and a SHA-256.
//
// The manifests are TUF-signed; we read the payload but do not verify signatures, relying on
// HTTPS for authenticity and the manifest hash for integrity. Reimplementing TUF is out of
// scope for a test helper — use BinariesPath if your threat model needs more.
type mirror struct {
	baseURL  string
	client   *http.Client
	fetcher  *http.Client
	snapshot *snapshotManifest
}

func newMirror(baseURL string) *mirror {
	return &mirror{
		baseURL: strings.TrimSuffix(baseURL, "/"),
		client:  &http.Client{Timeout: 30 * time.Second},
		// Downloads run into the hundreds of megabytes, so they get no overall deadline —
		// only per-stage timeouts from the default transport.
		fetcher: &http.Client{},
	}
}

func (m *mirror) resolve(component, version, platformKey string) (mirrorEntry, error) {
	if m.snapshot == nil {
		var snapshot snapshotManifest
		if err := m.getJSON("/snapshot.json", &snapshot); err != nil {
			return mirrorEntry{}, err
		}

		m.snapshot = &snapshot
	}

	meta, ok := m.snapshot.Signed.Meta["/"+component+".json"]
	if !ok {
		return mirrorEntry{}, fmt.Errorf("embedded-tikv: mirror %s does not publish component %q", m.baseURL, component)
	}

	var manifest componentManifest
	if err := m.getJSON(fmt.Sprintf("/%d.%s.json", meta.Version, component), &manifest); err != nil {
		return mirrorEntry{}, err
	}

	platforms, ok := manifest.Signed.Platforms[platformKey]
	if !ok {
		return mirrorEntry{}, fmt.Errorf("embedded-tikv: %s publishes no %s build; available platforms are %s", component, platformKey, strings.Join(sortedKeys(manifest.Signed.Platforms), ", "))
	}

	entry, ok := platforms[version]
	if !ok {
		return mirrorEntry{}, fmt.Errorf("embedded-tikv: %s has no version %s for %s; recent releases are %s", component, version, platformKey, strings.Join(recentVersions(platforms), ", "))
	}

	if entry.Yanked {
		return mirrorEntry{}, fmt.Errorf("embedded-tikv: %s %s has been yanked upstream, pick another version", component, version)
	}

	if entry.Entry == "" || entry.URL == "" {
		return mirrorEntry{}, fmt.Errorf("embedded-tikv: manifest entry for %s %s is incomplete", component, version)
	}

	return entry, nil
}

func (m *mirror) getJSON(path string, into any) error {
	url := m.baseURL + path

	response, err := m.client.Get(url)
	if err != nil {
		return fmt.Errorf("embedded-tikv: unable to reach mirror %s: %w", url, err)
	}

	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("embedded-tikv: mirror %s returned %s", url, response.Status)
	}

	if err := json.NewDecoder(response.Body).Decode(into); err != nil {
		return fmt.Errorf("embedded-tikv: unable to parse manifest %s: %w", url, err)
	}

	return nil
}

// download streams the component tarball, extracting the single binary it contains while
// hashing the compressed bytes, then moves the result into place atomically.
//
// Only the extracted binary is kept. Caching the archive as well, the way embedded-postgres
// does, would roughly double an already large footprint.
func (m *mirror) download(entry mirrorEntry, destination string) error {
	url := m.baseURL + entry.URL

	response, err := m.fetcher.Get(url)
	if err != nil {
		return fmt.Errorf("embedded-tikv: unable to download %s: %w", url, err)
	}

	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("embedded-tikv: downloading %s returned %s", url, response.Status)
	}

	hasher := sha256.New()
	hashed := io.TeeReader(response.Body, hasher)

	temporary, err := os.CreateTemp(filepath.Dir(destination), "."+filepath.Base(destination)+".*")
	if err != nil {
		return fmt.Errorf("embedded-tikv: unable to create temporary file: %w", err)
	}

	moved := false

	defer func() {
		temporary.Close()

		if !moved {
			os.Remove(temporary.Name())
		}
	}()

	if err := extractEntry(hashed, entry.Entry, temporary); err != nil {
		return fmt.Errorf("embedded-tikv: extracting %s from %s: %w", entry.Entry, url, err)
	}

	// The tar reader stops at the entry we wanted, so the rest of the body still has to be
	// read for the hash to cover the whole artifact.
	if _, err := io.Copy(io.Discard, hashed); err != nil {
		return fmt.Errorf("embedded-tikv: unable to finish reading %s: %w", url, err)
	}

	if actual := hex.EncodeToString(hasher.Sum(nil)); actual != entry.Hashes.SHA256 {
		return fmt.Errorf("embedded-tikv: checksum mismatch for %s: manifest says %s, download was %s", url, entry.Hashes.SHA256, actual)
	}

	if err := temporary.Chmod(0o755); err != nil {
		return fmt.Errorf("embedded-tikv: unable to make %s executable: %w", temporary.Name(), err)
	}

	if err := temporary.Close(); err != nil {
		return fmt.Errorf("embedded-tikv: unable to close %s: %w", temporary.Name(), err)
	}

	if err := os.Rename(temporary.Name(), destination); err != nil {
		return fmt.Errorf("embedded-tikv: unable to install %s: %w", destination, err)
	}

	moved = true

	return nil
}

// extractEntry copies the named file out of a gzipped tar stream. Component tarballs are flat,
// with the binary at the root, so a base-name match is enough.
func extractEntry(archive io.Reader, name string, destination io.Writer) error {
	decompressed, err := gzip.NewReader(archive)
	if err != nil {
		return err
	}

	defer decompressed.Close()

	reader := tar.NewReader(decompressed)

	for {
		header, err := reader.Next()
		if err == io.EOF {
			return fmt.Errorf("archive does not contain %s", name)
		}

		if err != nil {
			return err
		}

		if header.Typeflag != tar.TypeReg || path.Base(header.Name) != name {
			continue
		}

		if _, err := io.Copy(destination, reader); err != nil {
			return err
		}

		return nil
	}
}

func sortedKeys[V any](m map[string]V) []string {
	return slices.Sorted(maps.Keys(m))
}

// recentVersions lists the newest handful of stable releases, to make a bad Version() call
// self-correcting rather than a bare "not found".
func recentVersions(platforms map[string]mirrorEntry) []string {
	stable := make([]string, 0, len(platforms))

	for version := range platforms {
		if strings.ContainsAny(version, "-") {
			continue
		}

		stable = append(stable, version)
	}

	slices.SortFunc(stable, compareVersions)

	if len(stable) > 8 {
		stable = stable[len(stable)-8:]
	}

	return stable
}

func compareVersions(a, b string) int {
	fieldsA := strings.Split(strings.TrimPrefix(a, "v"), ".")
	fieldsB := strings.Split(strings.TrimPrefix(b, "v"), ".")

	for i := 0; i < len(fieldsA) && i < len(fieldsB); i++ {
		if fieldsA[i] != fieldsB[i] {
			if len(fieldsA[i]) != len(fieldsB[i]) {
				return len(fieldsA[i]) - len(fieldsB[i])
			}

			return strings.Compare(fieldsA[i], fieldsB[i])
		}
	}

	return len(fieldsA) - len(fieldsB)
}
