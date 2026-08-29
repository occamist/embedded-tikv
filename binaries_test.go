//go:build unix

package embeddedtikv

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
)

const testVersion = "v9.9.9"

// fakeMirror serves the same manifest shape as the real TiUP mirror, so the resolution and
// download paths are exercised without pulling hundreds of megabytes.
type fakeMirror struct {
	server   *httptest.Server
	requests atomic.Int64
	corrupt  bool
}

func newFakeMirror(t *testing.T) *fakeMirror {
	t.Helper()

	platformKey, slug, err := platformFor(runtime.GOOS, runtime.GOARCH)
	if err != nil {
		t.Skipf("unsupported test platform: %v", err)
	}

	mirror := &fakeMirror{}

	archives := map[string][]byte{}
	hashes := map[string]string{}

	for component, binary := range map[string]string{componentPD: binaryPD, componentTiKV: binaryTiKV} {
		archive := buildTarGz(t, binary, "#!/bin/sh\necho "+binary+"\n")
		archives[component] = archive

		sum := sha256.Sum256(archive)
		hashes[component] = hex.EncodeToString(sum[:])
	}

	mux := http.NewServeMux()

	mux.HandleFunc("/snapshot.json", func(w http.ResponseWriter, _ *http.Request) {
		mirror.requests.Add(1)
		writeJSON(w, map[string]any{"signed": map[string]any{"meta": map[string]any{
			"/pd.json":   map[string]any{"version": 1},
			"/tikv.json": map[string]any{"version": 1},
		}}})
	})

	for component, binary := range map[string]string{componentPD: binaryPD, componentTiKV: binaryTiKV} {
		archivePath := fmt.Sprintf("/%s-%s-%s.tar.gz", component, testVersion, slug)

		mux.HandleFunc("/1."+component+".json", func(w http.ResponseWriter, _ *http.Request) {
			mirror.requests.Add(1)
			writeJSON(w, map[string]any{"signed": map[string]any{"platforms": map[string]any{
				platformKey: map[string]any{
					testVersion: map[string]any{
						"entry":  binary,
						"url":    archivePath,
						"length": len(archives[component]),
						"hashes": map[string]any{"sha256": hashes[component]},
					},
					"v1.0.0": map[string]any{"entry": binary, "url": archivePath},
					"v1.2.0": map[string]any{"entry": binary, "url": archivePath},
				},
			}}})
		})

		mux.HandleFunc(archivePath, func(w http.ResponseWriter, _ *http.Request) {
			mirror.requests.Add(1)

			body := archives[component]
			if mirror.corrupt {
				body = append(append([]byte{}, body...), 'x')
			}

			w.Write(body)
		})
	}

	mirror.server = httptest.NewServer(mux)
	t.Cleanup(mirror.server.Close)

	return mirror
}

func writeJSON(w http.ResponseWriter, value any) {
	json.NewEncoder(w).Encode(value)
}

func buildTarGz(t *testing.T, name, content string) []byte {
	t.Helper()

	var buffer bytes.Buffer

	compressor := gzip.NewWriter(&buffer)
	archive := tar.NewWriter(compressor)

	if err := archive.WriteHeader(&tar.Header{
		Name:     name,
		Mode:     0o755,
		Size:     int64(len(content)),
		Typeflag: tar.TypeReg,
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := archive.Write([]byte(content)); err != nil {
		t.Fatal(err)
	}

	if err := archive.Close(); err != nil {
		t.Fatal(err)
	}

	if err := compressor.Close(); err != nil {
		t.Fatal(err)
	}

	return buffer.Bytes()
}

func testConfig(t *testing.T, mirror *fakeMirror) Config {
	t.Helper()

	// Never let a unit test read or write the developer's real binary cache.
	return DefaultConfig().
		Version(TiKVVersion(testVersion)).
		CachePath(t.TempDir()).
		MirrorURL(mirror.server.URL)
}

func TestResolveBinariesDownloadsAndCaches(t *testing.T) {
	t.Setenv(BinariesPathEnv, "")

	mirror := newFakeMirror(t)
	config := testConfig(t, mirror)

	installed, err := resolveBinaries(config)
	if err != nil {
		t.Fatal(err)
	}

	for _, binary := range []string{installed.pd, installed.tikv} {
		if !isExecutable(binary) {
			t.Fatalf("%s was not installed as an executable", binary)
		}
	}

	content, err := os.ReadFile(installed.tikv)
	if err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(string(content), binaryTiKV) {
		t.Errorf("extracted the wrong file: %q", content)
	}

	// A second resolve must be served entirely from the cache: a cold start pulls hundreds of
	// megabytes, so re-downloading per test run would make the library unusable.
	before := mirror.requests.Load()

	if _, err := resolveBinaries(config); err != nil {
		t.Fatal(err)
	}

	if after := mirror.requests.Load(); after != before {
		t.Errorf("cached resolve made %d extra mirror requests, want 0", after-before)
	}
}

func TestResolveBinariesRejectsCorruptDownload(t *testing.T) {
	t.Setenv(BinariesPathEnv, "")

	mirror := newFakeMirror(t)
	mirror.corrupt = true

	_, err := resolveBinaries(testConfig(t, mirror))
	if err == nil {
		t.Fatal("expected a checksum failure")
	}

	if !strings.Contains(err.Error(), "checksum mismatch") {
		t.Errorf("error %q does not report a checksum mismatch", err)
	}
}

func TestResolveBinariesUnknownVersionListsAlternatives(t *testing.T) {
	t.Setenv(BinariesPathEnv, "")

	mirror := newFakeMirror(t)
	config := testConfig(t, mirror).Version("v0.0.1")

	_, err := resolveBinaries(config)
	if err == nil {
		t.Fatal("expected an error for an unpublished version")
	}

	// A bare "not found" leaves the user guessing; the real versions are right there.
	if !strings.Contains(err.Error(), "v1.2.0") {
		t.Errorf("error %q does not list available versions", err)
	}
}

func TestResolveBinariesPrefersSuppliedDirectory(t *testing.T) {
	directory := t.TempDir()

	for _, name := range []string{binaryPD, binaryTiKV} {
		if err := os.WriteFile(filepath.Join(directory, name), []byte("#!/bin/sh\n"), 0o755); err != nil {
			t.Fatal(err)
		}
	}

	// No mirror is configured at all, so any attempt to download would fail loudly.
	installed, err := resolveBinaries(DefaultConfig().BinariesPath(directory))
	if err != nil {
		t.Fatal(err)
	}

	if installed.tikv != filepath.Join(directory, binaryTiKV) {
		t.Errorf("resolved %s, want the supplied directory", installed.tikv)
	}
}

func TestResolveBinariesHonoursEnvironmentOverride(t *testing.T) {
	directory := t.TempDir()

	for _, name := range []string{binaryPD, binaryTiKV} {
		if err := os.WriteFile(filepath.Join(directory, name), []byte("#!/bin/sh\n"), 0o755); err != nil {
			t.Fatal(err)
		}
	}

	t.Setenv(BinariesPathEnv, directory)

	installed, err := resolveBinaries(DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}

	if installed.pd != filepath.Join(directory, binaryPD) {
		t.Errorf("resolved %s, want the directory from %s", installed.pd, BinariesPathEnv)
	}
}

func TestResolveBinariesReportsIncompleteSuppliedDirectory(t *testing.T) {
	directory := t.TempDir()

	if err := os.WriteFile(filepath.Join(directory, binaryPD), []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	_, err := resolveBinaries(DefaultConfig().BinariesPath(directory))
	if err == nil {
		t.Fatal("expected an error when tikv-server is missing")
	}

	if !strings.Contains(err.Error(), binaryTiKV) {
		t.Errorf("error %q does not name the missing binary", err)
	}
}

func TestPlatformForSupportedHosts(t *testing.T) {
	for _, test := range []struct {
		goos, goarch, key, slug string
	}{
		{"linux", "amd64", "linux/amd64", "linux-amd64"},
		{"linux", "arm64", "linux/arm64", "linux-arm64"},
		{"darwin", "amd64", "darwin/amd64", "darwin-amd64"},
		{"darwin", "arm64", "darwin/arm64", "darwin-arm64"},
	} {
		key, slug, err := platformFor(test.goos, test.goarch)
		if err != nil {
			t.Fatalf("platformFor(%s, %s) returned %v", test.goos, test.goarch, err)
		}

		if key != test.key || slug != test.slug {
			t.Errorf("platformFor(%s, %s) = %q, %q; want %q, %q", test.goos, test.goarch, key, slug, test.key, test.slug)
		}
	}
}

// TestPlatformForRejectsUnpublishedHosts covers the Unixes that compile this package but have
// no published TiKV build. Windows is not among them: the package is //go:build unix and does
// not compile there at all.
func TestPlatformForRejectsUnpublishedHosts(t *testing.T) {
	for _, goos := range []string{"freebsd", "openbsd", "illumos", "solaris"} {
		if _, _, err := platformFor(goos, "amd64"); err == nil {
			t.Errorf("expected %s to be rejected: TiKV publishes no build for it", goos)
		}
	}

	if _, _, err := platformFor("linux", "riscv64"); err == nil {
		t.Error("expected riscv64 to be rejected")
	}
}
