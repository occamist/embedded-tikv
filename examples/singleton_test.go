package examples_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"testing"

	embeddedtikv "github.com/occamist/embedded-tikv"
	"github.com/tikv/client-go/v2/rawkv"
)

// sharedCluster demonstrates the second supported usage pattern: one cluster started in
// TestMain and reused by every test that wants it.
//
// Starting a cluster takes a couple of seconds, so a suite with many tests is better served by
// sharing one and keeping tests isolated by key prefix, as TestFirstTenant and TestSecondTenant
// do below. It is named rather than called "cluster" because the per-test examples in this
// package declare their own.
var sharedCluster *embeddedtikv.Cluster

// TestMain governs every test in the package, so the shared cluster is started even for the
// per-test examples that build their own. That is the cost of the pattern, not a mistake.
func TestMain(m *testing.M) {
	sharedCluster = embeddedtikv.New()

	if err := sharedCluster.Start(); err != nil {
		fmt.Fprintln(os.Stderr, "unable to start TiKV:", err)
		os.Exit(1)
	}

	code := m.Run()

	if err := sharedCluster.Stop(); err != nil {
		fmt.Fprintln(os.Stderr, "unable to stop TiKV:", err)
	}

	os.Exit(code)
}

func newClient(t *testing.T) *rawkv.Client {
	t.Helper()

	client, err := rawkv.NewClientWithOpts(context.Background(), sharedCluster.Endpoints())
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { client.Close() })

	return client
}

func TestFirstTenant(t *testing.T) {
	assertRoundTrip(t, []byte("tenant-1/key"), []byte("first"))
}

func TestSecondTenant(t *testing.T) {
	assertRoundTrip(t, []byte("tenant-2/key"), []byte("second"))
}

func assertRoundTrip(t *testing.T, key, value []byte) {
	t.Helper()

	ctx := context.Background()
	client := newClient(t)

	if err := client.Put(ctx, key, value); err != nil {
		t.Fatal(err)
	}

	got, err := client.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(got, value) {
		t.Fatalf("Get(%q) returned %q, want %q", key, got, value)
	}
}
