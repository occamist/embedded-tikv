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

var cluster *embeddedtikv.Cluster

func TestMain(m *testing.M) {
	cluster = embeddedtikv.New()

	if err := cluster.Start(); err != nil {
		fmt.Fprintln(os.Stderr, "unable to start TiKV:", err)
		os.Exit(1)
	}

	code := m.Run()

	if err := cluster.Stop(); err != nil {
		fmt.Fprintln(os.Stderr, "unable to stop TiKV:", err)
	}

	os.Exit(code)
}

func newClient(t *testing.T) *rawkv.Client {
	t.Helper()

	client, err := rawkv.NewClientWithOpts(context.Background(), cluster.Endpoints())
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
