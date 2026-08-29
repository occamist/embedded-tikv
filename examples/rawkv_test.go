package examples_test

import (
	"bytes"
	"context"
	"testing"

	embeddedtikv "github.com/occamist/embedded-tikv"
	"github.com/tikv/client-go/v2/rawkv"
)

// TestRawKVRoundTrip is the end-to-end proof that Start returns a cluster which is genuinely
// writable, not merely one whose processes are running.
func TestRawKVRoundTrip(t *testing.T) {
	cluster := embeddedtikv.New()

	if err := cluster.Start(); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := cluster.Stop(); err != nil {
			t.Error(err)
		}
	})

	ctx := context.Background()

	client, err := rawkv.NewClientWithOpts(ctx, cluster.Endpoints())
	if err != nil {
		t.Fatal(err)
	}

	defer client.Close()

	key, value := []byte("greeting"), []byte("hello from a test")

	if err := client.Put(ctx, key, value); err != nil {
		t.Fatal(err)
	}

	got, err := client.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(got, value) {
		t.Fatalf("Get returned %q, want %q", got, value)
	}

	if err := client.Delete(ctx, key); err != nil {
		t.Fatal(err)
	}

	got, err = client.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}

	if got != nil {
		t.Fatalf("Get after Delete returned %q, want nil", got)
	}
}
