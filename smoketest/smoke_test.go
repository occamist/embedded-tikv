// Package smoke_test is the minimal end-to-end check that the whole path works, and is what CI
// runs on every supported OS and architecture.
//
// It is a separate module so it can be run against a published version of the library, and it
// deliberately does the smallest possible thing: prove that the downloaded binaries execute on
// this platform and that a write reaches storage.
package smoke_test

import (
	"bytes"
	"context"
	"testing"

	embeddedtikv "github.com/occamist/embedded-tikv"
	"github.com/tikv/client-go/v2/rawkv"
)

func TestClusterStartsAndServesOnThisPlatform(t *testing.T) {
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

	key, value := []byte("platform"), []byte("ok")

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
}
