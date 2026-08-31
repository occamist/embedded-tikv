package examples_test

import (
	"bytes"
	"context"
	"testing"

	embeddedtikv "github.com/occamist/embedded-tikv"
	"github.com/tikv/client-go/v2/txnkv"
)

func TestTxnKVCommit(t *testing.T) {
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

	client, err := txnkv.NewClient(cluster.Endpoints())
	if err != nil {
		t.Fatal(err)
	}

	defer client.Close()

	key, value := []byte("account/1"), []byte("100")

	write, err := client.Begin()
	if err != nil {
		t.Fatal(err)
	}

	if err := write.Set(key, value); err != nil {
		t.Fatal(err)
	}

	if err := write.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	read, err := client.Begin()
	if err != nil {
		t.Fatal(err)
	}

	got, err := read.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(got, value) {
		t.Fatalf("Get returned %q, want %q", got, value)
	}

	if err := read.Rollback(); err != nil {
		t.Fatal(err)
	}
}
