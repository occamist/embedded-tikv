//go:build unix

package embeddedtikv

import (
	"net"
	"strconv"
	"testing"
)

func TestFreePortsAreDistinctAndBindable(t *testing.T) {
	ports, err := freePorts("127.0.0.1", 8)
	if err != nil {
		t.Fatal(err)
	}

	seen := map[int]bool{}

	for _, port := range ports {
		if seen[port] {
			t.Fatalf("port %d was handed out twice", port)
		}

		seen[port] = true

		listener, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
		if err != nil {
			t.Fatalf("port %d was not actually free: %v", port, err)
		}

		listener.Close()
	}
}

func TestReservePortWithholdsPortFromLaterCallers(t *testing.T) {
	port, err := freePort("127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}

	// The kernel will happily hand the same port out again once the probe listener closes;
	// the reservation table is what stops two clusters in one test binary colliding.
	if reservePort(port) {
		t.Errorf("port %d was reserved twice", port)
	}
}
