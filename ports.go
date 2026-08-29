//go:build unix

package embeddedtikv

import (
	"fmt"
	"net"
	"sync"
	"time"
)

// portReservationTTL is how long a port handed out by freePort is withheld from subsequent
// callers in this process. It only needs to outlive the gap between allocating a port and the
// child process binding it.
const portReservationTTL = time.Minute

var (
	portMu       sync.Mutex
	portReserved = map[int]time.Time{}
)

// freePort asks the kernel for an unused port on host, then closes the listener so the child
// process can bind it.
//
// The window between closing and binding is unavoidable — neither tikv-server nor pd-server
// accepts a pre-opened socket — so the port is recorded in a short-lived reservation table to
// stop concurrent clusters in the same test binary from being handed the same number. Races
// against other processes still exist and are handled a level up, by retrying Start.
func freePort(host string) (int, error) {
	for range 100 {
		port, err := listenForPort(host)
		if err != nil {
			return 0, err
		}

		if reservePort(port) {
			return port, nil
		}
	}

	return 0, fmt.Errorf("embedded-tikv: could not find an unreserved free port on %s", host)
}

// freePorts allocates n distinct ports, all held under the same reservation window.
func freePorts(host string, n int) ([]int, error) {
	ports := make([]int, 0, n)

	for range n {
		port, err := freePort(host)
		if err != nil {
			return nil, err
		}
		ports = append(ports, port)
	}

	return ports, nil
}

func listenForPort(host string) (int, error) {
	listener, err := net.Listen("tcp", net.JoinHostPort(host, "0"))
	if err != nil {
		return 0, fmt.Errorf("embedded-tikv: unable to allocate a port on %s: %w", host, err)
	}

	defer listener.Close()

	addr, ok := listener.Addr().(*net.TCPAddr)
	if !ok {
		return 0, fmt.Errorf("embedded-tikv: unexpected listener address type %T", listener.Addr())
	}

	return addr.Port, nil
}

// reservePort records port as in use, reporting false when a live reservation already exists.
func reservePort(port int) bool {
	portMu.Lock()
	defer portMu.Unlock()

	now := time.Now()

	for reserved, at := range portReserved {
		if now.Sub(at) > portReservationTTL {
			delete(portReserved, reserved)
		}
	}

	if _, taken := portReserved[port]; taken {
		return false
	}

	portReserved[port] = now

	return true
}
