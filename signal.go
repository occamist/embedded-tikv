//go:build unix

package embeddedtikv

import "syscall"

// signalGroup delivers sig to a server's whole process group, falling back to the process
// itself if it has no group of its own.
//
// Servers are started with Setpgid, so the group holds the server and its watchdog together and
// a single call takes both down. TiKV spawns worker threads rather than child processes, but
// signalling the group is still the safer default and matches how TiUP tears playground
// instances down.
func signalGroup(pid int, sig syscall.Signal) error {
	if err := syscall.Kill(-pid, sig); err == nil {
		return nil
	}

	return syscall.Kill(pid, sig)
}
