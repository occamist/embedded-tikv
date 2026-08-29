//go:build unix

package embeddedtikv

import "syscall"

// sysProcAttr puts each server in its own process group, so it can be signalled as a unit and
// so the watchdog can take the group down with a single kill.
//
// Pdeathsig is deliberately not used. It exists only on Linux, so relying on it would mean the
// platform most tests run on used a different mechanism from macOS, leaving the portable
// watchdog unexercised. It is also unsound: the kernel delivers it when the *creating thread*
// exits, which the Go runtime may do while the process is perfectly healthy
// (go.dev/issue/27505, closed in 2026 by documenting the hazard rather than removing it) — a
// server killed at random in the middle of a test.
//
// Parent death is handled by the watchdog in watchdog.go, and by the registry sweep where no
// watchdog could be installed.
func sysProcAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Setpgid: true}
}
