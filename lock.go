//go:build unix

package embeddedtikv

import (
	"fmt"
	"os"
	"syscall"
)

// fileLock is an advisory lock held on an open file descriptor.
//
// The kernel releases it when the holding process exits, however it exits. That property is
// what makes it a reliable liveness signal: a lock this process can acquire is one whose owner
// is definitively gone. Unlike checking a recorded PID, it cannot be fooled by PID reuse.
type fileLock struct {
	file *os.File
}

// acquireFileLock takes an exclusive lock on path. When block is false and another process
// holds the lock, it returns (nil, nil) rather than waiting.
//
// create must be false for callers that are probing somebody else's lock. Creating a missing
// lock file would manufacture a brand-new, trivially lockable inode and so report a live owner
// as dead — the opposite of what this is for.
func acquireFileLock(path string, block, create bool) (*fileLock, error) {
	flags := os.O_RDWR
	if create {
		flags |= os.O_CREATE
	}

	file, err := os.OpenFile(path, flags, 0o644)
	if err != nil {
		if !create && os.IsNotExist(err) {
			return nil, nil
		}

		return nil, fmt.Errorf("embedded-tikv: unable to open lock file %s: %w", path, err)
	}

	how := syscall.LOCK_EX
	if !block {
		how |= syscall.LOCK_NB
	}

	if err := syscall.Flock(int(file.Fd()), how); err != nil {
		file.Close()

		if !block {
			return nil, nil
		}

		return nil, fmt.Errorf("embedded-tikv: unable to lock %s: %w", path, err)
	}

	return &fileLock{file: file}, nil
}

func (l *fileLock) release() {
	if l == nil || l.file == nil {
		return
	}

	syscall.Flock(int(l.file.Fd()), syscall.LOCK_UN)
	l.file.Close()
	l.file = nil
}

// withFileLock runs fn while holding an exclusive lock on path.
//
// This is what stops several `go test` processes starting at once from each downloading the
// same few-hundred-megabyte tarball.
func withFileLock(path string, fn func() error) error {
	lock, err := acquireFileLock(path, true, true)
	if err != nil {
		return err
	}

	defer lock.release()

	return fn()
}
