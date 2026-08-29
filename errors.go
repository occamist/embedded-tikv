//go:build unix

package embeddedtikv

import "errors"

var (
	// ErrClusterAlreadyStarted is returned by Start when the cluster is already running.
	ErrClusterAlreadyStarted = errors.New("embedded-tikv: cluster is already started")
	// ErrClusterNotStarted is returned by Stop when there is nothing to stop.
	ErrClusterNotStarted = errors.New("embedded-tikv: cluster has not been started")
)
