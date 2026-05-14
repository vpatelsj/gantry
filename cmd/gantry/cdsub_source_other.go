//go:build !linux

package main

import (
	"log/slog"

	"github.com/gantry/gantry/internal/cdsub"
	"github.com/gantry/gantry/internal/config"
	"github.com/gantry/gantry/internal/ifaces"
)

// newCdsubSource on non-linux always returns NoOpSource — the
// containerd Go client only links cleanly on linux, and gantry is
// only meaningful as a kubelet-adjacent DaemonSet anyway. Non-linux
// builds are dev/test only.
func newCdsubSource(_ *config.Config, logger *slog.Logger) cdsub.ImageSource {
	logger.Info("cdsub: containerd integration unavailable on this platform — using NoOpSource")
	return cdsub.NoOpSource{}
}

// cdsubBlobSource on non-linux always returns nil — there is no
// containerd to back the SecondaryBlobSource with. The transfer
// endpoint falls through to its plain 404 path on cache miss.
func cdsubBlobSource(_ cdsub.ImageSource) ifaces.SecondaryBlobSource { return nil }
