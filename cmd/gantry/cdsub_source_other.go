//go:build !linux

package main

import (
	"log/slog"

	"github.com/gantry/gantry/internal/cdsub"
	"github.com/gantry/gantry/internal/config"
)

// newCdsubSource on non-linux always returns NoOpSource — the
// containerd Go client only links cleanly on linux, and gantry is
// only meaningful as a kubelet-adjacent DaemonSet anyway. Non-linux
// builds are dev/test only.
func newCdsubSource(_ *config.Config, logger *slog.Logger) cdsub.ImageSource {
	logger.Info("cdsub: containerd integration unavailable on this platform — using NoOpSource")
	return cdsub.NoOpSource{}
}
