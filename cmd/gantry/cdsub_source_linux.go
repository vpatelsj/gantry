//go:build linux

package main

import (
	"log/slog"

	"github.com/gantry/gantry/internal/cdsub"
	"github.com/gantry/gantry/internal/config"
	"github.com/gantry/gantry/internal/ifaces"
)

// newCdsubSource returns the production containerd-backed ImageSource
// when running on linux. If the operator cleared ContainerdSocket the
// cdsub subsystem is explicitly disabled (NoOpSource) — useful for
// dev clusters that have no containerd to talk to.
//
// Construction errors are logged and downgraded to NoOpSource so a
// missing/unreachable socket does not prevent the agent from serving
// peer fetches. Operators see the error in the structured log and
// in the absence of cdsub-namespaced metrics.
func newCdsubSource(c *config.Config, logger *slog.Logger) cdsub.ImageSource {
	if c.ContainerdSocket == "" {
		logger.Info("cdsub: containerd_socket empty — running with NoOpSource")
		return cdsub.NoOpSource{}
	}
	src, err := cdsub.NewContainerdSource(c.ContainerdSocket, c.ContainerdNamespace,
		cdsub.WithContainerdLogger(logger),
	)
	if err != nil {
		logger.Warn("cdsub: containerd source unavailable, falling back to NoOpSource",
			slog.String("socket", c.ContainerdSocket),
			slog.String("namespace", c.ContainerdNamespace),
			slog.Any("err", err),
		)
		return cdsub.NoOpSource{}
	}
	return src
}

// cdsubBlobSource returns the SecondaryBlobSource backing the given
// cdsub source when it points at a real containerd content store
// (linux + reachable socket). The transfer endpoint chains this in
// after the local cache so that digests cdsub announced are actually
// serveable to peers. Returns nil for NoOpSource, which makes the
// transfer endpoint fall through to its plain 404 path.
func cdsubBlobSource(src cdsub.ImageSource) ifaces.SecondaryBlobSource {
	cs, ok := src.(*cdsub.ContainerdSource)
	if !ok {
		return nil
	}
	bs := cdsub.NewContainerdBlobSource(cs)
	if bs == nil {
		return nil
	}
	return bs
}
