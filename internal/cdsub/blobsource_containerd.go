//go:build linux

package cdsub

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"github.com/containerd/containerd/v2/pkg/namespaces"
	cerrdefs "github.com/containerd/errdefs"
	godigest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	gdigest "github.com/gantry/gantry/internal/digest"
	"github.com/gantry/gantry/internal/ifaces"
)

// ContainerdBlobSource is the ifaces.SecondaryBlobSource implementation
// that reads blobs out of containerd's local content store. The
// transfer endpoint uses it as the cache-miss fallback so that digests
// announced by cdsub.Source (which walks the same content store) are
// actually serveable to peers.
//
// Background: cdsub.Source publishes Provider records for every digest
// it walks from containerd's content store. Before this hop existed,
// the transfer endpoint only knew about Gantry's own cache and 404'd
// on those digests — peers received misleading announcements and the
// origin-bandwidth-amplification problem wasn't actually solved.
//
// The content store already enforces digest integrity; we trust its
// bytes without re-verifying. Returns *ifaces.ErrNotFound for missing
// digests so the transfer endpoint can branch on miss vs error.
type ContainerdBlobSource struct {
	src    *ContainerdSource
	logger *slog.Logger
}

// NewContainerdBlobSource adapts an already-connected ContainerdSource
// into a SecondaryBlobSource. Sharing the client avoids opening a
// second gRPC connection and pins both subsystems to the same
// containerd namespace (k8s.io by default).
func NewContainerdBlobSource(src *ContainerdSource) *ContainerdBlobSource {
	if src == nil {
		return nil
	}
	return &ContainerdBlobSource{
		src:    src,
		logger: src.logger.With(slog.String("component", "blob-source")),
	}
}

// Open returns a streaming reader for d from the containerd content
// store. Pins the request to the source's containerd namespace.
func (b *ContainerdBlobSource) Open(ctx context.Context, d gdigest.Digest) (io.ReadCloser, int64, error) {
	if b == nil || b.src == nil || b.src.client == nil {
		return nil, 0, &ifaces.ErrNotFound{Digest: d}
	}
	ctx = namespaces.WithNamespace(ctx, b.src.namespace)

	desc := ocispec.Descriptor{Digest: godigest.Digest(d.String())}
	store := b.src.client.ContentStore()
	ra, err := store.ReaderAt(ctx, desc)
	if err != nil {
		if errors.Is(err, cerrdefs.ErrNotFound) {
			return nil, 0, &ifaces.ErrNotFound{Digest: d}
		}
		return nil, 0, fmt.Errorf("containerd blob source: ReaderAt: %w", err)
	}
	size := ra.Size()
	// Wrap the ReaderAt in a SectionReader for streaming semantics;
	// the transfer endpoint reads sequentially and only needs Read +
	// (occasionally) Seek through io.ReadSeeker. SectionReader gives
	// us both without staging the blob in memory.
	return &readerAtCloser{
		SectionReader: io.NewSectionReader(ra, 0, size),
		closer:        ra,
	}, size, nil
}

// Has reports whether the containerd content store already has d
// locally. Metadata-only — does NOT open a reader handle. The coord
// pull_intent_query handler uses this to OR with the Gantry cache so
// has_cached reflects effective local availability (a digest in the
// containerd store is serveable to peers via Open, so advertising it
// is honest).
//
// nil error + true: present.
// nil error + false: definitively absent.
// non-nil error: backend failure; caller MUST NOT roll into
// has_cached=true (treat as unknown / absent).
func (b *ContainerdBlobSource) Has(ctx context.Context, d gdigest.Digest) (bool, error) {
	if b == nil || b.src == nil || b.src.client == nil {
		return false, nil
	}
	ctx = namespaces.WithNamespace(ctx, b.src.namespace)
	_, err := b.src.client.ContentStore().Info(ctx, godigest.Digest(d.String()))
	if err != nil {
		if errors.Is(err, cerrdefs.ErrNotFound) {
			return false, nil
		}
		return false, fmt.Errorf("containerd blob source: Info: %w", err)
	}
	return true, nil
}

// readerAtCloser bundles a SectionReader (Read+Seek) with the
// underlying content.ReaderAt's Close so the transfer endpoint can
// type-assert io.ReadSeeker for Range serving while still releasing
// the content-store handle.
type readerAtCloser struct {
	*io.SectionReader
	closer io.Closer
}

func (r *readerAtCloser) Close() error { return r.closer.Close() }

// Compile-time interface check.
var _ ifaces.SecondaryBlobSource = (*ContainerdBlobSource)(nil)
