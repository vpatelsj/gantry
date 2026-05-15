// Package ifaces declares the cross-cutting interfaces that Gantry's
// subsystems implement and depend on.
//
// Each subsystem (cache, members, origin, peer, DHT) is reachable through
// the interfaces defined here so that:
//
//   - Unit tests can replace any subsystem with a fake (see internal/ifaces/fakes).
//   - The top-level agent wiring in internal/agent depends only on interfaces,
//     not on concrete libp2p / Kubernetes / hostPath implementations.
//
// Interfaces are intentionally minimal — only the methods the agent actually
// uses are exposed. Adding a method here should follow real demand from a
// caller, not speculative API surface.
package ifaces

import (
	"context"
	"io"
	"time"

	"github.com/gantry/gantry/internal/digest"
)

// ---------------------------------------------------------------------------
// Cache: on-disk content store for blobs and manifests.
// Implemented by internal/cache (Phase 1).
// ---------------------------------------------------------------------------

// Cache is a content-addressed store keyed by OCI digest. Implementations
// MUST verify the streamed bytes against the digest before treating an entry
// as committed (F7 in archecture.md).
type Cache interface {
	// Has reports whether the digest is present in the local store.
	Has(ctx context.Context, d digest.Digest) (bool, error)

	// Open returns a reader for the cached bytes plus the content length.
	// Returns ErrNotFound if absent.
	Open(ctx context.Context, d digest.Digest) (io.ReadCloser, int64, error)

	// Writer returns a digest-verifying writer for d. Bytes written are
	// staged; the entry becomes visible to subsequent Has/Open calls only
	// after Commit. Abort discards the staging area.
	Writer(ctx context.Context, d digest.Digest) (CacheWriter, error)
}

// CacheWriter accumulates bytes for a single Cache entry. The implementation
// computes the digest incrementally; Commit fails if the streamed bytes do
// not match the digest passed to Cache.Writer.
type CacheWriter interface {
	io.Writer

	// Commit finalizes the entry. Fails if the streamed bytes did not hash
	// to the declared digest.
	Commit(ctx context.Context) error

	// Abort discards staged bytes. Idempotent.
	Abort(ctx context.Context) error
}

// ---------------------------------------------------------------------------
// Members: cluster-membership view, sourced from a Kubernetes informer.
// Implemented by internal/members (Phase 2).
// ---------------------------------------------------------------------------

// NodeID is the stable identity used by HRW (§5.2 step 3) — typically the
// pod or node name. It MUST be stable across an individual node's lifetime
// and identical across all agents' views (modulo informer lag, §5.3).
type NodeID string

// Node is one entry in the cluster-membership view.
type Node struct {
	ID NodeID

	// Addr is the network address to reach this node's transfer
	// endpoint (HTTP/2 on the configured transfer port). When the
	// transfer port is known (production deploy), Addr is "ip:port";
	// for back-compat with older snapshots it may be a bare IP and
	// callers must append the port.
	Addr string

	// Zone is the optional topology label `topology.kubernetes.io/zone`.
	// Empty when not topology-aware (§4.3).
	Zone string

	// PeerID is the libp2p peer.ID (CID-encoded string form) the node
	// publishes via its pod annotation. Empty until the peer announces.
	// coord.Client uses this to dial via libp2p without requiring that
	// NodeID itself be a peer.ID string.
	PeerID string

	// P2PAddrs lists the node's libp2p listen multiaddrs published via
	// pod annotation. Empty until the peer announces. main.go reads
	// this on startup to seed disco.Connect for DHT bootstrap (§7.2)
	// without needing operator-supplied bootstrap_peers.
	P2PAddrs []string
}

// Members is the live cluster-membership view.
type Members interface {
	// Self returns this agent's own NodeID.
	Self() NodeID

	// Snapshot returns the current node list. The returned slice is owned
	// by the caller; implementations MUST copy if they retain it.
	Snapshot() []Node

	// WaitForSync blocks until the underlying informer has completed its
	// initial list-and-watch sync. Used by readiness probes.
	WaitForSync(ctx context.Context) error
}

// ---------------------------------------------------------------------------
// OriginPuller: pulls bytes from the upstream OCI registry.
// Implemented by internal/origin (Phase 1).
// ---------------------------------------------------------------------------

// OriginRef identifies a digest at a specific upstream registry / repository.
// The triple matches the fields of coordv1.PleasePullRequest.
type OriginRef struct {
	Registry   string // e.g. "registry.example.com"
	Repository string // e.g. "library/nginx"
	Digest     digest.Digest

	// Kind discriminates the OCI Distribution Spec URL family for this
	// reference. Manifests live at /v2/<repo>/manifests/<digest>, blobs at
	// /v2/<repo>/blobs/<digest>. Zero value (KindBlob) is the common case;
	// only the mirror's manifest-by-digest path and cold-start manifest
	// pulls set KindManifest.
	Kind OriginRefKind
}

// OriginRefKind discriminates manifest vs blob URLs at the upstream.
//
// Note on KindConfig: per the OCI Distribution Spec the image-config
// document is fetched from /v2/<repo>/blobs/<digest> — the same URL
// family as KindBlob. KindConfig therefore does NOT change routing;
// it exists purely to tighten metric/log labels so cold-start manifest
// → config → blob traversal is distinguishable from regular layer
// fetches when reading dashboards or traces.
type OriginRefKind int

// Recognised OriginRefKind values.
const (
	KindBlob     OriginRefKind = 0
	KindManifest OriginRefKind = 1
	KindConfig   OriginRefKind = 2
)

func (k OriginRefKind) String() string {
	switch k {
	case KindManifest:
		return "manifest"
	case KindConfig:
		return "config"
	default:
		return "blob"
	}
}

// OriginPuller fetches a single digest from origin.
type OriginPuller interface {
	// Pull opens a streaming read of the digest's bytes from origin. The
	// returned ReadCloser is digest-unverified; the caller is expected to
	// verify via a Cache writer or equivalent.
	//
	// On terminal failure the returned error is wrapped in an *OriginError
	// carrying the failure classification used by §5.8.
	Pull(ctx context.Context, ref OriginRef) (io.ReadCloser, int64, error)
}

// OriginError is the error returned by OriginPuller.Pull for terminal
// failures. The Class field is the §5.8 classification used by the negative
// cache and propagated via PullIntentResponse.failure_class.
type OriginError struct {
	Ref   OriginRef
	Class FailureClass
	Err   error
}

func (e *OriginError) Error() string {
	if e.Err == nil {
		return "origin error: " + string(e.Class)
	}
	return "origin error (" + string(e.Class) + "): " + e.Err.Error()
}

func (e *OriginError) Unwrap() error { return e.Err }

// FailureClass mirrors coordv1.FailureClass; defined here so non-proto
// callers don't import the generated package.
type FailureClass string

// Recognised §5.8 failure classifications.
const (
	FailureUnspecified FailureClass = ""
	FailureAuth        FailureClass = "auth"
	FailureNotFound    FailureClass = "not_found"
	FailureRateLimited FailureClass = "rate_limited"
	FailureTransient   FailureClass = "transient"
)

// ---------------------------------------------------------------------------
// PeerDialer: fetches a digest from another agent's :5001 transfer endpoint.
// Implemented by internal/transfer (Phase 2).
// ---------------------------------------------------------------------------

// PeerDialer fetches a digest from a peer's transfer endpoint with the
// `Gantry-Mirrored: 1` header set (archecture.md §API).
type PeerDialer interface {
	// FetchFromPeer streams the digest's bytes from peerAddr's :5001
	// endpoint. The implementation MUST set `Gantry-Mirrored: 1` and MUST
	// surface a NotFound error distinctly from transport errors so the
	// caller can fail over to the next provider.
	FetchFromPeer(ctx context.Context, peerAddr string, ref OriginRef) (io.ReadCloser, int64, error)
}

// ---------------------------------------------------------------------------
// DHT: digest-keyed discovery layer.
// Implemented by internal/discovery (Phase 2).
// ---------------------------------------------------------------------------

// Provider is one entry returned by DHT.FindProviders.
type Provider struct {
	NodeID NodeID
	Addr   string
}

// DHT exposes the libp2p Kademlia operations Gantry needs.
type DHT interface {
	// FindProviders returns providers of d. Returning an empty slice and a
	// nil error is the "DHT-empty" case (§5.2): the caller MUST NOT treat
	// it as ground truth and SHOULD fall through to the HRW top-K probe.
	FindProviders(ctx context.Context, d digest.Digest) ([]Provider, error)

	// Provide advertises that this node holds d. Idempotent at the DHT
	// level; refreshing is the implementation's responsibility (libp2p
	// default 12 h refresh, 24 h TTL — §7.2).
	Provide(ctx context.Context, d digest.Digest) error

	// Health returns the current DHT health score in [0,1] as defined by
	// §7.7 (geometric mean of routing-table coverage, lookup-latency
	// score, and self-test success rate).
	Health() float64
}

// ---------------------------------------------------------------------------
// Coordinator: libp2p coordination RPC client (caller side).
// Implemented by internal/coord (Phase 3).
// ---------------------------------------------------------------------------

// PullIntent is the requester-side view of a PullIntentResponse.
type PullIntent struct {
	HasCached      bool
	InFlight       bool
	StartedAt      time.Time
	RecipientRank  int32
	RecentlyFailed bool
	CooldownUntil  time.Time
	FailureClass   FailureClass
}

// PleasePullOutcome is the requester-side view of a single
// PleasePullResponse.Result.
type PleasePullOutcome struct {
	Digest        digest.Digest
	Outcome       PleasePullStatus
	StartedAt     time.Time
	CooldownUntil time.Time
	FailureClass  FailureClass
}

// PleasePullStatus mirrors coordv1.PleasePullResponse.Result.Outcome.
type PleasePullStatus int

// Recognised PleasePull outcome values.
const (
	PleasePullUnspecified PleasePullStatus = iota
	PleasePullAlreadyPulling
	PleasePullStarted
	PleasePullRecentlyFailed
)

// Coordinator issues coordination RPCs to peers. Implementations are
// expected to open one libp2p stream per call.
type Coordinator interface {
	PullIntentQuery(ctx context.Context, peer NodeID, d digest.Digest) (PullIntent, error)
	PleasePull(ctx context.Context, peer NodeID, registry, repository string, kind OriginRefKind, digests []digest.Digest) ([]PleasePullOutcome, error)
}

// LocalIntentProvider computes the PullIntent for self synchronously,
// without going through a libp2p coord stream. The cold-start
// orchestrator uses it to include self as a first-class participant
// in the §5.2 rule cascade so that when self is HRW rank 0, self
// pulls instead of delegating to rank 1 (which violates the
// "one origin pull per digest" thundering-herd invariant — every
// requester must converge on the same designated puller, and that
// puller MAY be self).
type LocalIntentProvider interface {
	LocalPullIntent(ctx context.Context, d digest.Digest) PullIntent
}

// LocalPullStarter starts an origin pull on the local node without
// going through a libp2p coord stream. The cold-start orchestrator
// invokes this when rule 7 selects self as the designated puller;
// the wire-level alternative (Coord.PleasePull(self, ...)) would
// either fail to dial self or — worse — round-trip through libp2p
// and burn a stream slot. Semantics MUST match the server-side
// please_pull handler: each digest either starts a new origin pull,
// piggybacks on an already-in-flight one (PleasePullAlreadyPulling),
// or short-circuits on the negative cache (PleasePullRecentlyFailed).
type LocalPullStarter interface {
	StartLocalPull(ctx context.Context, registry, repository string, kind OriginRefKind, digests []digest.Digest) ([]PleasePullOutcome, error)
}

// ---------------------------------------------------------------------------
// Errors.
// ---------------------------------------------------------------------------

// SecondaryBlobSource is an optional read-only blob source consulted by
// the peer-fetch transfer endpoint when the local cache returns
// ErrNotFound, and by the coord pull_intent_query handler when computing
// effective local availability. The canonical implementation wraps the
// local containerd content store so that blobs containerd already
// pulled (and that the cdsub source announced on the DHT) can be
// served to peers without re-downloading them through the mirror.
//
// Without this hop, cdsub.Source announces presence of a digest the
// transfer endpoint then 404s on — peers fetch nothing useful and the
// origin-pull-bandwidth-amplification problem isn't actually solved.
// Similarly, pull_intent_query that consulted only the Gantry cache
// would advertise has_cached=false for digests containerd already has,
// triggering redundant please_pull / origin fetches.
//
// Implementations MUST verify the digest of returned bytes (the
// containerd content store already maintains digest integrity, but a
// custom backend would need its own check). Open returns *ErrNotFound
// when the digest is not locally present so the transfer endpoint can
// distinguish miss from error.
//
// Has is a metadata-only existence check used by pull_intent_query.
// It MUST NOT open a streaming reader; the containerd impl uses
// content.Store.Info() which is a single map lookup. A nil error and
// false return signals "definitively absent"; a non-nil error means
// the backend itself failed (the caller should treat this as "do not
// claim local availability" without rolling it into has_cached=true).
type SecondaryBlobSource interface {
	Open(ctx context.Context, d digest.Digest) (io.ReadCloser, int64, error)
	Has(ctx context.Context, d digest.Digest) (bool, error)
}

// ErrNotFound is returned by Cache and PeerDialer to signal a digest is not
// locally available. Distinct from transport-level errors so callers can
// distinguish "fall back to next provider" from "definitively missing here".
type ErrNotFound struct {
	Digest digest.Digest
}

func (e *ErrNotFound) Error() string { return "not found: " + e.Digest.String() }
