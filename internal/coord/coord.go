// Package coord implements Gantry's libp2p coordination RPCs.
//
// Wire protocol: `/gantry/coord/1.0.0` (one libp2p stream per
// request/response pair, closed after reply). Framing: length-delimited
// protobuf via `go-msgio` — the design forbids gRPC (§4.4). Forward
// compatibility: additive changes bump the minor (e.g. `1.1.0`);
// breaking changes bump the major.
//
// Two coordinated message families:
//
//   - `pull_intent_query` / `pull_intent_response` (§5.2 step 4) — a
//     stateless probe asking a peer "do you have this digest cached,
//     are you pulling it, or have you recently failed to pull it?".
//     The responder fills hrw_rank from its own view of cluster
//     membership so the requester can detect informer divergence
//     (§5.3).
//
//   - `please_pull` / `please_pull_response` (§5.2 step 6) — asks a
//     peer (the designated puller per HRW) to pull one or more digests
//     of a single repo. The responder's in-flight map dedupes; we get
//     STARTED / ALREADY_PULLING / RECENTLY_FAILED per-digest results
//     back.
//
// This package owns both the server-side stream handler and a typed
// client. Higher layers (cold-start orchestrator, mirror) interact
// only via the `ifaces.Coordinator` interface.
package coord

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-msgio"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/gantry/gantry/internal/digest"
	"github.com/gantry/gantry/internal/hrw"
	"github.com/gantry/gantry/internal/ifaces"
	"github.com/gantry/gantry/internal/inflight"
	coordv1 "github.com/gantry/gantry/proto/gantry/coord/v1"
)

// ProtocolID is the libp2p stream protocol the coord handler binds.
const ProtocolID protocol.ID = "/gantry/coord/1.0.0"

// MaxMessageBytes caps a single inbound Envelope. PullIntentRequest is
// tiny; PleasePullRequest grows linearly with batch size. 1 MiB is
// orders of magnitude beyond the realistic ceiling but keeps memory
// bounded under malformed input.
const MaxMessageBytes = 1 << 20

// MetricsHooks lets callers wire Prometheus counters/gauges without
// importing the metrics package. All fields may be nil.
type MetricsHooks struct {
	// OnPullIntentServed fires once per pull_intent_query handled.
	OnPullIntentServed func()
	// OnPleasePullServed fires once per please_pull *request* handled
	// (not per digest in the batch).
	OnPleasePullServed func()
	// OnPleasePullStarted is called once per digest the server
	// transitions into in_flight from a please_pull batch.
	OnPleasePullStarted func()
	// OnStreamError fires for any malformed or oversized stream.
	OnStreamError func()
}

// Server handles inbound coord streams: pull_intent_query and
// please_pull RPCs. One stream per request, closed after reply.
type Server struct {
	logger   *slog.Logger
	hooks    MetricsHooks
	cache    ifaces.Cache
	members  ifaces.Members
	inflight *inflight.Map
	// negCache is consulted by pull_intent_query to populate
	// recently_failed / cooldown_until / failure_class. Phase 4 lands a
	// real implementation; Phase 3 ships with a nil-safe call site so
	// the wire field round-trips even before the negative cache exists.
	negCache NegativeCache
	// pullerPump is invoked by the please_pull handler for each
	// (registry, repository, digest) we accept. The supplied function
	// is expected to start a background pull (it must not block the
	// stream handler). nil disables please_pull semantically — the
	// handler still acks but with OUTCOME_UNSPECIFIED.
	pullerPump PullerPump
}

// NegativeCache is the read interface coord needs from the §5.8
// circuit-breaker (Phase 4). Returning ok == false means the digest
// has no negative-cache entry on this node.
type NegativeCache interface {
	Lookup(d digest.Digest) (entry NegativeEntry, ok bool)
}

// NegativeEntry mirrors §5.8 state for a single digest.
type NegativeEntry struct {
	CooldownUntil time.Time
	Class         ifaces.FailureClass
}

// PullerPump is invoked by the please_pull handler with a fully-
// classified pull request. It MUST return promptly: the call happens
// inside the stream handler and the server response can't be written
// until pump returns. Long-running work (the actual origin pull) MUST
// be moved to a goroutine inside the pump's implementation.
//
// The returned (started_at, alreadyPulling) tuple drives the wire-
// level OUTCOME_STARTED vs OUTCOME_ALREADY_PULLING decision. Stable
// across phases.
type PullerPump func(ctx context.Context, registry, repository string, d digest.Digest, kind ifaces.OriginRefKind) (startedAt time.Time, alreadyPulling bool, fail *NegativeEntry)

// Option configures a Server.
type Option func(*Server)

// WithLogger plumbs a structured logger.
func WithLogger(l *slog.Logger) Option {
	return func(s *Server) {
		if l != nil {
			s.logger = l.With(slog.String("subsystem", "coord"))
		}
	}
}

// WithMetrics attaches metric callbacks.
func WithMetrics(h MetricsHooks) Option {
	return func(s *Server) { s.hooks = h }
}

// WithNegativeCache attaches a §5.8 read interface. nil is fine; the
// response just doesn't set recently_failed.
func WithNegativeCache(n NegativeCache) Option {
	return func(s *Server) { s.negCache = n }
}

// WithPullerPump wires the please_pull handler to the local origin
// puller. Required for please_pull to do useful work; without it the
// handler returns OUTCOME_UNSPECIFIED.
func WithPullerPump(p PullerPump) Option {
	return func(s *Server) { s.pullerPump = p }
}

// NewServer constructs a coord server. The cache + members + inflight
// dependencies are required (everything else is optional via Option).
func NewServer(cache ifaces.Cache, members ifaces.Members, inflight *inflight.Map, opts ...Option) *Server {
	s := &Server{
		logger:   slog.Default().With(slog.String("subsystem", "coord")),
		cache:    cache,
		members:  members,
		inflight: inflight,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Bind registers the stream handler on h. After Bind returns, peers
// dialing ProtocolID will be served by s.
func (s *Server) Bind(h host.Host) {
	h.SetStreamHandler(ProtocolID, s.handleStream)
}

// handleStream is invoked by libp2p for each inbound stream. The
// design pins "one stream per request/response pair" — we read one
// length-delimited envelope, dispatch, write one envelope, close.
func (s *Server) handleStream(str network.Stream) {
	defer func() { _ = str.Close() }()

	r := msgio.NewVarintReaderSize(str, MaxMessageBytes)
	w := msgio.NewVarintWriter(str)

	bytes, err := r.ReadMsg()
	if err != nil {
		s.bumpStreamErr()
		s.logger.Debug("coord: read envelope", slog.Any("err", err))
		return
	}
	defer r.ReleaseMsg(bytes)

	in := &coordv1.Envelope{}
	if err := proto.Unmarshal(bytes, in); err != nil {
		s.bumpStreamErr()
		s.logger.Debug("coord: unmarshal envelope", slog.Any("err", err))
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	out, err := s.dispatch(ctx, str.Conn().RemotePeer(), in)
	if err != nil {
		s.bumpStreamErr()
		s.logger.Debug("coord: dispatch", slog.Any("err", err))
		return
	}
	if out == nil {
		return
	}

	rb, err := proto.Marshal(out)
	if err != nil {
		s.bumpStreamErr()
		s.logger.Debug("coord: marshal response", slog.Any("err", err))
		return
	}
	if err := w.WriteMsg(rb); err != nil {
		s.bumpStreamErr()
		s.logger.Debug("coord: write response", slog.Any("err", err))
		return
	}
}

func (s *Server) dispatch(ctx context.Context, remote peer.ID, in *coordv1.Envelope) (*coordv1.Envelope, error) {
	switch m := in.GetMsg().(type) {
	case *coordv1.Envelope_PullIntentRequest:
		resp, err := s.servePullIntent(ctx, m.PullIntentRequest)
		if err != nil {
			return nil, err
		}
		if s.hooks.OnPullIntentServed != nil {
			s.hooks.OnPullIntentServed()
		}
		return wrapPullIntentResponse(resp), nil
	case *coordv1.Envelope_PleasePullRequest:
		resp, err := s.servePleasePull(ctx, remote, m.PleasePullRequest)
		if err != nil {
			return nil, err
		}
		if s.hooks.OnPleasePullServed != nil {
			s.hooks.OnPleasePullServed()
		}
		return wrapPleasePullResponse(resp), nil
	case nil:
		return nil, errors.New("coord: empty envelope")
	default:
		return nil, fmt.Errorf("coord: unexpected message %T (this side is a server only)", m)
	}
}

func (s *Server) servePullIntent(ctx context.Context, req *coordv1.PullIntentRequest) (*coordv1.PullIntentResponse, error) {
	d, err := digest.Parse(req.GetDigest())
	if err != nil {
		return nil, fmt.Errorf("pull_intent: %w", err)
	}

	resp := &coordv1.PullIntentResponse{}

	// has_cached
	if ok, err := s.cache.Has(ctx, d); err == nil && ok {
		resp.HasCached = true
	}

	// in_flight / started_at
	if e, ok := s.inflight.LookupForIntent(d); ok {
		resp.InFlight = true
		resp.StartedAt = timestamppb.New(e.StartedAt)
	}

	// hrw_rank — own rank in own membership view.
	if s.members != nil {
		nodes := s.members.Snapshot()
		resp.HrwRank = hrw.RankOf(nodes, s.members.Self(), d)
	} else {
		resp.HrwRank = -1
	}

	// §5.8 negative-cache fields.
	if s.negCache != nil {
		if e, ok := s.negCache.Lookup(d); ok {
			resp.RecentlyFailed = true
			resp.CooldownUntil = timestamppb.New(e.CooldownUntil)
			resp.FailureClass = failureClassToProto(e.Class)
		}
	}

	return resp, nil
}

func (s *Server) servePleasePull(ctx context.Context, _ peer.ID, req *coordv1.PleasePullRequest) (*coordv1.PleasePullResponse, error) {
	// §4.4 invariant: one repo per batch. Empty / malformed → reject.
	if req.GetUpstreamRegistry() == "" || req.GetRepository() == "" {
		return nil, errors.New("please_pull: missing registry/repository")
	}
	if len(req.GetDigests()) == 0 {
		return &coordv1.PleasePullResponse{}, nil
	}

	results := make([]*coordv1.PleasePullResponse_Result, 0, len(req.GetDigests()))
	for _, raw := range req.GetDigests() {
		d, err := digest.Parse(raw)
		if err != nil {
			s.bumpStreamErr()
			s.logger.Debug("please_pull: bad digest",
				slog.String("digest", raw),
				slog.Any("err", err),
			)
			continue
		}
		r := &coordv1.PleasePullResponse_Result{Digest: d.String()}
		if s.pullerPump == nil {
			r.Outcome = coordv1.PleasePullResponse_Result_OUTCOME_UNSPECIFIED
			results = append(results, r)
			continue
		}
		startedAt, already, fail := s.pullerPump(ctx, req.GetUpstreamRegistry(), req.GetRepository(), d, ifaces.KindBlob)
		switch {
		case fail != nil:
			r.Outcome = coordv1.PleasePullResponse_Result_OUTCOME_RECENTLY_FAILED
			r.CooldownUntil = timestamppb.New(fail.CooldownUntil)
			r.FailureClass = failureClassToProto(fail.Class)
		case already:
			r.Outcome = coordv1.PleasePullResponse_Result_OUTCOME_ALREADY_PULLING
			r.StartedAt = timestamppb.New(startedAt)
		default:
			r.Outcome = coordv1.PleasePullResponse_Result_OUTCOME_STARTED
			r.StartedAt = timestamppb.New(startedAt)
			if s.hooks.OnPleasePullStarted != nil {
				s.hooks.OnPleasePullStarted()
			}
		}
		results = append(results, r)
	}
	return &coordv1.PleasePullResponse{Results: results}, nil
}

func (s *Server) bumpStreamErr() {
	if s.hooks.OnStreamError != nil {
		s.hooks.OnStreamError()
	}
}

// ---------------------------------------------------------------------------
// Client side. Implements ifaces.Coordinator.
// ---------------------------------------------------------------------------

// Client opens a libp2p stream per RPC. Members is used to resolve a
// ifaces.NodeID to a libp2p peer.ID for the dial. Phase 3 ships a
// minimal NodeID→peer.ID mapping that accepts the libp2p peer.ID string
// form directly as the NodeID (matches what `internal/discovery.Host`
// returns from FindProviders). Real K8s-pod-name → peer.ID mapping is
// owned by `internal/members` and surfaces through a richer Node type
// in Phase 4+.
type Client struct {
	h            host.Host
	dialTimeout  time.Duration
	rpcTimeout   time.Duration
	logger       *slog.Logger
	resolveMu    sync.RWMutex
	resolveCache map[ifaces.NodeID]peer.ID
}

// ClientOption configures a Client.
type ClientOption func(*Client)

// WithDialTimeout overrides the per-RPC dial timeout (default 2s).
func WithDialTimeout(d time.Duration) ClientOption {
	return func(c *Client) {
		if d > 0 {
			c.dialTimeout = d
		}
	}
}

// WithRPCTimeout overrides the per-RPC end-to-end timeout (default 2s).
func WithRPCTimeout(d time.Duration) ClientOption {
	return func(c *Client) {
		if d > 0 {
			c.rpcTimeout = d
		}
	}
}

// WithClientLogger overrides the logger.
func WithClientLogger(l *slog.Logger) ClientOption {
	return func(c *Client) {
		if l != nil {
			c.logger = l.With(slog.String("subsystem", "coord-client"))
		}
	}
}

// NewClient returns a coord RPC client. h must be a running libp2p
// host already participating in the coord protocol's transports.
func NewClient(h host.Host, opts ...ClientOption) *Client {
	c := &Client{
		h:            h,
		dialTimeout:  2 * time.Second,
		rpcTimeout:   2 * time.Second,
		logger:       slog.Default().With(slog.String("subsystem", "coord-client")),
		resolveCache: map[ifaces.NodeID]peer.ID{},
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// PullIntentQuery implements ifaces.Coordinator.
func (c *Client) PullIntentQuery(ctx context.Context, target ifaces.NodeID, d digest.Digest) (ifaces.PullIntent, error) {
	in := &coordv1.Envelope{Msg: &coordv1.Envelope_PullIntentRequest{
		PullIntentRequest: &coordv1.PullIntentRequest{Digest: d.String()},
	}}
	out, err := c.roundTrip(ctx, target, in)
	if err != nil {
		return ifaces.PullIntent{}, err
	}
	resp := out.GetPullIntentResponse()
	if resp == nil {
		return ifaces.PullIntent{}, errors.New("coord: empty pull_intent_response")
	}
	return ifaces.PullIntent{
		HasCached:      resp.GetHasCached(),
		InFlight:       resp.GetInFlight(),
		StartedAt:      resp.GetStartedAt().AsTime(),
		RecipientRank:  resp.GetHrwRank(),
		RecentlyFailed: resp.GetRecentlyFailed(),
		CooldownUntil:  resp.GetCooldownUntil().AsTime(),
		FailureClass:   failureClassFromProto(resp.GetFailureClass()),
	}, nil
}

// PleasePull implements ifaces.Coordinator.
func (c *Client) PleasePull(ctx context.Context, target ifaces.NodeID, registry, repository string, digests []digest.Digest) ([]ifaces.PleasePullOutcome, error) {
	raws := make([]string, len(digests))
	for i, d := range digests {
		raws[i] = d.String()
	}
	in := &coordv1.Envelope{Msg: &coordv1.Envelope_PleasePullRequest{
		PleasePullRequest: &coordv1.PleasePullRequest{
			Digests:          raws,
			UpstreamRegistry: registry,
			Repository:       repository,
		},
	}}
	out, err := c.roundTrip(ctx, target, in)
	if err != nil {
		return nil, err
	}
	resp := out.GetPleasePullResponse()
	if resp == nil {
		return nil, errors.New("coord: empty please_pull_response")
	}
	outs := make([]ifaces.PleasePullOutcome, 0, len(resp.GetResults()))
	for _, r := range resp.GetResults() {
		d, err := digest.Parse(r.GetDigest())
		if err != nil {
			c.logger.Debug("please_pull: bad result digest", slog.String("digest", r.GetDigest()), slog.Any("err", err))
			continue
		}
		outs = append(outs, ifaces.PleasePullOutcome{
			Digest:        d,
			Outcome:       pleasePullStatusFromProto(r.GetOutcome()),
			StartedAt:     r.GetStartedAt().AsTime(),
			CooldownUntil: r.GetCooldownUntil().AsTime(),
			FailureClass:  failureClassFromProto(r.GetFailureClass()),
		})
	}
	return outs, nil
}

// ResolvePeerID lets external wiring teach the client how to map a
// NodeID to a libp2p peer.ID. Higher layers (members, discovery) own
// this mapping; we just cache lookups.
func (c *Client) ResolvePeerID(id ifaces.NodeID, pid peer.ID) {
	c.resolveMu.Lock()
	c.resolveCache[id] = pid
	c.resolveMu.Unlock()
}

func (c *Client) lookupPeerID(id ifaces.NodeID) (peer.ID, error) {
	c.resolveMu.RLock()
	if pid, ok := c.resolveCache[id]; ok {
		c.resolveMu.RUnlock()
		return pid, nil
	}
	c.resolveMu.RUnlock()
	// Fallback: treat NodeID as a peer.ID string. internal/discovery
	// surfaces providers this way already.
	pid, err := peer.Decode(string(id))
	if err != nil {
		return "", fmt.Errorf("coord: cannot resolve NodeID %q to peer.ID: %w", id, err)
	}
	return pid, nil
}

func (c *Client) roundTrip(ctx context.Context, target ifaces.NodeID, env *coordv1.Envelope) (*coordv1.Envelope, error) {
	pid, err := c.lookupPeerID(target)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, c.rpcTimeout)
	defer cancel()

	dialCtx, dialCancel := context.WithTimeout(ctx, c.dialTimeout)
	str, err := c.h.NewStream(dialCtx, pid, ProtocolID)
	dialCancel()
	if err != nil {
		return nil, fmt.Errorf("coord: open stream: %w", err)
	}
	defer func() { _ = str.Close() }()

	if dl, ok := ctx.Deadline(); ok {
		_ = str.SetDeadline(dl)
	}

	bytes, err := proto.Marshal(env)
	if err != nil {
		return nil, fmt.Errorf("coord: marshal: %w", err)
	}
	w := msgio.NewVarintWriter(str)
	if err := w.WriteMsg(bytes); err != nil {
		return nil, fmt.Errorf("coord: write: %w", err)
	}
	// Signal end of write side so the server can read EOF if it pages.
	_ = str.CloseWrite()

	r := msgio.NewVarintReaderSize(str, MaxMessageBytes)
	rb, err := r.ReadMsg()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, errors.New("coord: peer closed stream without response")
		}
		return nil, fmt.Errorf("coord: read: %w", err)
	}
	defer r.ReleaseMsg(rb)

	out := &coordv1.Envelope{}
	if err := proto.Unmarshal(rb, out); err != nil {
		return nil, fmt.Errorf("coord: unmarshal: %w", err)
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// proto helpers
// ---------------------------------------------------------------------------

func wrapPullIntentResponse(r *coordv1.PullIntentResponse) *coordv1.Envelope {
	return &coordv1.Envelope{Msg: &coordv1.Envelope_PullIntentResponse{PullIntentResponse: r}}
}

func wrapPleasePullResponse(r *coordv1.PleasePullResponse) *coordv1.Envelope {
	return &coordv1.Envelope{Msg: &coordv1.Envelope_PleasePullResponse{PleasePullResponse: r}}
}

func failureClassToProto(c ifaces.FailureClass) coordv1.FailureClass {
	switch c {
	case ifaces.FailureAuth:
		return coordv1.FailureClass_FAILURE_CLASS_AUTH
	case ifaces.FailureNotFound:
		return coordv1.FailureClass_FAILURE_CLASS_NOT_FOUND
	case ifaces.FailureRateLimited:
		return coordv1.FailureClass_FAILURE_CLASS_RATE_LIMITED
	case ifaces.FailureTransient:
		return coordv1.FailureClass_FAILURE_CLASS_TRANSIENT
	default:
		return coordv1.FailureClass_FAILURE_CLASS_UNSPECIFIED
	}
}

func failureClassFromProto(c coordv1.FailureClass) ifaces.FailureClass {
	switch c {
	case coordv1.FailureClass_FAILURE_CLASS_AUTH:
		return ifaces.FailureAuth
	case coordv1.FailureClass_FAILURE_CLASS_NOT_FOUND:
		return ifaces.FailureNotFound
	case coordv1.FailureClass_FAILURE_CLASS_RATE_LIMITED:
		return ifaces.FailureRateLimited
	case coordv1.FailureClass_FAILURE_CLASS_TRANSIENT:
		return ifaces.FailureTransient
	default:
		return ifaces.FailureUnspecified
	}
}

func pleasePullStatusFromProto(o coordv1.PleasePullResponse_Result_Outcome) ifaces.PleasePullStatus {
	switch o {
	case coordv1.PleasePullResponse_Result_OUTCOME_ALREADY_PULLING:
		return ifaces.PleasePullAlreadyPulling
	case coordv1.PleasePullResponse_Result_OUTCOME_STARTED:
		return ifaces.PleasePullStarted
	case coordv1.PleasePullResponse_Result_OUTCOME_RECENTLY_FAILED:
		return ifaces.PleasePullRecentlyFailed
	default:
		return ifaces.PleasePullUnspecified
	}
}

// Compile-time conformance.
var _ ifaces.Coordinator = (*Client)(nil)
var _ net.Addr = (*nopAddr)(nil)

type nopAddr struct{}

func (nopAddr) Network() string { return "coord" }
func (nopAddr) String() string  { return "coord" }
