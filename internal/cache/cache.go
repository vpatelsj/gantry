// Package cache is Gantry's on-disk content-addressed store.
//
// Layout:
//
//	{CacheDir}/blobs/sha256/<ab>/<hex>      committed blobs (sharded by first 2 hex chars)
//	{CacheDir}/tmp/<random>.partial         staging files for in-progress writes
//
// Phase 6 semantics (§7.4):
//
//   - Writes go through a digest-verifying CacheWriter. Commit renames the
//     temp file into place atomically. The expected digest is recomputed
//     incrementally during Write so Commit's hash check is constant-time
//     and the entry never appears under the wrong digest.
//
//   - Eviction is LRU at the layer level with provider-count deferral.
//     Before evicting an entry, the agent queries the configured
//     ProviderCount callback (typically dht.FindProviders). When the
//     local node is one of fewer than EvictionThreshold providers,
//     eviction is deferred and the loop moves on to the next-oldest
//     candidate. A short-interval local count cache (ProviderCountTTL)
//     prevents DHT storms.
//
//   - Forced-eviction headroom: when free disk on the cache volume
//     falls below cache_budget × ForcedHeadroomPct / 100 (default 5%),
//     eviction proceeds against the LRU candidate regardless of
//     provider count. The eviction is logged at WARN level with the
//     CID and provider count and increments
//     p2p_cache_forced_eviction_total.
//
//   - On startup, Open walks the on-disk content tree and seeds the LRU
//     order by mtime (oldest first) so the in-memory size accounting
//     matches what's actually on disk.
package cache

import (
	"container/list"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/gantry/gantry/internal/digest"
	"github.com/gantry/gantry/internal/digestpipe"
	"github.com/gantry/gantry/internal/ifaces"
)

// Cache is the on-disk content store.
//
// All public methods are safe for concurrent use.
type Cache struct {
	root        string
	budgetBytes int64
	logger      *slog.Logger
	metrics     metricsHooks
	policy      evictionPolicy

	mu      sync.Mutex
	size    int64
	entries map[string]*list.Element // key: digest.String()
	lru     *list.List               // front = MRU, back = LRU

	pcMu  sync.Mutex
	pcCnt map[string]providerCount

	evictMu sync.Mutex // serialises eviction sweeps
}

type entry struct {
	digest digest.Digest
	size   int64
}

type providerCount struct {
	count int
	at    time.Time
}

// evictionPolicy captures the §7.4 knobs. All fields are optional;
// nil/zero defaults preserve the simple-LRU behaviour used by tests.
type evictionPolicy struct {
	providerCount    func(context.Context, digest.Digest) (int, error)
	threshold        int
	headroomPct      int
	diskFree         func() (uint64, error)
	providerCountTTL time.Duration
	now              func() time.Time
}

// metricsHooks lets the cache emit counters without importing the metrics
// package directly (keeps the cache testable without Prometheus). Each
// hook is allowed to be nil.
type metricsHooks struct {
	onHit            func()
	onMiss           func()
	onEvicted        func(bytes int64)
	onForcedEviction func()
	onDeferredEvict  func()
}

// Option configures a Cache at Open time.
type Option func(*Cache)

// WithLogger plumbs a structured logger into the cache. The cache emits
// WARN on forced eviction (§7.4) and DEBUG on per-op events.
func WithLogger(l *slog.Logger) Option {
	return func(c *Cache) {
		if l != nil {
			c.logger = l.With(slog.String("subsystem", "cache"))
		}
	}
}

// WithMetrics registers the cache's metric hooks.
func WithMetrics(onHit, onMiss func(), onEvicted func(bytes int64)) Option {
	return func(c *Cache) {
		c.metrics.onHit = onHit
		c.metrics.onMiss = onMiss
		c.metrics.onEvicted = onEvicted
	}
}

// EvictionPolicy bundles the §7.4 knobs that turn on provider-count
// deferral and forced-headroom eviction. A zero value is a valid
// "preserve simple LRU" configuration.
type EvictionPolicy struct {
	// ProviderCount returns the number of distinct providers known
	// to the DHT for a digest. Nil disables deferral (simple LRU).
	// The callback is invoked WITHOUT the cache lock held but may
	// block on DHT I/O; the cache memoises the result for
	// ProviderCountTTL to avoid eviction-time storms.
	ProviderCount func(context.Context, digest.Digest) (int, error)

	// Threshold is the §7.4 deferral cut-off. Eviction is deferred
	// when ProviderCount returns a value < Threshold. Zero defaults
	// to 3.
	Threshold int

	// HeadroomPct is the percentage of the cache budget that acts as
	// the forced-eviction floor: when DiskFree() < budget * pct/100
	// eviction proceeds regardless of provider count. Zero defaults
	// to 5.
	HeadroomPct int

	// DiskFree returns the bytes free on the cache volume. Nil
	// disables forced-headroom logic.
	DiskFree func() (uint64, error)

	// ProviderCountTTL bounds how long a provider-count answer is
	// reused before a fresh DHT lookup is issued. Zero defaults to
	// 60 seconds.
	ProviderCountTTL time.Duration

	// OnForcedEviction is invoked once per forced-eviction event for
	// the p2p_cache_forced_eviction_total metric.
	OnForcedEviction func()

	// OnDeferredEvict is invoked once per deferred candidate (low
	// provider count, no forced pressure). Optional; used by tests.
	OnDeferredEvict func()

	// Now is a clock override for the count cache; nil uses
	// time.Now.
	Now func() time.Time
}

// WithEviction wires §7.4's provider-count deferral and forced-headroom
// eviction. Passing a zero EvictionPolicy is a no-op.
func WithEviction(p EvictionPolicy) Option {
	return func(c *Cache) {
		c.policy.providerCount = p.ProviderCount
		c.policy.threshold = p.Threshold
		c.policy.headroomPct = p.HeadroomPct
		c.policy.diskFree = p.DiskFree
		c.policy.providerCountTTL = p.ProviderCountTTL
		c.policy.now = p.Now
		c.metrics.onForcedEviction = p.OnForcedEviction
		c.metrics.onDeferredEvict = p.OnDeferredEvict
	}
}

// DefaultDiskFree returns the bytes free on the volume containing dir
// via Statfs. Suitable for plugging into EvictionPolicy.DiskFree on
// any POSIX host (Linux + Darwin tested).
func DefaultDiskFree(dir string) func() (uint64, error) {
	return func() (uint64, error) {
		var st syscall.Statfs_t
		if err := syscall.Statfs(dir, &st); err != nil {
			return 0, fmt.Errorf("cache: statfs %s: %w", dir, err)
		}
		// Bavail = blocks available to unprivileged user; the safe
		// choice (Bfree includes root-reserved blocks). Bavail is
		// uint64 on every supported platform, Bsize is signed (int32
		// on darwin, int64 on linux), so we widen only that side.
		return st.Bavail * uint64(st.Bsize), nil
	}
}

// Open returns a Cache rooted at dir with the given byte budget. The
// directory is created if needed. Existing content is enrolled into the
// in-memory LRU; corrupted entries (wrong digest or filename) are removed.
func Open(dir string, budgetBytes int64, opts ...Option) (*Cache, error) {
	if dir == "" {
		return nil, errors.New("cache: empty cache_dir")
	}
	if budgetBytes <= 0 {
		return nil, fmt.Errorf("cache: invalid budget %d", budgetBytes)
	}
	c := &Cache{
		root:        dir,
		budgetBytes: budgetBytes,
		logger:      slog.Default().With(slog.String("subsystem", "cache")),
		entries:     map[string]*list.Element{},
		lru:         list.New(),
		pcCnt:       map[string]providerCount{},
	}
	for _, opt := range opts {
		opt(c)
	}
	if c.policy.threshold <= 0 {
		c.policy.threshold = 3
	}
	if c.policy.headroomPct <= 0 {
		c.policy.headroomPct = 5
	}
	if c.policy.providerCountTTL <= 0 {
		c.policy.providerCountTTL = 60 * time.Second
	}
	if c.policy.now == nil {
		c.policy.now = time.Now
	}
	if err := os.MkdirAll(c.tmpDir(), 0o755); err != nil {
		return nil, fmt.Errorf("cache: mkdir tmp: %w", err)
	}
	if err := os.MkdirAll(c.blobsDir(), 0o755); err != nil {
		return nil, fmt.Errorf("cache: mkdir blobs: %w", err)
	}
	if err := c.scan(); err != nil {
		return nil, fmt.Errorf("cache: scan: %w", err)
	}
	// Drop any leftover .partial files from a previous crashed run.
	_ = c.purgeStaleTmp()
	// If we already exceed budget at startup (operator shrank it),
	// drive a sweep through the full §7.4 path (deferral + headroom).
	c.evictIfOver(context.Background())
	return c, nil
}

// Has reports whether d is currently in the cache.
func (c *Cache) Has(_ context.Context, d digest.Digest) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.entries[d.String()]
	return ok, nil
}

// Open returns a reader for d. Returns *ifaces.ErrNotFound on miss.
//
// The returned reader is a direct os.File handle; closing it has no effect
// on the cache. Reading a cache entry promotes it to the front of the LRU.
func (c *Cache) Open(_ context.Context, d digest.Digest) (io.ReadCloser, int64, error) {
	c.mu.Lock()
	el, ok := c.entries[d.String()]
	if !ok {
		c.mu.Unlock()
		if c.metrics.onMiss != nil {
			c.metrics.onMiss()
		}
		return nil, 0, &ifaces.ErrNotFound{Digest: d}
	}
	c.lru.MoveToFront(el)
	size := el.Value.(*entry).size
	c.mu.Unlock()

	f, err := os.Open(c.pathFor(d))
	if err != nil {
		// File vanished underneath us — treat as miss and drop the entry.
		c.mu.Lock()
		if cur, ok := c.entries[d.String()]; ok && cur == el {
			c.lru.Remove(cur)
			c.size -= size
			delete(c.entries, d.String())
		}
		c.mu.Unlock()
		if c.metrics.onMiss != nil {
			c.metrics.onMiss()
		}
		return nil, 0, &ifaces.ErrNotFound{Digest: d}
	}
	if c.metrics.onHit != nil {
		c.metrics.onHit()
	}
	return f, size, nil
}

// Writer opens a digest-verifying writer for d. If d is already present,
// the returned writer's Commit is a no-op and the bytes are discarded —
// the existing entry stays untouched. This makes concurrent pulls for the
// same digest idempotent.
func (c *Cache) Writer(_ context.Context, d digest.Digest) (ifaces.CacheWriter, error) {
	tmpPath, err := c.newTmpPath()
	if err != nil {
		return nil, err
	}
	f, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return nil, fmt.Errorf("cache: create temp: %w", err)
	}
	return &writer{
		cache:   c,
		want:    d,
		pipe:    digestpipe.New(f),
		file:    f,
		tmpPath: tmpPath,
	}, nil
}

// Close flushes any in-memory state. Currently a no-op; reserved for
// future write-ahead state.
func (c *Cache) Close() error { return nil }

// SizeBytes returns the current total byte size of cached content.
func (c *Cache) SizeBytes() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.size
}

// EntryCount returns the number of committed entries.
func (c *Cache) EntryCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries)
}

// Digests returns a snapshot of every digest currently in the cache. Used
// by the startup re-announce path (§Phase 2: re-announce all cached
// digests via dht.Provide on startup).
func (c *Cache) Digests() []digest.Digest {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]digest.Digest, 0, len(c.entries))
	for _, el := range c.entries {
		out = append(out, el.Value.(*entry).digest)
	}
	return out
}

// admit records a freshly-committed entry. Called by writer.Commit while
// holding no locks. Triggers eviction if size > budget or if forced
// headroom has been breached.
func (c *Cache) admit(d digest.Digest, size int64) {
	c.mu.Lock()
	if existing, ok := c.entries[d.String()]; ok {
		// Race: another writer already committed this digest. Promote the
		// existing entry and let the caller delete its temp.
		c.lru.MoveToFront(existing)
		c.mu.Unlock()
		return
	}
	e := &entry{digest: d, size: size}
	el := c.lru.PushFront(e)
	c.entries[d.String()] = el
	c.size += size
	c.mu.Unlock()
	c.evictIfOver(context.Background())
}

// evictIfOver runs the §7.4 eviction policy: LRU with provider-count
// deferral and forced-headroom escape. Callers must NOT hold c.mu.
// At most one sweep runs at a time; concurrent admits are coalesced
// via c.evictMu.
func (c *Cache) evictIfOver(ctx context.Context) {
	c.evictMu.Lock()
	defer c.evictMu.Unlock()

	// visited entries during this sweep — entries we've decided to
	// defer. Avoids re-querying their provider count in a tight loop
	// and guarantees forward progress when every tail candidate
	// defers.
	visited := map[string]struct{}{}

	for {
		// Decide whether we need to evict at all.
		c.mu.Lock()
		overBudget := c.size > c.budgetBytes
		c.mu.Unlock()

		forced := c.forcedHeadroom()
		if !overBudget && !forced {
			return
		}

		// Walk the LRU from oldest to newest, looking for an
		// un-visited candidate.
		c.mu.Lock()
		var cand *entry
		for e := c.lru.Back(); e != nil; e = e.Prev() {
			candidate := e.Value.(*entry)
			if _, seen := visited[candidate.digest.String()]; seen {
				continue
			}
			cand = candidate
			break
		}
		c.mu.Unlock()
		if cand == nil {
			// Every entry deferred; nothing more we can do without
			// the forced-headroom path.
			if overBudget && !forced {
				c.logger.Warn("cache: over budget but every tail entry deferred",
					slog.Int64("size", c.SizeBytes()),
					slog.Int64("budget", c.budgetBytes),
				)
			}
			return
		}

		// Provider-count check, only if we have a callback and we're
		// not in forced mode.
		evict := true
		var pCount int
		if !forced && c.policy.providerCount != nil {
			n, err := c.providerCountCached(ctx, cand.digest)
			pCount = n
			switch {
			case err != nil:
				// DHT failed — defer (safer to keep low-replication
				// content; forced-headroom will pick up slack).
				visited[cand.digest.String()] = struct{}{}
				c.logger.Debug("cache: provider-count failed; deferring",
					slog.String("digest", cand.digest.String()),
					slog.Any("err", err),
				)
				if c.metrics.onDeferredEvict != nil {
					c.metrics.onDeferredEvict()
				}
				continue
			case n < c.policy.threshold:
				visited[cand.digest.String()] = struct{}{}
				evict = false
				if c.metrics.onDeferredEvict != nil {
					c.metrics.onDeferredEvict()
				}
			}
		}
		if !evict {
			continue
		}

		// Actually evict — re-acquire the lock and verify the entry
		// is still the one we picked.
		c.mu.Lock()
		el, ok := c.entries[cand.digest.String()]
		if !ok {
			c.mu.Unlock()
			continue
		}
		current := el.Value.(*entry)
		c.lru.Remove(el)
		delete(c.entries, cand.digest.String())
		c.size -= current.size
		c.mu.Unlock()

		path := c.pathFor(cand.digest)
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			c.logger.Warn("cache: evict remove failed",
				slog.String("digest", cand.digest.String()),
				slog.Any("err", err),
			)
		}
		// Drop any cached provider count so a future re-admit re-queries.
		c.pcMu.Lock()
		delete(c.pcCnt, cand.digest.String())
		c.pcMu.Unlock()

		if forced {
			c.logger.Warn("cache: forced eviction (§7.4 headroom)",
				slog.String("digest", cand.digest.String()),
				slog.Int("provider_count", pCount),
				slog.Int64("bytes", current.size),
				slog.Int64("size_after", c.SizeBytes()),
				slog.Int64("budget", c.budgetBytes),
			)
			if c.metrics.onForcedEviction != nil {
				c.metrics.onForcedEviction()
			}
		} else {
			c.logger.Info("cache: evicted",
				slog.String("digest", cand.digest.String()),
				slog.Int64("bytes", current.size),
				slog.Int64("size_after", c.SizeBytes()),
				slog.Int64("budget", c.budgetBytes),
			)
		}
		if c.metrics.onEvicted != nil {
			c.metrics.onEvicted(current.size)
		}
	}
}

// forcedHeadroom returns true when free disk on the cache volume has
// fallen below the §7.4 headroom floor.
func (c *Cache) forcedHeadroom() bool {
	if c.policy.diskFree == nil || c.policy.headroomPct <= 0 || c.budgetBytes <= 0 {
		return false
	}
	floor := uint64(c.budgetBytes) * uint64(c.policy.headroomPct) / 100
	free, err := c.policy.diskFree()
	if err != nil {
		c.logger.Debug("cache: diskFree failed; ignoring",
			slog.Any("err", err),
		)
		return false
	}
	return free < floor
}

// providerCountCached returns the provider count for d, hitting the
// per-cache local count cache when an entry is within
// providerCountTTL. The DHT callback is invoked WITHOUT c.mu held.
func (c *Cache) providerCountCached(ctx context.Context, d digest.Digest) (int, error) {
	key := d.String()
	now := c.policy.now()
	c.pcMu.Lock()
	if pc, ok := c.pcCnt[key]; ok && now.Sub(pc.at) < c.policy.providerCountTTL {
		c.pcMu.Unlock()
		return pc.count, nil
	}
	c.pcMu.Unlock()

	n, err := c.policy.providerCount(ctx, d)
	if err != nil {
		return 0, err
	}
	c.pcMu.Lock()
	c.pcCnt[key] = providerCount{count: n, at: now}
	c.pcMu.Unlock()
	return n, nil
}

func (c *Cache) blobsDir() string { return filepath.Join(c.root, "blobs", "sha256") }
func (c *Cache) tmpDir() string   { return filepath.Join(c.root, "tmp") }

func (c *Cache) pathFor(d digest.Digest) string {
	hex := d.Hex()
	return filepath.Join(c.blobsDir(), hex[:2], hex)
}

func (c *Cache) newTmpPath() (string, error) {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", err
	}
	return filepath.Join(c.tmpDir(), hex.EncodeToString(buf[:])+".partial"), nil
}

// scan walks blobsDir and builds the in-memory LRU. Entries are seeded in
// mtime order (oldest at the back of the LRU) so a process restart doesn't
// re-promote everything to MRU.
func (c *Cache) scan() error {
	type onDisk struct {
		d     digest.Digest
		size  int64
		mtime int64
	}
	var found []onDisk

	err := filepath.WalkDir(c.blobsDir(), func(path string, dent os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if dent.IsDir() {
			return nil
		}
		name := dent.Name()
		// Filename MUST be 64 lowercase hex chars.
		if len(name) != 64 {
			c.logger.Warn("cache: unexpected file in blobs dir; removing",
				slog.String("path", path))
			_ = os.Remove(path)
			return nil
		}
		d, err := digest.Parse("sha256:" + name)
		if err != nil {
			c.logger.Warn("cache: malformed digest file; removing",
				slog.String("path", path), slog.Any("err", err))
			_ = os.Remove(path)
			return nil
		}
		info, err := dent.Info()
		if err != nil {
			return err
		}
		found = append(found, onDisk{d: d, size: info.Size(), mtime: info.ModTime().UnixNano()})
		return nil
	})
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}

	// Push oldest first so MRU sorting reflects mtime.
	sort.Slice(found, func(i, j int) bool { return found[i].mtime < found[j].mtime })
	for _, f := range found {
		e := &entry{digest: f.d, size: f.size}
		el := c.lru.PushFront(e)
		c.entries[f.d.String()] = el
		c.size += f.size
	}
	return nil
}

func (c *Cache) purgeStaleTmp() error {
	dents, err := os.ReadDir(c.tmpDir())
	if err != nil {
		return err
	}
	for _, dent := range dents {
		if dent.IsDir() {
			continue
		}
		_ = os.Remove(filepath.Join(c.tmpDir(), dent.Name()))
	}
	return nil
}

// writer is the per-call CacheWriter implementation. The digest-tee
// itself lives in internal/digestpipe; this type owns the temp-file
// lifecycle and the atomic rename into the content-addressed tree.
type writer struct {
	cache   *Cache
	want    digest.Digest
	pipe    *digestpipe.Writer
	file    *os.File
	tmpPath string
	closed  bool
}

func (w *writer) Write(p []byte) (int, error) {
	if w.closed {
		return 0, errors.New("cache: write after close")
	}
	return w.pipe.Write(p)
}

func (w *writer) Commit(_ context.Context) error {
	if w.closed {
		return errors.New("cache: commit after close")
	}
	w.closed = true
	if err := w.file.Sync(); err != nil {
		_ = w.file.Close()
		_ = os.Remove(w.tmpPath)
		return fmt.Errorf("cache: fsync: %w", err)
	}
	if err := w.file.Close(); err != nil {
		_ = os.Remove(w.tmpPath)
		return fmt.Errorf("cache: close temp: %w", err)
	}
	if err := w.pipe.Verify(w.want); err != nil {
		_ = os.Remove(w.tmpPath)
		return fmt.Errorf("cache: %w", err)
	}
	final := w.cache.pathFor(w.want)
	if err := os.MkdirAll(filepath.Dir(final), 0o755); err != nil {
		_ = os.Remove(w.tmpPath)
		return fmt.Errorf("cache: mkdir shard: %w", err)
	}
	// If a concurrent writer already committed this digest, the existing
	// final file is byte-identical (digests match). Either remove our temp
	// or replace the existing file; renaming over is safe on POSIX.
	if err := os.Rename(w.tmpPath, final); err != nil {
		_ = os.Remove(w.tmpPath)
		return fmt.Errorf("cache: rename: %w", err)
	}
	w.cache.admit(w.want, w.pipe.Written())
	return nil
}

func (w *writer) Abort(_ context.Context) error {
	if w.closed {
		return nil
	}
	w.closed = true
	_ = w.file.Close()
	if err := os.Remove(w.tmpPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

// Compile-time check.
var _ ifaces.Cache = (*Cache)(nil)
