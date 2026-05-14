// Package cache is Gantry's on-disk content-addressed store.
//
// Layout:
//
//	{CacheDir}/blobs/sha256/<ab>/<hex>      committed blobs (sharded by first 2 hex chars)
//	{CacheDir}/tmp/<random>.partial         staging files for in-progress writes
//
// Phase 1 semantics:
//
//   - Writes go through a digest-verifying CacheWriter. Commit renames the
//     temp file into place atomically. The expected digest is recomputed
//     incrementally during Write so Commit's hash check is constant-time
//     and the entry never appears under the wrong digest.
//
//   - Eviction is a simple bounded LRU at cfg.CacheBudgetBytes. The §7.4
//     provider-count deferral lands in Phase 6 once DHT.FindProviders is
//     real; Phase 1's strict LRU is correct because there are no peers to
//     defer for yet.
//
//   - On startup, Open walks the on-disk content tree and seeds the LRU
//     order by mtime (oldest first) so the in-memory size accounting
//     matches what's actually on disk.
package cache

import (
	"container/list"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/gantry/gantry/internal/digest"
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

	mu      sync.Mutex
	size    int64
	entries map[string]*list.Element // key: digest.String()
	lru     *list.List               // front = MRU, back = LRU
}

type entry struct {
	digest digest.Digest
	size   int64
}

// metricsHooks lets the cache emit counters without importing the metrics
// package directly (keeps the cache testable without Prometheus). Each
// hook is allowed to be nil.
type metricsHooks struct {
	onHit     func()
	onMiss    func()
	onEvicted func(bytes int64)
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
	}
	for _, opt := range opts {
		opt(c)
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
		hasher:  sha256.New(),
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
// holding no locks. Triggers eviction if size > budget.
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
	c.evictIfOverLocked()
	c.mu.Unlock()
}

// evictIfOverLocked is called with c.mu held. Drops LRU-tail entries until
// c.size <= c.budgetBytes.
func (c *Cache) evictIfOverLocked() {
	for c.size > c.budgetBytes {
		el := c.lru.Back()
		if el == nil {
			return
		}
		e := el.Value.(*entry)
		c.lru.Remove(el)
		delete(c.entries, e.digest.String())
		c.size -= e.size
		path := c.pathFor(e.digest)
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			c.logger.Warn("cache: evict remove failed",
				slog.String("digest", e.digest.String()),
				slog.Any("err", err),
			)
		}
		c.logger.Warn("cache: evicted",
			slog.String("digest", e.digest.String()),
			slog.Int64("bytes", e.size),
			slog.Int64("size_after", c.size),
			slog.Int64("budget", c.budgetBytes),
		)
		if c.metrics.onEvicted != nil {
			c.metrics.onEvicted(e.size)
		}
	}
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
	// If we already exceed budget at startup (operator shrank it), evict.
	c.evictIfOverLocked()
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

// writer is the per-call CacheWriter implementation.
type writer struct {
	cache   *Cache
	want    digest.Digest
	hasher  hash.Hash
	file    *os.File
	tmpPath string
	written int64
	closed  bool
}

func (w *writer) Write(p []byte) (int, error) {
	if w.closed {
		return 0, errors.New("cache: write after close")
	}
	n, err := w.file.Write(p)
	if n > 0 {
		w.hasher.Write(p[:n])
		w.written += int64(n)
	}
	return n, err
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
	got := hex.EncodeToString(w.hasher.Sum(nil))
	if got != w.want.Hex() {
		_ = os.Remove(w.tmpPath)
		return fmt.Errorf("cache: digest mismatch: want %s, got sha256:%s", w.want.String(), got)
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
	w.cache.admit(w.want, w.written)
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
