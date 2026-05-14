// Tests for the §7.4 cache eviction policy: provider-count deferral
// and forced-headroom escape.

package cache_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gantry/gantry/internal/cache"
	"github.com/gantry/gantry/internal/digest"
)

// fakeDiskFree returns whatever free is loaded into the pointer.
// Lets a test toggle "disk full" before triggering the next admit.
func fakeDiskFree(free *uint64) func() (uint64, error) {
	return func() (uint64, error) { return atomic.LoadUint64(free), nil }
}

func TestEviction_DefersLowProviderCount(t *testing.T) {
	dir := t.TempDir()
	// Provider count is always 1 for every digest → < threshold (3),
	// so eviction should be deferred and the cache should grow past
	// budget without forced headroom triggering.
	var defers int
	c, err := cache.Open(dir, 30,
		cache.WithEviction(cache.EvictionPolicy{
			ProviderCount: func(_ context.Context, _ digest.Digest) (int, error) {
				return 1, nil
			},
			Threshold:        3,
			OnDeferredEvict:  func() { defers++ },
			ProviderCountTTL: time.Hour, // memoise
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	d1 := writeBlob(t, c, []byte("aaaaaaaaaaa"))
	d2 := writeBlob(t, c, []byte("bbbbbbbbbbb"))
	d3 := writeBlob(t, c, []byte("ccccccccccc")) // 33 bytes total; would normally evict

	if c.SizeBytes() != 33 {
		t.Errorf("SizeBytes = %d; want 33 (no eviction under deferral)", c.SizeBytes())
	}
	for _, d := range []digest.Digest{d1, d2, d3} {
		if ok, _ := c.Has(context.Background(), d); !ok {
			t.Errorf("digest %s should remain under deferral", d)
		}
	}
	if defers == 0 {
		t.Errorf("expected at least one deferred-eviction event; got 0")
	}
}

func TestEviction_NoDeferralWhenAtOrAboveThreshold(t *testing.T) {
	dir := t.TempDir()
	// All entries have 3 providers (== threshold). Spec defers only
	// when count < threshold, so eviction must proceed normally.
	c, err := cache.Open(dir, 30,
		cache.WithEviction(cache.EvictionPolicy{
			ProviderCount: func(_ context.Context, _ digest.Digest) (int, error) {
				return 3, nil
			},
			Threshold:        3,
			ProviderCountTTL: time.Hour,
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	d1 := writeBlob(t, c, []byte("aaaaaaaaaaa"))
	_ = writeBlob(t, c, []byte("bbbbbbbbbbb"))
	_ = writeBlob(t, c, []byte("ccccccccccc"))
	if c.SizeBytes() > 30 {
		t.Errorf("SizeBytes = %d; want <= 30", c.SizeBytes())
	}
	if ok, _ := c.Has(context.Background(), d1); ok {
		t.Errorf("d1 should have been evicted (provider count meets threshold)")
	}
}

func TestEviction_ForcedHeadroomBypassesDeferral(t *testing.T) {
	dir := t.TempDir()
	// budget=1000, headroom=5% → floor=50. Start with free=0 to force
	// eviction; after the first eviction, simulate the OS freeing up
	// disk so the second admit doesn't keep evicting.
	var free uint64 = 0
	var forced int
	c, err := cache.Open(dir, 1000,
		cache.WithEviction(cache.EvictionPolicy{
			ProviderCount: func(_ context.Context, _ digest.Digest) (int, error) {
				return 1, nil // would normally defer
			},
			Threshold:   3,
			HeadroomPct: 5,
			DiskFree:    fakeDiskFree(&free),
			OnForcedEviction: func() {
				forced++
				// After the first forced eviction, disk pressure is
				// relieved (simulating real fs reclaiming the freed
				// blocks).
				atomic.StoreUint64(&free, 1000)
			},
			ProviderCountTTL: time.Hour,
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	d1 := writeBlob(t, c, []byte("aaaaaaaaaa"))
	d2 := writeBlob(t, c, []byte("bbbbbbbbbb"))

	if forced == 0 {
		t.Fatalf("expected at least one forced-eviction event under no-free-disk; got 0")
	}
	// d1 was admitted while free=0 → forced-eviction loop drained
	// the cache (d1 was the only entry). The subsequent admit of d2
	// runs with free=1000 (post-callback) so d2 must remain.
	if ok, _ := c.Has(context.Background(), d1); ok {
		t.Errorf("d1 should have been forcibly evicted")
	}
	if ok, _ := c.Has(context.Background(), d2); !ok {
		t.Errorf("d2 must still be present after disk-free recovered")
	}
}

func TestEviction_ProviderCountCachedWithinTTL(t *testing.T) {
	dir := t.TempDir()
	// Count the calls into the DHT callback.
	var calls int32
	c, err := cache.Open(dir, 30,
		cache.WithEviction(cache.EvictionPolicy{
			ProviderCount: func(_ context.Context, _ digest.Digest) (int, error) {
				atomic.AddInt32(&calls, 1)
				return 1, nil
			},
			Threshold:        3,
			ProviderCountTTL: time.Hour, // long TTL → second admit reuses
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// Three admits in a row; the first walks the (empty/short) LRU
	// without deferring; subsequent admits walk over deferred entries
	// but should hit the count cache for repeats.
	_ = writeBlob(t, c, []byte("aaaaaaaaaaa"))
	_ = writeBlob(t, c, []byte("bbbbbbbbbbb"))
	_ = writeBlob(t, c, []byte("ccccccccccc"))
	_ = writeBlob(t, c, []byte("ddddddddddd"))

	// Each of d1..d4 is visited at most once during eviction sweeps
	// thanks to the count cache. Without memoisation the count would
	// be 3 + 4 = 7 calls (one per visit in each sweep). Loose bound:
	// <= 4 captures the per-digest memoisation while remaining
	// resilient to minor implementation re-orderings.
	if got := atomic.LoadInt32(&calls); got > 4 {
		t.Errorf("ProviderCount calls = %d; want <= 4 (count cache should memoise)", got)
	}
}

func TestEviction_DHTErrorDefers(t *testing.T) {
	dir := t.TempDir()
	// Provider-count callback fails; entries should be deferred but
	// the cache must keep accepting writes (deferral != failure).
	c, err := cache.Open(dir, 30,
		cache.WithEviction(cache.EvictionPolicy{
			ProviderCount: func(_ context.Context, _ digest.Digest) (int, error) {
				return 0, errors.New("dht down")
			},
			Threshold:        3,
			ProviderCountTTL: time.Hour,
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	_ = writeBlob(t, c, []byte("aaaaaaaaaaa"))
	_ = writeBlob(t, c, []byte("bbbbbbbbbbb"))
	_ = writeBlob(t, c, []byte("ccccccccccc"))
	// All three should remain — deferral protects content when the
	// DHT is unreachable.
	if c.EntryCount() != 3 {
		t.Errorf("EntryCount = %d; want 3 (DHT errors must defer, not evict)", c.EntryCount())
	}
}

func TestEviction_ForcedHeadroomMetricCounts(t *testing.T) {
	dir := t.TempDir()
	var free uint64 = 0
	var forced int32
	c, err := cache.Open(dir, 100,
		cache.WithEviction(cache.EvictionPolicy{
			ProviderCount: func(_ context.Context, _ digest.Digest) (int, error) {
				return 1, nil
			},
			Threshold:        3,
			HeadroomPct:      5,
			DiskFree:         fakeDiskFree(&free),
			OnForcedEviction: func() { atomic.AddInt32(&forced, 1) },
			ProviderCountTTL: time.Hour,
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// Write five blobs back-to-back; each admit re-checks headroom
	// and forces at least one eviction.
	for i := 0; i < 5; i++ {
		body := make([]byte, 10)
		for j := range body {
			body[j] = byte('a' + i)
		}
		_ = writeBlob(t, c, body)
	}
	if atomic.LoadInt32(&forced) == 0 {
		t.Errorf("expected forced-eviction events when free disk is 0; got 0")
	}
}

func TestEviction_NoDeferralWhenCallbackNil(t *testing.T) {
	// Backward-compatibility: a cache opened without WithEviction
	// must keep simple-LRU semantics (no deferral, no headroom logic).
	dir := t.TempDir()
	c, err := cache.Open(dir, 30)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	d1 := writeBlob(t, c, []byte("aaaaaaaaaaa"))
	_ = writeBlob(t, c, []byte("bbbbbbbbbbb"))
	_ = writeBlob(t, c, []byte("ccccccccccc"))
	if ok, _ := c.Has(context.Background(), d1); ok {
		t.Errorf("simple-LRU mode: d1 should have been evicted")
	}
}
