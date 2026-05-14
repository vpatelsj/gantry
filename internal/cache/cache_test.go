package cache_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gantry/gantry/internal/cache"
	"github.com/gantry/gantry/internal/digest"
	"github.com/gantry/gantry/internal/ifaces"
)

func digestOf(b []byte) digest.Digest {
	sum := sha256.Sum256(b)
	d, err := digest.Parse("sha256:" + hex.EncodeToString(sum[:]))
	if err != nil {
		panic(err)
	}
	return d
}

func writeBlob(t *testing.T, c *cache.Cache, body []byte) digest.Digest {
	t.Helper()
	d := digestOf(body)
	w, err := c.Writer(context.Background(), d)
	if err != nil {
		t.Fatalf("Writer: %v", err)
	}
	if _, err := w.Write(body); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Commit(context.Background()); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	return d
}

func TestRoundTrip(t *testing.T) {
	dir := t.TempDir()
	c, err := cache.Open(dir, 1<<20)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer c.Close()

	body := []byte("hello world")
	d := writeBlob(t, c, body)

	ok, err := c.Has(context.Background(), d)
	if err != nil || !ok {
		t.Fatalf("Has after Commit: ok=%v err=%v", ok, err)
	}

	rc, size, err := c.Open(context.Background(), d)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer rc.Close()
	if size != int64(len(body)) {
		t.Errorf("size = %d, want %d", size, len(body))
	}
	got, _ := io.ReadAll(rc)
	if string(got) != string(body) {
		t.Errorf("read = %q, want %q", got, body)
	}
}

func TestDigestMismatchRejected(t *testing.T) {
	dir := t.TempDir()
	c, err := cache.Open(dir, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	wrong := digestOf([]byte("nope"))
	w, err := c.Writer(context.Background(), wrong)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("real bytes")); err != nil {
		t.Fatal(err)
	}
	err = w.Commit(context.Background())
	if err == nil || !strings.Contains(err.Error(), "digest mismatch") {
		t.Fatalf("Commit: want digest mismatch, got %v", err)
	}

	// Final file MUST NOT exist after a failed commit.
	hexs := wrong.Hex()
	final := filepath.Join(dir, "blobs", "sha256", hexs[:2], hexs)
	if _, err := os.Stat(final); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("final file exists after mismatch: %v", err)
	}
}

func TestOpenMissReturnsErrNotFound(t *testing.T) {
	dir := t.TempDir()
	c, err := cache.Open(dir, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	d := digestOf([]byte("never written"))
	_, _, err = c.Open(context.Background(), d)
	var enf *ifaces.ErrNotFound
	if !errors.As(err, &enf) {
		t.Fatalf("Open miss: want *ErrNotFound, got %T %v", err, err)
	}
}

func TestAbortRemovesTemp(t *testing.T) {
	dir := t.TempDir()
	c, err := cache.Open(dir, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	d := digestOf([]byte("payload"))
	w, _ := c.Writer(context.Background(), d)
	_, _ = w.Write([]byte("partial"))
	if err := w.Abort(context.Background()); err != nil {
		t.Fatalf("Abort: %v", err)
	}

	dents, _ := os.ReadDir(filepath.Join(dir, "tmp"))
	for _, dent := range dents {
		if strings.HasSuffix(dent.Name(), ".partial") {
			t.Errorf("temp file still present after Abort: %s", dent.Name())
		}
	}
}

func TestLRUEviction(t *testing.T) {
	dir := t.TempDir()
	// Budget is 30 bytes: third 11-byte blob should evict the first.
	c, err := cache.Open(dir, 30)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	d1 := writeBlob(t, c, []byte("aaaaaaaaaaa")) // 11 bytes
	d2 := writeBlob(t, c, []byte("bbbbbbbbbbb")) // 11 bytes; total 22
	d3 := writeBlob(t, c, []byte("ccccccccccc")) // 11 bytes; total would be 33; evict d1

	if got := c.SizeBytes(); got > 30 {
		t.Errorf("SizeBytes = %d, want <= 30", got)
	}
	if ok, _ := c.Has(context.Background(), d1); ok {
		t.Error("d1 should have been evicted")
	}
	if ok, _ := c.Has(context.Background(), d2); !ok {
		t.Error("d2 must still be present")
	}
	if ok, _ := c.Has(context.Background(), d3); !ok {
		t.Error("d3 must be present")
	}
}

func TestLRUEvictsLeastRecentlyUsed(t *testing.T) {
	dir := t.TempDir()
	c, err := cache.Open(dir, 30)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	d1 := writeBlob(t, c, []byte("aaaaaaaaaaa"))
	d2 := writeBlob(t, c, []byte("bbbbbbbbbbb"))
	// Touch d1 so it becomes MRU; d2 is now LRU.
	rc, _, err := c.Open(context.Background(), d1)
	if err != nil {
		t.Fatal(err)
	}
	rc.Close()

	d3 := writeBlob(t, c, []byte("ccccccccccc"))
	if ok, _ := c.Has(context.Background(), d1); !ok {
		t.Error("d1 must remain (touched after d2)")
	}
	if ok, _ := c.Has(context.Background(), d2); ok {
		t.Error("d2 should have been evicted (LRU)")
	}
	if ok, _ := c.Has(context.Background(), d3); !ok {
		t.Error("d3 must be present")
	}
}

func TestReopenRestoresContents(t *testing.T) {
	dir := t.TempDir()
	c1, err := cache.Open(dir, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	d := writeBlob(t, c1, []byte("survive me"))
	c1.Close()

	c2, err := cache.Open(dir, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	if ok, _ := c2.Has(context.Background(), d); !ok {
		t.Errorf("digest %s missing after reopen", d)
	}
	if c2.EntryCount() != 1 {
		t.Errorf("EntryCount = %d, want 1", c2.EntryCount())
	}
}

func TestStaleTmpPurged(t *testing.T) {
	dir := t.TempDir()
	// Pre-seed a stale .partial.
	if err := os.MkdirAll(filepath.Join(dir, "tmp"), 0o755); err != nil {
		t.Fatal(err)
	}
	stale := filepath.Join(dir, "tmp", "abc.partial")
	if err := os.WriteFile(stale, []byte("junk"), 0o644); err != nil {
		t.Fatal(err)
	}

	c, err := cache.Open(dir, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := os.Stat(stale); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("stale .partial not purged: %v", err)
	}
}

func TestDoubleCommitIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	c, err := cache.Open(dir, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	body := []byte("idempotent")
	d := writeBlob(t, c, body)
	// Second writer for the same digest.
	d2 := writeBlob(t, c, body)
	if d != d2 {
		t.Fatalf("digests differ: %s vs %s", d, d2)
	}
	if c.EntryCount() != 1 {
		t.Errorf("EntryCount = %d, want 1", c.EntryCount())
	}
	if c.SizeBytes() != int64(len(body)) {
		t.Errorf("SizeBytes = %d, want %d (no double-count)", c.SizeBytes(), len(body))
	}
}
