package config

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestDefaultsValidateAfterMinimalUpstream(t *testing.T) {
	c := NewDefault()
	// Defaults intentionally have no upstream registries — operator must
	// supply at least one. Seed one and re-validate.
	c.UpstreamRegistries = []UpstreamRegistry{
		{Name: "registry.example.com", Endpoint: "https://registry.example.com"},
	}
	if err := c.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
}

func TestValidate_RequiresUpstream(t *testing.T) {
	c := NewDefault()
	err := c.Validate()
	if err == nil || !strings.Contains(err.Error(), "upstream_registries") {
		t.Fatalf("want upstream_registries error, got %v", err)
	}
}

func TestValidate_MirrorListenMustBeLoopback(t *testing.T) {
	c := NewDefault()
	c.UpstreamRegistries = []UpstreamRegistry{{Name: "r", Endpoint: "https://r"}}
	c.MirrorListen = "0.0.0.0:5000"
	err := c.Validate()
	if err == nil || !strings.Contains(err.Error(), "loopback") {
		t.Fatalf("want loopback error, got %v", err)
	}
}

// MirrorBindAllowNonLoopback is the operator opt-in for deployments that
// rely on hostPort + hostIP=127.0.0.1 to keep the mirror node-local while
// still binding 0.0.0.0 inside the pod (so kube-proxy's DNAT into the pod
// network reaches the listener). When set, validation must accept the
// non-loopback bind.
func TestValidate_MirrorListenAllowNonLoopbackOptIn(t *testing.T) {
	c := NewDefault()
	c.UpstreamRegistries = []UpstreamRegistry{{Name: "r", Endpoint: "https://r"}}
	c.MirrorListen = "0.0.0.0:5000"
	c.MirrorBindAllowNonLoopback = true
	if err := c.Validate(); err != nil {
		t.Fatalf("validate (opt-in): %v", err)
	}
}

func TestValidate_DuplicateUpstreamName(t *testing.T) {
	c := NewDefault()
	c.UpstreamRegistries = []UpstreamRegistry{
		{Name: "r", Endpoint: "https://r"},
		{Name: "r", Endpoint: "https://r2"},
	}
	err := c.Validate()
	if err == nil || !strings.Contains(err.Error(), "duplicates") {
		t.Fatalf("want duplicates error, got %v", err)
	}
}

func TestValidate_HRWScope(t *testing.T) {
	c := NewDefault()
	c.UpstreamRegistries = []UpstreamRegistry{{Name: "r", Endpoint: "https://r"}}
	c.HRWTopologyScope = "rack"
	err := c.Validate()
	if err == nil || !strings.Contains(err.Error(), "hrw_topology_scope") {
		t.Fatalf("want hrw_topology_scope error, got %v", err)
	}
}

func TestLoadYAML_KnownFieldsOnly(t *testing.T) {
	c := NewDefault()
	in := []byte("totally_unknown_field: 1\n")
	if err := c.LoadYAML(bytes.NewReader(in)); err == nil {
		t.Fatal("expected unknown-field error")
	}
}

func TestLoadYAML_Roundtrip(t *testing.T) {
	c := NewDefault()
	in := []byte(`
cache_dir: /tmp/gantry-cache
cache_budget_bytes: 12345
upstream_registries:
  - name: registry.example.com
    endpoint: https://registry.example.com
    credentials_path: /etc/gantry/creds.txt
hrw_k: 5
nf5_jitter_base: 7s
log_level: debug
`)
	if err := c.LoadYAML(bytes.NewReader(in)); err != nil {
		t.Fatalf("LoadYAML: %v", err)
	}
	if c.CacheDir != "/tmp/gantry-cache" || c.CacheBudgetBytes != 12345 {
		t.Errorf("scalar overlay failed: %+v", c)
	}
	if len(c.UpstreamRegistries) != 1 || c.UpstreamRegistries[0].Name != "registry.example.com" {
		t.Errorf("upstream overlay failed: %+v", c.UpstreamRegistries)
	}
	if c.HRWK != 5 {
		t.Errorf("HRWK = %d, want 5", c.HRWK)
	}
	if c.NF5JitterBase != 7*time.Second {
		t.Errorf("NF5JitterBase = %v, want 7s", c.NF5JitterBase)
	}
	if c.LogLevel != "debug" {
		t.Errorf("LogLevel = %q, want debug", c.LogLevel)
	}
}

func TestLoadEnv(t *testing.T) {
	c := NewDefault()
	env := map[string]string{
		"GANTRY_CACHE_DIR":          "/etc/gantry/cache",
		"GANTRY_CACHE_BUDGET_BYTES": "7777",
		"GANTRY_HRW_K":              "9",
		"GANTRY_NF5_JITTER_BASE":    "4500ms",
	}
	getenv := func(k string) string { return env[k] }
	if err := c.LoadEnv(getenv); err != nil {
		t.Fatalf("LoadEnv: %v", err)
	}
	if c.CacheDir != "/etc/gantry/cache" {
		t.Errorf("CacheDir = %q", c.CacheDir)
	}
	if c.CacheBudgetBytes != 7777 {
		t.Errorf("CacheBudgetBytes = %d", c.CacheBudgetBytes)
	}
	if c.HRWK != 9 {
		t.Errorf("HRWK = %d", c.HRWK)
	}
	if c.NF5JitterBase != 4500*time.Millisecond {
		t.Errorf("NF5JitterBase = %v", c.NF5JitterBase)
	}
}

func TestLoadEnv_RejectsBadDuration(t *testing.T) {
	c := NewDefault()
	getenv := func(k string) string {
		if k == "GANTRY_NF5_JITTER_BASE" {
			return "not-a-duration"
		}
		return ""
	}
	if err := c.LoadEnv(getenv); err == nil {
		t.Fatal("expected duration parse error")
	}
}

func TestResolveUpstream(t *testing.T) {
	c := NewDefault()
	c.UpstreamRegistries = []UpstreamRegistry{
		{Name: "registry.example.com", Endpoint: "https://registry.example.com"},
		{Name: "ghcr.io", Endpoint: "https://ghcr.io", NSAlias: "github"},
	}
	if _, ok := c.ResolveUpstream("registry.example.com"); !ok {
		t.Error("ResolveUpstream(name) miss")
	}
	if _, ok := c.ResolveUpstream("github"); !ok {
		t.Error("ResolveUpstream(alias) miss")
	}
	if _, ok := c.ResolveUpstream("unknown"); ok {
		t.Error("ResolveUpstream(unknown) hit")
	}
}
