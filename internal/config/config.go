// Package config is the single source of truth for every operator-tunable
// knob the Gantry agent exposes.
//
// The design docs enumerate configuration in many places (§7.4 cache, §7.7
// NF5 / DHT health, §5.8 origin-failure circuit breaker, §7.1 hosts.toml /
// upstream registries, §8 open questions). This package collects them into
// a single Config struct with field-level documentation pointing at the
// design-doc citation.
//
// Sources, in increasing precedence:
//
//  1. Built-in defaults from NewDefault().
//  2. YAML file at --config=PATH (optional).
//  3. Environment variables prefixed GANTRY_.
//  4. Command-line flags.
//
// Later sources win. Validate() runs after all sources have been merged and
// returns an aggregate of every problem found, not just the first.
package config

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the typed configuration surface.
//
// Every field carries a yaml/json tag matching the file/env name and a comment
// citing the design-doc section it derives from. Defaults are set by
// NewDefault(); see Validate() for hard correctness constraints.
type Config struct {
	// ---------- Listeners ----------

	// MirrorListen is the loopback address for containerd's mirror endpoint
	// (detailed-design.md §4.1, §7.1). MUST be loopback-only.
	MirrorListen string `yaml:"mirror_listen"`

	// TransferListen is the peer-facing HTTP/2 endpoint (§4.4). NetworkPolicy
	// restricts inter-node visibility; the agent itself binds 0.0.0.0.
	TransferListen string `yaml:"transfer_listen"`

	// MetricsListen is the Prometheus scrape endpoint (§7.6).
	MetricsListen string `yaml:"metrics_listen"`

	// Libp2pListen is the multiaddr(s) the libp2p host advertises (§7.2).
	// Empty means "use libp2p defaults" and pick at random.
	Libp2pListen []string `yaml:"libp2p_listen"`

	// Libp2pIdentityPath is the on-disk path of the persisted libp2p key
	// (§7.2). Lost identity is not catastrophic; old DHT records age out.
	Libp2pIdentityPath string `yaml:"libp2p_identity_path"`

	// Libp2pBootstrapPeers is an optional list of static multiaddrs to seed
	// the libp2p host's connection set on startup. In production these are
	// usually discovered via the K8s informer (§7.2) so this field defaults
	// to empty; tests and small clusters can use it directly.
	Libp2pBootstrapPeers []string `yaml:"libp2p_bootstrap_peers"`

	// ---------- Cluster membership (§7.3) ----------

	// NodeName is the Kubernetes node this agent runs on. Sourced via the
	// Downward API (env spec.nodeName) into GANTRY_NODE_NAME. Used as the
	// stable HRW NodeID and as the join key against the Node informer for
	// zone resolution.
	NodeName string `yaml:"node_name"`

	// MembersNamespace restricts the Pod informer to a single namespace.
	// Empty means cluster-wide (typical for Gantry as a privileged DaemonSet).
	MembersNamespace string `yaml:"members_namespace"`

	// MembersLabelSelector is the K8s label selector that identifies Gantry
	// DaemonSet pods. Used to find peer agents (§7.3). Default matches the
	// canonical app.kubernetes.io label.
	MembersLabelSelector string `yaml:"members_label_selector"`

	// MembersKubeconfig is an optional path to a kubeconfig file. Empty
	// means in-cluster service-account discovery (the production path).
	MembersKubeconfig string `yaml:"members_kubeconfig"`

	// ---------- Cache ----------

	// CacheDir is the hostPath root for the content store (§4.1, §7.4).
	CacheDir string `yaml:"cache_dir"`

	// CacheBudgetBytes is the soft cap on local cache size (§7.4 default
	// 50 GB).
	CacheBudgetBytes int64 `yaml:"cache_budget_bytes"`

	// CacheForcedEvictionHeadroomPct is the §7.4 "headroom ceiling" that
	// forces eviction regardless of provider count. Expressed as integer
	// percent of CacheBudgetBytes; default 5.
	CacheForcedEvictionHeadroomPct int `yaml:"cache_forced_eviction_headroom_pct"`

	// EvictionProviderCountThreshold is the §7.4 deferral threshold:
	// eviction is deferred when the local node is one of fewer than N
	// providers. Default 3; §8 open question.
	EvictionProviderCountThreshold int `yaml:"eviction_provider_count_threshold"`

	// ---------- Upstream registries ----------

	// UpstreamRegistries enumerates every OCI registry the agent mirrors
	// (§7.1, §7.3). The agent rejects requests whose ?ns= does not match
	// one of these once more than one is configured.
	UpstreamRegistries []UpstreamRegistry `yaml:"upstream_registries"`

	// ---------- HRW / coordination ----------

	// HRWK is the top-K size for HRW probe (§5.2 step 3 default 3; §8
	// open question).
	HRWK int `yaml:"hrw_k"`

	// HRWTopologyScope selects "cluster" (HRW over all nodes) or "zone"
	// (HRW within the requester's zone) — §4.3 / §8 open question.
	HRWTopologyScope string `yaml:"hrw_topology_scope"`

	// ZoneLabelKey is the Kubernetes node label that identifies the zone
	// when HRWTopologyScope == "zone". Default
	// `topology.kubernetes.io/zone` (§7.3).
	ZoneLabelKey string `yaml:"zone_label_key"`

	// ---------- DHT / NF5 ----------

	// NF5JitterBase is the base delay in the NF5 jitter window
	// `[0, base * ln(N))` (§7.7 default 3 s).
	NF5JitterBase time.Duration `yaml:"nf5_jitter_base"`

	// NF5PerNodeRateLimit is the per-node direct-origin fallback rate
	// (token bucket, fallbacks/minute; §7.7 default 2).
	NF5PerNodeRateLimit int `yaml:"nf5_per_node_rate_limit"`

	// BootstrapWindow is the time after startup during which DHT-empty is
	// not trusted as cold-start evidence (§7.7 default 30 s).
	BootstrapWindow time.Duration `yaml:"bootstrap_window"`

	// BootstrapRoutingTablePct is the routing-table-size threshold that
	// supersedes BootstrapWindow once met (§7.7 default 25%).
	BootstrapRoutingTablePct int `yaml:"bootstrap_routing_table_pct"`

	// TopKExpansionFactorDegraded is the multiplier applied to HRWK when
	// expanding top-K under Degraded health (§5.2 step 5 / §7.7 default 2).
	TopKExpansionFactorDegraded int `yaml:"topk_expansion_factor_degraded"`

	// ---------- Origin-failure circuit breaker (§5.8) ----------

	OriginFailureCooldownInitial    time.Duration `yaml:"origin_failure_cooldown_initial"`
	OriginFailureCooldownMax        time.Duration `yaml:"origin_failure_cooldown_max"`
	OriginFailureCooldownMultiplier int           `yaml:"origin_failure_cooldown_multiplier"`
	OriginFailureHonorWindowCap     time.Duration `yaml:"origin_failure_honor_window_cap"`

	// OriginFailureClassesTrustedClusterWide controls which §5.8 failure
	// classes are propagated cluster-wide as 5xx-immediate (default
	// {auth, not_found, rate_limited}; `transient` is honored locally
	// only).
	OriginFailureClassesTrustedClusterWide []string `yaml:"origin_failure_classes_trusted_cluster_wide"`

	// ---------- Logging ----------

	// LogLevel is one of "debug", "info", "warn", "error".
	LogLevel string `yaml:"log_level"`

	// LogFormat is "json" (production) or "text" (development).
	LogFormat string `yaml:"log_format"`
}

// UpstreamRegistry describes one OCI registry the agent mirrors.
type UpstreamRegistry struct {
	// Name is the canonical identifier (used as the ?ns= value from
	// containerd and as the lookup key for credentials).
	Name string `yaml:"name"`

	// Endpoint is the HTTPS URL of the registry, e.g.
	// "https://registry.example.com".
	Endpoint string `yaml:"endpoint"`

	// CredentialsPath is a hostPath-mounted file containing the registry
	// credentials. Format: "username:password" (or "_json_key:<json>" for
	// the well-known GCR pattern). Empty means anonymous pulls.
	CredentialsPath string `yaml:"credentials_path"`

	// NSAlias lets containerd's ?ns= use a different name than Name.
	// Empty means ?ns= must equal Name.
	NSAlias string `yaml:"ns_alias"`
}

// NewDefault returns a Config populated with the design-doc defaults.
// All fields are set; Validate() against this MUST pass.
func NewDefault() *Config {
	return &Config{
		MirrorListen:       "127.0.0.1:5000",
		TransferListen:     "0.0.0.0:5001",
		MetricsListen:      "127.0.0.1:9095",
		Libp2pListen:       nil,
		Libp2pIdentityPath: "/var/lib/gantry/libp2p.key",

		NodeName:             "",
		MembersNamespace:     "",
		MembersLabelSelector: "app.kubernetes.io/name=gantry",
		MembersKubeconfig:    "",

		CacheDir:                       "/var/lib/gantry/cache",
		CacheBudgetBytes:               50 * 1024 * 1024 * 1024, // 50 GiB (§7.4)
		CacheForcedEvictionHeadroomPct: 5,
		EvictionProviderCountThreshold: 3,

		UpstreamRegistries: nil,

		HRWK:             3,
		HRWTopologyScope: "cluster",
		ZoneLabelKey:     "topology.kubernetes.io/zone",

		NF5JitterBase:               3 * time.Second,
		NF5PerNodeRateLimit:         2,
		BootstrapWindow:             30 * time.Second,
		BootstrapRoutingTablePct:    25,
		TopKExpansionFactorDegraded: 2,

		OriginFailureCooldownInitial:    10 * time.Second,
		OriginFailureCooldownMax:        10 * time.Minute,
		OriginFailureCooldownMultiplier: 3,
		OriginFailureHonorWindowCap:     30 * time.Second,
		OriginFailureClassesTrustedClusterWide: []string{
			"auth", "not_found", "rate_limited",
		},

		LogLevel:  "info",
		LogFormat: "json",
	}
}

// LoadYAML overlays a YAML document onto c. Unknown fields are an error so
// typos in config files don't silently no-op.
func (c *Config) LoadYAML(r io.Reader) error {
	dec := yaml.NewDecoder(r)
	dec.KnownFields(true)
	return dec.Decode(c)
}

// LoadEnv overlays environment variables of the form GANTRY_<UPPER_SNAKE>.
// Returns a multi-error of any parse failures encountered.
//
// Only scalar fields are overlaid here; list fields (UpstreamRegistries,
// Libp2pListen, OriginFailureClassesTrustedClusterWide) are file-only by
// design — env vars are an awkward shape for them.
func (c *Config) LoadEnv(env func(string) string) error {
	var errs []error
	setStr := func(key string, dst *string) {
		if v, ok := lookup(env, key); ok {
			*dst = v
		}
	}
	setInt := func(key string, dst *int) {
		if v, ok := lookup(env, key); ok {
			n, err := strconv.Atoi(v)
			if err != nil {
				errs = append(errs, fmt.Errorf("env GANTRY_%s: %w", key, err))
				return
			}
			*dst = n
		}
	}
	setInt64 := func(key string, dst *int64) {
		if v, ok := lookup(env, key); ok {
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				errs = append(errs, fmt.Errorf("env GANTRY_%s: %w", key, err))
				return
			}
			*dst = n
		}
	}
	setDur := func(key string, dst *time.Duration) {
		if v, ok := lookup(env, key); ok {
			d, err := time.ParseDuration(v)
			if err != nil {
				errs = append(errs, fmt.Errorf("env GANTRY_%s: %w", key, err))
				return
			}
			*dst = d
		}
	}

	setStr("MIRROR_LISTEN", &c.MirrorListen)
	setStr("TRANSFER_LISTEN", &c.TransferListen)
	setStr("METRICS_LISTEN", &c.MetricsListen)
	setStr("LIBP2P_IDENTITY_PATH", &c.Libp2pIdentityPath)

	setStr("NODE_NAME", &c.NodeName)
	setStr("MEMBERS_NAMESPACE", &c.MembersNamespace)
	setStr("MEMBERS_LABEL_SELECTOR", &c.MembersLabelSelector)
	setStr("MEMBERS_KUBECONFIG", &c.MembersKubeconfig)

	setStr("CACHE_DIR", &c.CacheDir)
	setInt64("CACHE_BUDGET_BYTES", &c.CacheBudgetBytes)
	setInt("CACHE_FORCED_EVICTION_HEADROOM_PCT", &c.CacheForcedEvictionHeadroomPct)
	setInt("EVICTION_PROVIDER_COUNT_THRESHOLD", &c.EvictionProviderCountThreshold)

	setInt("HRW_K", &c.HRWK)
	setStr("HRW_TOPOLOGY_SCOPE", &c.HRWTopologyScope)
	setStr("ZONE_LABEL_KEY", &c.ZoneLabelKey)

	setDur("NF5_JITTER_BASE", &c.NF5JitterBase)
	setInt("NF5_PER_NODE_RATE_LIMIT", &c.NF5PerNodeRateLimit)
	setDur("BOOTSTRAP_WINDOW", &c.BootstrapWindow)
	setInt("BOOTSTRAP_ROUTING_TABLE_PCT", &c.BootstrapRoutingTablePct)
	setInt("TOPK_EXPANSION_FACTOR_DEGRADED", &c.TopKExpansionFactorDegraded)

	setDur("ORIGIN_FAILURE_COOLDOWN_INITIAL", &c.OriginFailureCooldownInitial)
	setDur("ORIGIN_FAILURE_COOLDOWN_MAX", &c.OriginFailureCooldownMax)
	setInt("ORIGIN_FAILURE_COOLDOWN_MULTIPLIER", &c.OriginFailureCooldownMultiplier)
	setDur("ORIGIN_FAILURE_HONOR_WINDOW_CAP", &c.OriginFailureHonorWindowCap)

	setStr("LOG_LEVEL", &c.LogLevel)
	setStr("LOG_FORMAT", &c.LogFormat)

	return errors.Join(errs...)
}

// BindFlags registers command-line flags on fs that overlay c. Call after
// LoadYAML / LoadEnv but before fs.Parse() so flags win.
func (c *Config) BindFlags(fs *flag.FlagSet) {
	fs.StringVar(&c.MirrorListen, "mirror-listen", c.MirrorListen, "address for the containerd-facing mirror endpoint (loopback)")
	fs.StringVar(&c.TransferListen, "transfer-listen", c.TransferListen, "address for the peer-facing transfer endpoint")
	fs.StringVar(&c.MetricsListen, "metrics-listen", c.MetricsListen, "address for the Prometheus metrics endpoint")
	fs.StringVar(&c.Libp2pIdentityPath, "libp2p-identity-path", c.Libp2pIdentityPath, "path to the persisted libp2p identity key")

	fs.StringVar(&c.NodeName, "node-name", c.NodeName, "Kubernetes node name this agent runs on (Downward API spec.nodeName)")
	fs.StringVar(&c.MembersNamespace, "members-namespace", c.MembersNamespace, "namespace to scope the pod informer (empty = cluster-wide)")
	fs.StringVar(&c.MembersLabelSelector, "members-label-selector", c.MembersLabelSelector, "label selector identifying Gantry DaemonSet pods")
	fs.StringVar(&c.MembersKubeconfig, "members-kubeconfig", c.MembersKubeconfig, "optional path to a kubeconfig file (empty = in-cluster)")

	fs.StringVar(&c.CacheDir, "cache-dir", c.CacheDir, "hostPath directory for the content cache")
	fs.Int64Var(&c.CacheBudgetBytes, "cache-budget-bytes", c.CacheBudgetBytes, "soft cap on cache size in bytes")
	fs.IntVar(&c.CacheForcedEvictionHeadroomPct, "cache-forced-eviction-headroom-pct", c.CacheForcedEvictionHeadroomPct, "force eviction when free disk < this percent of budget")
	fs.IntVar(&c.EvictionProviderCountThreshold, "eviction-provider-count-threshold", c.EvictionProviderCountThreshold, "defer eviction when this node is one of fewer than N providers")

	fs.IntVar(&c.HRWK, "hrw-k", c.HRWK, "HRW top-K size")
	fs.StringVar(&c.HRWTopologyScope, "hrw-topology-scope", c.HRWTopologyScope, `HRW scope: "cluster" or "zone"`)
	fs.StringVar(&c.ZoneLabelKey, "zone-label-key", c.ZoneLabelKey, "Kubernetes node label identifying the zone (used when hrw-topology-scope=zone)")

	fs.DurationVar(&c.NF5JitterBase, "nf5-jitter-base", c.NF5JitterBase, "base delay for the NF5 jitter window")
	fs.IntVar(&c.NF5PerNodeRateLimit, "nf5-per-node-rate-limit", c.NF5PerNodeRateLimit, "per-node direct-origin fallback rate (per minute)")
	fs.DurationVar(&c.BootstrapWindow, "bootstrap-window", c.BootstrapWindow, "time after startup during which DHT-empty is not trusted as cold-start")
	fs.IntVar(&c.BootstrapRoutingTablePct, "bootstrap-routing-table-pct", c.BootstrapRoutingTablePct, "routing-table-size percent that ends the bootstrap window")
	fs.IntVar(&c.TopKExpansionFactorDegraded, "topk-expansion-factor-degraded", c.TopKExpansionFactorDegraded, "multiplier applied to HRW K when expanding under Degraded health")

	fs.DurationVar(&c.OriginFailureCooldownInitial, "origin-failure-cooldown-initial", c.OriginFailureCooldownInitial, "initial cooldown for the §5.8 origin-failure circuit breaker")
	fs.DurationVar(&c.OriginFailureCooldownMax, "origin-failure-cooldown-max", c.OriginFailureCooldownMax, "max cooldown for the §5.8 origin-failure circuit breaker")
	fs.IntVar(&c.OriginFailureCooldownMultiplier, "origin-failure-cooldown-multiplier", c.OriginFailureCooldownMultiplier, "exponential multiplier between successive cooldowns")
	fs.DurationVar(&c.OriginFailureHonorWindowCap, "origin-failure-honor-window-cap", c.OriginFailureHonorWindowCap, "requester-side honor window cap for transient cooldowns")

	fs.StringVar(&c.LogLevel, "log-level", c.LogLevel, "log level (debug/info/warn/error)")
	fs.StringVar(&c.LogFormat, "log-format", c.LogFormat, "log format (json/text)")
}

// Load is the convenience composition of NewDefault → LoadYAML(file) →
// LoadEnv → BindFlags → fs.Parse(args). It returns the fully-merged Config
// and the FlagSet for callers that want to print -help text. Validate() is
// the caller's responsibility — Load does not call it so callers can
// inspect partial configs (e.g., for `gantry version`).
func Load(args []string, env func(string) string, configPath string) (*Config, *flag.FlagSet, error) {
	c := NewDefault()

	if configPath != "" {
		f, err := os.Open(configPath) //#nosec G304 -- operator-supplied path
		if err != nil {
			return nil, nil, fmt.Errorf("config: open %s: %w", configPath, err)
		}
		defer func() { _ = f.Close() }()
		if err := c.LoadYAML(f); err != nil {
			return nil, nil, fmt.Errorf("config: parse %s: %w", configPath, err)
		}
	}
	if err := c.LoadEnv(env); err != nil {
		return nil, nil, err
	}

	fs := flag.NewFlagSet("gantry", flag.ContinueOnError)
	c.BindFlags(fs)
	// --config is parsed by the caller (chicken-and-egg with the file load)
	// but we still register it so -help lists it.
	_ = fs.String("config", configPath, "path to YAML config file")
	if err := fs.Parse(args); err != nil {
		return nil, fs, err
	}
	return c, fs, nil
}

// Validate runs hard-correctness checks on c. Returns nil if c is usable;
// otherwise returns a joined error listing every problem found.
func (c *Config) Validate() error {
	var errs []error

	mustAddr := func(field, val string) {
		if val == "" {
			errs = append(errs, fmt.Errorf("%s: required", field))
			return
		}
		if _, _, err := net.SplitHostPort(val); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", field, err))
		}
	}
	mustAddr("mirror_listen", c.MirrorListen)
	mustAddr("transfer_listen", c.TransferListen)
	mustAddr("metrics_listen", c.MetricsListen)

	// MirrorListen MUST be loopback (§4.1, §7.5).
	if host, _, err := net.SplitHostPort(c.MirrorListen); err == nil {
		ip := net.ParseIP(host)
		if ip != nil && !ip.IsLoopback() {
			errs = append(errs, fmt.Errorf("mirror_listen %q is not loopback; only 127.0.0.1 / ::1 are safe (containerd mirror uses skip_verify=true)", c.MirrorListen))
		}
		if ip == nil && host != "localhost" && host != "" {
			errs = append(errs, fmt.Errorf("mirror_listen host %q: must be loopback", host))
		}
	}

	if c.CacheDir == "" {
		errs = append(errs, errors.New("cache_dir: required"))
	} else if !filepath.IsAbs(c.CacheDir) {
		errs = append(errs, fmt.Errorf("cache_dir %q: must be absolute", c.CacheDir))
	}
	if c.CacheBudgetBytes <= 0 {
		errs = append(errs, fmt.Errorf("cache_budget_bytes: must be > 0, got %d", c.CacheBudgetBytes))
	}
	if c.CacheForcedEvictionHeadroomPct < 0 || c.CacheForcedEvictionHeadroomPct >= 100 {
		errs = append(errs, fmt.Errorf("cache_forced_eviction_headroom_pct: must be in [0,100), got %d", c.CacheForcedEvictionHeadroomPct))
	}
	if c.EvictionProviderCountThreshold < 1 {
		errs = append(errs, fmt.Errorf("eviction_provider_count_threshold: must be >= 1, got %d", c.EvictionProviderCountThreshold))
	}

	if len(c.UpstreamRegistries) == 0 {
		errs = append(errs, errors.New("upstream_registries: at least one entry required"))
	}
	seen := map[string]int{}
	for i, ur := range c.UpstreamRegistries {
		if ur.Name == "" {
			errs = append(errs, fmt.Errorf("upstream_registries[%d].name: required", i))
		} else if prev, ok := seen[ur.Name]; ok {
			errs = append(errs, fmt.Errorf("upstream_registries[%d].name: duplicates upstream_registries[%d]", i, prev))
		} else {
			seen[ur.Name] = i
		}
		if ur.Endpoint == "" {
			errs = append(errs, fmt.Errorf("upstream_registries[%d].endpoint: required", i))
		} else if !strings.HasPrefix(ur.Endpoint, "http://") && !strings.HasPrefix(ur.Endpoint, "https://") {
			errs = append(errs, fmt.Errorf("upstream_registries[%d].endpoint %q: must start with http:// or https://", i, ur.Endpoint))
		}
	}

	if c.HRWK < 1 {
		errs = append(errs, fmt.Errorf("hrw_k: must be >= 1, got %d", c.HRWK))
	}
	switch c.HRWTopologyScope {
	case "cluster", "zone":
	default:
		errs = append(errs, fmt.Errorf("hrw_topology_scope %q: must be \"cluster\" or \"zone\"", c.HRWTopologyScope))
	}

	if c.NF5JitterBase <= 0 {
		errs = append(errs, fmt.Errorf("nf5_jitter_base: must be > 0, got %v", c.NF5JitterBase))
	}
	if c.NF5PerNodeRateLimit < 1 {
		errs = append(errs, fmt.Errorf("nf5_per_node_rate_limit: must be >= 1, got %d", c.NF5PerNodeRateLimit))
	}
	if c.BootstrapWindow <= 0 {
		errs = append(errs, fmt.Errorf("bootstrap_window: must be > 0, got %v", c.BootstrapWindow))
	}
	if c.BootstrapRoutingTablePct < 1 || c.BootstrapRoutingTablePct > 100 {
		errs = append(errs, fmt.Errorf("bootstrap_routing_table_pct: must be in [1,100], got %d", c.BootstrapRoutingTablePct))
	}
	if c.TopKExpansionFactorDegraded < 1 {
		errs = append(errs, fmt.Errorf("topk_expansion_factor_degraded: must be >= 1, got %d", c.TopKExpansionFactorDegraded))
	}

	if c.OriginFailureCooldownInitial <= 0 {
		errs = append(errs, fmt.Errorf("origin_failure_cooldown_initial: must be > 0, got %v", c.OriginFailureCooldownInitial))
	}
	if c.OriginFailureCooldownMax < c.OriginFailureCooldownInitial {
		errs = append(errs, fmt.Errorf("origin_failure_cooldown_max %v: must be >= origin_failure_cooldown_initial %v", c.OriginFailureCooldownMax, c.OriginFailureCooldownInitial))
	}
	if c.OriginFailureCooldownMultiplier < 1 {
		errs = append(errs, fmt.Errorf("origin_failure_cooldown_multiplier: must be >= 1, got %d", c.OriginFailureCooldownMultiplier))
	}

	switch c.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		errs = append(errs, fmt.Errorf("log_level %q: must be debug|info|warn|error", c.LogLevel))
	}
	switch c.LogFormat {
	case "json", "text":
	default:
		errs = append(errs, fmt.Errorf("log_format %q: must be json|text", c.LogFormat))
	}

	return errors.Join(errs...)
}

// ResolveUpstream returns the UpstreamRegistry whose Name (or NSAlias)
// equals ns. Returns false if ns does not match any configured registry.
func (c *Config) ResolveUpstream(ns string) (UpstreamRegistry, bool) {
	for _, ur := range c.UpstreamRegistries {
		if ur.Name == ns || (ur.NSAlias != "" && ur.NSAlias == ns) {
			return ur, true
		}
	}
	return UpstreamRegistry{}, false
}

// Redacted returns a copy of c suitable for logging. Currently, credentials
// are referenced only by path, so nothing requires actual redaction; the
// method exists so future secret-bearing fields have one obvious place to
// be sanitized.
func (c *Config) Redacted() *Config {
	cp := *c
	cp.UpstreamRegistries = append([]UpstreamRegistry(nil), c.UpstreamRegistries...)
	// CredentialsPath is a path, not the secret; safe to log as-is.
	return &cp
}

func lookup(env func(string) string, key string) (string, bool) {
	v := env("GANTRY_" + key)
	return v, v != ""
}
