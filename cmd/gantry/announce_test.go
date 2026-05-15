package main

import (
	"testing"

	"github.com/gantry/gantry/internal/config"
	"github.com/gantry/gantry/internal/ifaces"
	"github.com/gantry/gantry/internal/ifaces/fakes"
	"github.com/gantry/gantry/internal/members"
)

// rewriteWildcardMultiaddr substitutes a Pod IP into wildcard listen
// addresses so the agent publishes dialable p2p multiaddrs in its
// self-announce annotation. Without the substitution, peers receive
// /ip4/0.0.0.0/tcp/4001 and silently fail to connect, deadlocking
// libp2p bootstrap on a cold cluster.
func TestRewriteWildcardMultiaddr(t *testing.T) {
	cases := []struct {
		name  string
		in    string
		podIP string
		want  string
	}{
		{
			name:  "ipv4 wildcard with pod IP",
			in:    "/ip4/0.0.0.0/tcp/4001",
			podIP: "10.42.0.7",
			want:  "/ip4/10.42.0.7/tcp/4001",
		},
		{
			name:  "ipv4 wildcard without pod IP returns empty (skip)",
			in:    "/ip4/0.0.0.0/tcp/4001",
			podIP: "",
			want:  "",
		},
		{
			name:  "ipv6 wildcard with v6 pod IP rewrites to /ip6/",
			in:    "/ip6/::/tcp/4001",
			podIP: "fd00:10:244::7",
			want:  "/ip6/fd00:10:244::7/tcp/4001",
		},
		{
			name:  "ipv4 wildcard with v6 pod IP skips (no v6 listener)",
			in:    "/ip4/0.0.0.0/tcp/4001",
			podIP: "fd00:10:244::7",
			want:  "",
		},
		{
			name:  "ipv6 wildcard with v4 pod IP skips (no v4 listener under /ip6/)",
			in:    "/ip6/::/tcp/4001",
			podIP: "10.42.0.7",
			want:  "",
		},
		{
			name:  "wildcard with unparseable pod IP returns empty",
			in:    "/ip4/0.0.0.0/tcp/4001",
			podIP: "not-an-ip",
			want:  "",
		},
		{
			name:  "concrete ipv4 passes through",
			in:    "/ip4/10.42.0.7/tcp/4001",
			podIP: "10.42.0.7",
			want:  "/ip4/10.42.0.7/tcp/4001",
		},
		{
			name:  "concrete ipv6 passes through",
			in:    "/ip6/2001:db8::1/tcp/4001",
			podIP: "10.42.0.7",
			want:  "/ip6/2001:db8::1/tcp/4001",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := rewriteWildcardMultiaddr(tc.in, tc.podIP)
			if got != tc.want {
				t.Errorf("rewriteWildcardMultiaddr(%q, %q) = %q, want %q",
					tc.in, tc.podIP, got, tc.want)
			}
		})
	}
}

// advertisedTransferAddr leaves empty when the listen host is a
// wildcard and no Pod IP is available, so members.Snapshot falls back
// to composing podIP:port from the pod's status.podIP. A non-empty
// 0.0.0.0:port published in the annotation would override that
// fallback and produce an unreachable advertised address.
func TestAdvertisedTransferAddr(t *testing.T) {
	cases := []struct {
		name           string
		transferListen string
		podIP          string
		want           string
	}{
		{
			name:           "ipv4 wildcard + pod IP composes",
			transferListen: "0.0.0.0:5001",
			podIP:          "10.42.0.7",
			want:           "10.42.0.7:5001",
		},
		{
			name:           "ipv4 wildcard + empty pod IP returns empty",
			transferListen: "0.0.0.0:5001",
			podIP:          "",
			want:           "",
		},
		{
			name:           "ipv6 wildcard + pod IP composes",
			transferListen: "[::]:5001",
			podIP:          "10.42.0.7",
			want:           "10.42.0.7:5001",
		},
		{
			name:           "explicit bind passes through",
			transferListen: "10.42.0.7:5001",
			podIP:          "10.42.0.7",
			want:           "10.42.0.7:5001",
		},
		{
			name:           "empty host passes through wildcard handling",
			transferListen: ":5001",
			podIP:          "10.42.0.7",
			want:           "10.42.0.7:5001",
		},
		{
			name:           "unparseable listen passes through verbatim",
			transferListen: "notahostport",
			podIP:          "10.42.0.7",
			want:           "notahostport",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := advertisedTransferAddr(tc.transferListen, tc.podIP)
			if got != tc.want {
				t.Errorf("advertisedTransferAddr(%q, %q) = %q, want %q",
					tc.transferListen, tc.podIP, got, tc.want)
			}
		})
	}
}

// hasMultiNodeMembership controls whether the cold-start orchestrator
// is wired at startup. On first-cluster boot the membership snapshot
// is empty (no peer is Ready yet) — but cold-start must still be
// enabled because that is exactly the situation it exists to handle.
// The gate therefore looks at whether the membership view is backed
// by the real K8s informer (cold-start ON) vs the dev-mode single-
// self fake (cold-start OFF, no peers to coordinate with).
func TestHasMultiNodeMembership(t *testing.T) {
	t.Run("real manager with empty snapshot still enables cold-start", func(t *testing.T) {
		// A *members.Manager with no Start() called yet has an empty
		// snapshot — the first-cluster-boot scenario. The previous
		// implementation returned false here and permanently disabled
		// cold-start; the new implementation returns true.
		var mgr *members.Manager // typed nil; only the dynamic type matters
		got := hasMultiNodeMembership(mgr)
		if !got {
			t.Errorf("hasMultiNodeMembership(*members.Manager) = false, want true (first-cluster boot must enable cold-start)")
		}
	})
	t.Run("single-self fake disables cold-start", func(t *testing.T) {
		f := fakes.NewMembers(ifaces.NodeID("self"), ifaces.Node{ID: "self"})
		if hasMultiNodeMembership(f) {
			t.Errorf("hasMultiNodeMembership(single-self fake) = true, want false (no peers to coordinate)")
		}
	})
}

// isProductionMode is the gate that decides whether a Kubernetes-
// membership setup failure crashes the agent or silently falls back
// to a single-self stub. Misclassifying this is the difference
// between a clear deploy-time error and an apparently-healthy agent
// running with no peer coordination.
func TestIsProductionMode(t *testing.T) {
	cases := []struct {
		name string
		mut  func(c *config.Config)
		want bool
	}{
		{
			name: "all empty is dev mode",
			mut:  func(*config.Config) {},
			want: false,
		},
		{
			name: "NodeName set is production",
			mut:  func(c *config.Config) { c.NodeName = "node-1" },
			want: true,
		},
		{
			name: "PodName set is production",
			mut:  func(c *config.Config) { c.PodName = "gantry-abc" },
			want: true,
		},
		{
			name: "MembersNamespace set is production",
			mut:  func(c *config.Config) { c.MembersNamespace = "gantry-system" },
			want: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := &config.Config{}
			tc.mut(c)
			if got := isProductionMode(c); got != tc.want {
				t.Errorf("isProductionMode = %v, want %v", got, tc.want)
			}
		})
	}
}

// selfAnnounceRequiredForReadiness must be true exactly when the
// agent is in production mode, has a PodName (so AnnounceSelf has
// anything to patch), AND no static bootstrap peers are configured.
// A misclassification either lets a broken-RBAC pod report Ready
// silently isolated (the bug this gate fixes), or stalls rollouts
// when static peers would have seeded the DHT anyway.
func TestSelfAnnounceRequiredForReadiness(t *testing.T) {
	cases := []struct {
		name string
		c    *config.Config
		want bool
	}{
		{
			name: "dev mode never requires self-announce",
			c:    &config.Config{},
			want: false,
		},
		{
			name: "prod mode with PodName and no static peers requires it",
			c: &config.Config{
				NodeName: "node-1",
				PodName:  "gantry-abc",
			},
			want: true,
		},
		{
			name: "prod mode with static bootstrap peers bypasses gate",
			c: &config.Config{
				NodeName:             "node-1",
				PodName:              "gantry-abc",
				Libp2pBootstrapPeers: []string{"/ip4/10.0.0.1/tcp/4001/p2p/Qm..."},
			},
			want: false,
		},
		{
			name: "prod mode without PodName cannot self-announce, so no gate",
			c: &config.Config{
				NodeName: "node-1",
			},
			want: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := selfAnnounceRequiredForReadiness(tc.c); got != tc.want {
				t.Errorf("selfAnnounceRequiredForReadiness = %v, want %v", got, tc.want)
			}
		})
	}
}
