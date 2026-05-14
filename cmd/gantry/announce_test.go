package main

import (
	"testing"

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
			name:  "ipv6 wildcard with v4 pod IP rewrites to /ip4/",
			in:    "/ip6/::/tcp/4001",
			podIP: "10.42.0.7",
			want:  "/ip4/10.42.0.7/tcp/4001",
		},
		{
			name:  "ipv6 wildcard with v6 pod IP rewrites to /ip6/",
			in:    "/ip6/::/tcp/4001",
			podIP: "fd00:10:244::7",
			want:  "/ip6/fd00:10:244::7/tcp/4001",
		},
		{
			name:  "ipv4 wildcard with v6 pod IP rewrites to /ip6/",
			in:    "/ip4/0.0.0.0/tcp/4001",
			podIP: "fd00:10:244::7",
			want:  "/ip6/fd00:10:244::7/tcp/4001",
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
