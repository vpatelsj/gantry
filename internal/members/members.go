// Package members provides Gantry's cluster-membership view, sourced from
// Kubernetes informers.
//
// Design (detailed-design.md §7.3):
//
//   - A label-selected Pod informer enumerates Gantry DaemonSet pods. The
//     selector is operator-configurable (`members_label_selector`) so this
//     package does not assume a particular DaemonSet manifest.
//   - A cluster-scoped Node informer is joined on `pod.spec.nodeName` so
//     each peer carries its zone label (default
//     `topology.kubernetes.io/zone`). Phase 3's HRW reads `Node.Zone`
//     directly — Members owns the join so HRW does not re-fetch.
//   - `Self()` is set from the Downward API (`spec.nodeName` →
//     `GANTRY_NODE_NAME`); this is the stable identifier HRW uses to score
//     digests.
//
// Failure modes:
//
//   - In-cluster: a missing/invalid service-account token fails fast at
//     New(); the agent cannot run without a Kubernetes API view.
//   - Out-of-cluster: explicit `members_kubeconfig` is honored; this path
//     is the developer/test path, not production.
//
// Tests use `client-go/kubernetes/fake` via the Options.Clientset escape
// hatch so the package does not require a real cluster.
package members

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/gantry/gantry/internal/ifaces"
)

// Options configures a Manager.
type Options struct {
	// NodeName is the Kubernetes node this agent runs on. Required — used
	// as Self() and as the join key against the Node informer for zone
	// resolution.
	NodeName string

	// Namespace restricts the pod informer; empty means cluster-wide.
	Namespace string

	// LabelSelector is the K8s label selector identifying peer agents.
	// Required (empty selector would enrol every pod in the cluster).
	LabelSelector string

	// ZoneLabelKey is the node label that exposes the topology zone.
	// Defaults to `topology.kubernetes.io/zone` if empty.
	ZoneLabelKey string

	// Kubeconfig is the path to an out-of-cluster kubeconfig. Empty means
	// in-cluster service-account discovery.
	Kubeconfig string

	// Clientset is an optional pre-built clientset (typically the fake
	// clientset in tests). When non-nil, Kubeconfig is ignored.
	Clientset kubernetes.Interface

	// ResyncPeriod is the informer resync interval. Zero means "no resync"
	// (rely on watch events). Default 30s when zero.
	ResyncPeriod time.Duration

	// TransferPort is the TCP port each agent's transfer endpoint
	// listens on (§7.2). When non-zero, Snapshot fills Node.Addr as
	// "podIP:TransferPort"; when zero, Snapshot returns the bare pod
	// IP (back-compat). Production deployments MUST set this so
	// peer-fetch URLs are reachable.
	TransferPort int
}

// Manager owns the Pod+Node informers and exposes ifaces.Members.
type Manager struct {
	self         ifaces.NodeID
	zoneLabelKey string
	selector     labels.Selector
	transferPort int
	clientset    kubernetes.Interface
	namespace    string

	podFactory  informers.SharedInformerFactory
	nodeFactory informers.SharedInformerFactory
	podInf      cache.SharedIndexInformer
	nodeInf     cache.SharedIndexInformer

	stopCh chan struct{}
	once   sync.Once
}

// New builds a Manager. The factory is constructed but informers are not
// started until Start() is called.
func New(opts Options) (*Manager, error) {
	if opts.NodeName == "" {
		return nil, errors.New("members: NodeName required (set via spec.nodeName / GANTRY_NODE_NAME)")
	}
	if opts.LabelSelector == "" {
		return nil, errors.New("members: LabelSelector required")
	}
	sel, err := labels.Parse(opts.LabelSelector)
	if err != nil {
		return nil, fmt.Errorf("members: parse selector %q: %w", opts.LabelSelector, err)
	}

	cs := opts.Clientset
	if cs == nil {
		rc, err := loadRESTConfig(opts.Kubeconfig)
		if err != nil {
			return nil, fmt.Errorf("members: load kube config: %w", err)
		}
		cs, err = kubernetes.NewForConfig(rc)
		if err != nil {
			return nil, fmt.Errorf("members: build clientset: %w", err)
		}
	}

	resync := opts.ResyncPeriod
	if resync == 0 {
		resync = 30 * time.Second
	}
	zoneKey := opts.ZoneLabelKey
	if zoneKey == "" {
		zoneKey = "topology.kubernetes.io/zone"
	}

	// Pod factory: namespace-scoped + label-selected.
	podFactory := informers.NewSharedInformerFactoryWithOptions(cs, resync,
		informers.WithNamespace(opts.Namespace),
		informers.WithTweakListOptions(func(lo *metav1.ListOptions) {
			lo.LabelSelector = sel.String()
		}),
	)
	// Node factory: cluster-scoped, no selector (the pod selector must not
	// also filter nodes).
	nodeFactory := informers.NewSharedInformerFactory(cs, resync)

	return &Manager{
		self:         ifaces.NodeID(opts.NodeName),
		zoneLabelKey: zoneKey,
		selector:     sel,
		transferPort: opts.TransferPort,
		clientset:    cs,
		namespace:    opts.Namespace,
		podFactory:   podFactory,
		nodeFactory:  nodeFactory,
		podInf:       podFactory.Core().V1().Pods().Informer(),
		nodeInf:      nodeFactory.Core().V1().Nodes().Informer(),
		stopCh:       make(chan struct{}),
	}, nil
}

// Start begins the informers' list-and-watch in the background. It
// does NOT wait for initial sync — callers MUST follow Start with a
// WaitForSync(ctx) call under a bounded context so they own the
// sync-deadline policy (production mode treats a timeout as fatal,
// dev mode warns and continues; see cmd/gantry/main.go.buildMembers
// for the canonical use). Previously Start blocked on
// WaitForSync(ctx) using the long-lived app context, which made the
// 10s bounded WaitForSync in buildMembers dead code — an RBAC /
// permissions failure could pin startup indefinitely instead of
// reaching the deadline branch.
func (m *Manager) Start() {
	m.podFactory.Start(m.stopCh)
	m.nodeFactory.Start(m.stopCh)
}

// Stop tears down the informers. Safe to call multiple times.
func (m *Manager) Stop() {
	m.once.Do(func() { close(m.stopCh) })
}

// Self implements ifaces.Members.
func (m *Manager) Self() ifaces.NodeID { return m.self }

// WaitForSync blocks until both informers have completed initial list+watch
// or ctx is cancelled.
func (m *Manager) WaitForSync(ctx context.Context) error {
	if !cache.WaitForCacheSync(ctx.Done(), m.podInf.HasSynced, m.nodeInf.HasSynced) {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("members: wait for sync: %w", err)
		}
		return errors.New("members: informer sync failed")
	}
	return nil
}

// Annotation keys agents publish on their own pod so peers can discover
// libp2p identity and the transfer endpoint without operator-supplied
// bootstrap config.
const (
	AnnotationPeerID       = "gantry.io/peer-id"
	AnnotationP2PAddrs     = "gantry.io/p2p-addrs"     // comma-separated multiaddrs
	AnnotationTransferAddr = "gantry.io/transfer-addr" // host:port
)

// Snapshot returns the current peer view: one Node per Ready pod matching
// the selector, joined on spec.nodeName for zone labels. The returned slice
// is sorted by NodeID for deterministic HRW input.
//
// PeerID, P2PAddrs and a transfer-addr override are read from pod
// annotations (gantry.io/peer-id, gantry.io/p2p-addrs,
// gantry.io/transfer-addr) populated by each agent's AnnounceSelf call
// at startup. Pods that have not yet published these annotations still
// appear in the snapshot — Addr falls back to podIP[:TransferPort],
// PeerID/P2PAddrs are empty until the announcement arrives.
func (m *Manager) Snapshot() []ifaces.Node {
	out := []ifaces.Node{}
	for _, obj := range m.podInf.GetStore().List() {
		p, ok := obj.(*corev1.Pod)
		if !ok {
			continue
		}
		if !podReady(p) {
			continue
		}
		if p.Spec.NodeName == "" || p.Status.PodIP == "" {
			continue
		}
		addr := p.Status.PodIP
		if m.transferPort > 0 {
			addr = fmt.Sprintf("%s:%d", p.Status.PodIP, m.transferPort)
		}
		// Annotation override wins so operators can publish a
		// non-default transfer endpoint (NodePort, separate listener).
		if a := p.Annotations[AnnotationTransferAddr]; a != "" {
			addr = a
		}
		node := ifaces.Node{
			ID:       ifaces.NodeID(p.Spec.NodeName),
			Addr:     addr,
			PeerID:   p.Annotations[AnnotationPeerID],
			P2PAddrs: splitAnnotation(p.Annotations[AnnotationP2PAddrs]),
		}
		if obj, exists, err := m.nodeInf.GetStore().GetByKey(p.Spec.NodeName); err == nil && exists {
			if n, ok := obj.(*corev1.Node); ok {
				node.Zone = n.Labels[m.zoneLabelKey]
			}
		}
		out = append(out, node)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// splitAnnotation parses a comma-separated annotation value, trimming
// whitespace around each entry and dropping empty fields. Returns nil
// when no entries remain so callers can range over the result without
// special-casing the empty-annotation path.
func splitAnnotation(v string) []string {
	if v == "" {
		return nil
	}
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// SnapshotForBootstrap returns the membership view used for libp2p
// bootstrap dialing: every Running pod matching the selector that has
// published a gantry.io/p2p-addrs annotation, regardless of Ready
// status. This intentionally diverges from Snapshot() because the
// readiness probe depends on a populated DHT routing table, which
// depends on libp2p bootstrap, which (under Snapshot's filter) would
// depend on at least one peer already being Ready — a deadlock on
// first-cluster boot and full-cluster restart. Bootstrap-time peers
// are a strictly larger set than serving-time peers; we want any peer
// we can reach, even a NotReady one, just to seed the DHT.
//
// The serving path (HRW choice, transfer destinations) MUST keep
// using Snapshot() so unready peers don't receive request traffic.
func (m *Manager) SnapshotForBootstrap() []ifaces.Node {
	out := []ifaces.Node{}
	for _, obj := range m.podInf.GetStore().List() {
		p, ok := obj.(*corev1.Pod)
		if !ok {
			continue
		}
		if p.Status.Phase != corev1.PodRunning {
			continue
		}
		if p.Spec.NodeName == "" || p.Status.PodIP == "" {
			continue
		}
		// Bootstrap is only useful when the peer has published its
		// libp2p multiaddrs. Pods that haven't AnnounceSelf'd yet
		// contribute nothing to a dial seed.
		p2p := splitAnnotation(p.Annotations[AnnotationP2PAddrs])
		if len(p2p) == 0 {
			continue
		}
		addr := p.Status.PodIP
		if m.transferPort > 0 {
			addr = fmt.Sprintf("%s:%d", p.Status.PodIP, m.transferPort)
		}
		if a := p.Annotations[AnnotationTransferAddr]; a != "" {
			addr = a
		}
		out = append(out, ifaces.Node{
			ID:       ifaces.NodeID(p.Spec.NodeName),
			Addr:     addr,
			PeerID:   p.Annotations[AnnotationPeerID],
			P2PAddrs: p2p,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// podReady reports whether a pod is in Running phase with a Ready=True
// condition. Pending/Terminating pods are excluded from the membership view
// so HRW does not score nodes that cannot serve traffic.
func podReady(p *corev1.Pod) bool {
	if p.Status.Phase != corev1.PodRunning {
		return false
	}
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// Compile-time check.
var _ ifaces.Members = (*Manager)(nil)

// loadRESTConfig prefers in-cluster discovery; falls back to an explicit
// kubeconfig path. Empty path + no in-cluster env returns an error.
func loadRESTConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	rc, err := rest.InClusterConfig()
	if err == nil {
		return rc, nil
	}
	if !errors.Is(err, rest.ErrNotInCluster) {
		return nil, err
	}
	return nil, errors.New("members: not in cluster and no kubeconfig supplied")
}
