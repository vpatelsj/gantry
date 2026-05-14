package members

import (
	"context"
	"sort"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func newPod(name, ns, node, ip string, ready bool, labels map[string]string) *corev1.Pod {
	cond := corev1.ConditionFalse
	if ready {
		cond = corev1.ConditionTrue
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			Labels:    labels,
		},
		Spec: corev1.PodSpec{NodeName: node},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: ip,
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: cond},
			},
		},
	}
}

func newNode(name, zone string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: map[string]string{"topology.kubernetes.io/zone": zone},
		},
	}
}

func TestNew_RequiresNodeName(t *testing.T) {
	_, err := New(Options{LabelSelector: "app=gantry"})
	if err == nil {
		t.Fatal("expected NodeName-required error")
	}
}

func TestNew_RequiresSelector(t *testing.T) {
	_, err := New(Options{NodeName: "n1"})
	if err == nil {
		t.Fatal("expected LabelSelector-required error")
	}
}

func TestNew_RejectsBadSelector(t *testing.T) {
	_, err := New(Options{NodeName: "n1", LabelSelector: "$$bogus"})
	if err == nil {
		t.Fatal("expected bad-selector error")
	}
}

func TestSnapshot_JoinsPodIPAndNodeZone(t *testing.T) {
	cs := fake.NewSimpleClientset(
		newPod("gantry-a", "kube-system", "node-a", "10.0.0.1", true, map[string]string{"app.kubernetes.io/name": "gantry"}),
		newPod("gantry-b", "kube-system", "node-b", "10.0.0.2", true, map[string]string{"app.kubernetes.io/name": "gantry"}),
		newPod("noisy", "kube-system", "node-a", "10.0.0.99", true, map[string]string{"app.kubernetes.io/name": "other"}),
		newPod("not-ready", "kube-system", "node-c", "10.0.0.3", false, map[string]string{"app.kubernetes.io/name": "gantry"}),
		newNode("node-a", "us-east-1a"),
		newNode("node-b", "us-east-1b"),
		newNode("node-c", "us-east-1c"),
	)
	m, err := New(Options{
		NodeName:      "node-a",
		Namespace:     "",
		LabelSelector: "app.kubernetes.io/name=gantry",
		Clientset:     cs,
		ResyncPeriod:  10 * time.Minute,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(m.Stop)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if got, want := m.Self(), "node-a"; string(got) != want {
		t.Errorf("Self = %q, want %q", got, want)
	}

	got := m.Snapshot()
	if len(got) != 2 {
		t.Fatalf("Snapshot len = %d, want 2 (have %+v)", len(got), got)
	}
	// Snapshot is sorted by NodeID.
	if !sort.SliceIsSorted(got, func(i, j int) bool { return got[i].ID < got[j].ID }) {
		t.Errorf("Snapshot not sorted by ID: %+v", got)
	}
	want := map[string]struct {
		Addr string
		Zone string
	}{
		"node-a": {"10.0.0.1", "us-east-1a"},
		"node-b": {"10.0.0.2", "us-east-1b"},
	}
	for _, n := range got {
		w, ok := want[string(n.ID)]
		if !ok {
			t.Errorf("unexpected node %+v", n)
			continue
		}
		if n.Addr != w.Addr {
			t.Errorf("%s Addr = %q, want %q", n.ID, n.Addr, w.Addr)
		}
		if n.Zone != w.Zone {
			t.Errorf("%s Zone = %q, want %q", n.ID, n.Zone, w.Zone)
		}
	}
}

func TestSnapshot_ExcludesNonRunning(t *testing.T) {
	pending := newPod("pending", "ns", "node-x", "10.0.0.4", true, map[string]string{"app.kubernetes.io/name": "gantry"})
	pending.Status.Phase = corev1.PodPending
	cs := fake.NewSimpleClientset(
		pending,
		newNode("node-x", "us-east-1a"),
	)
	m, err := New(Options{
		NodeName:      "node-x",
		LabelSelector: "app.kubernetes.io/name=gantry",
		Clientset:     cs,
		ResyncPeriod:  10 * time.Minute,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(m.Stop)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if got := m.Snapshot(); len(got) != 0 {
		t.Errorf("Snapshot = %+v, want empty (pending pods excluded)", got)
	}
}

func TestSnapshot_OmitsPodsWithoutIP(t *testing.T) {
	cs := fake.NewSimpleClientset(
		newPod("no-ip", "ns", "node-x", "", true, map[string]string{"app.kubernetes.io/name": "gantry"}),
		newNode("node-x", "us-east-1a"),
	)
	m, err := New(Options{
		NodeName:      "node-x",
		LabelSelector: "app.kubernetes.io/name=gantry",
		Clientset:     cs,
		ResyncPeriod:  10 * time.Minute,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(m.Stop)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if got := m.Snapshot(); len(got) != 0 {
		t.Errorf("Snapshot = %+v, want empty", got)
	}
}

func TestZoneLabelKey_Override(t *testing.T) {
	n := newNode("node-x", "")
	n.Labels["custom/zone"] = "z1"
	cs := fake.NewSimpleClientset(
		newPod("p", "ns", "node-x", "10.0.0.1", true, map[string]string{"app": "gantry"}),
		n,
	)
	m, err := New(Options{
		NodeName:      "node-x",
		LabelSelector: "app=gantry",
		ZoneLabelKey:  "custom/zone",
		Clientset:     cs,
		ResyncPeriod:  10 * time.Minute,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(m.Stop)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	got := m.Snapshot()
	if len(got) != 1 || got[0].Zone != "z1" {
		t.Errorf("Snapshot = %+v, want zone=z1", got)
	}
}

func TestWaitForSync_RespectsCtx(t *testing.T) {
	cs := fake.NewSimpleClientset()
	m, err := New(Options{
		NodeName:      "n1",
		LabelSelector: "app=gantry",
		Clientset:     cs,
		ResyncPeriod:  10 * time.Minute,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(m.Stop)

	// Stop the manager *before* starting, so factory.Start is a no-op and
	// HasSynced never flips. WaitForSync should return when ctx expires.
	m.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	err = m.WaitForSync(ctx)
	if err == nil {
		t.Fatal("expected wait error from ctx deadline / sync failure")
	}
}
