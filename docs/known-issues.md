# Known Issues

Issues observed during the AKS 20-node demo run on 2026-05-16. Not blocking the
baseline-vs-cold-start headline numbers (F1 invariant proven), but they affect
the warm-cache phase and have product-design implications.

## I-1. cdsub aborts an entire image's announce on first missing child blob *(fixed)*

**Severity**: HIGH. Was preventing all DHT advertising on AKS — every cdsub
reconcile reported `digests:0` even with 218 images present in containerd.

**Symptom**: With `containerd_socket` configured and the gantry container able
to reach the socket, `cdsub: reconcile complete digests:0` on every node, and
`p2p_dht_provide_total` stayed at 0 cluster-wide. With debug logging enabled
every image emitted `cdsub: walk failed err="content digest sha256:…: not
found"`.

**Root cause**: AKS containerd (via the kubelet CRI plugin) only fetches the
platform-relevant subtree of a multi-arch image index when pulling. Attestation
manifests and other-arch child manifests appear in the index but their content
is never written to the local content store. `walkBlobs` calls `images.Walk` →
`images.Children` which reads each descriptor's body from the content store; on
the first `not found` error the walk aborts and we lose the entire image's
digest set — including the digests we DO have on disk.

**Fix**: [internal/cdsub/source_containerd.go](../internal/cdsub/source_containerd.go)
now wraps `images.Children` in `childrenIfPresent` which downgrades
`errdefs.IsNotFound` to `(nil, nil)`. We record the digests we can reach and
skip absent subtrees. Verified in the demo cluster: 2,561 digests reconciled
per pod, 52,159 cluster-wide DHT provides on a 20-node AKS.

## I-2. Warm-cache phase still leaks ≈10 origin requests *(open)*

**Severity**: MEDIUM. Headline F1 story (baseline-vs-cold-start) is unaffected,
but the RUNBOOK §6 Phase 3 expectation of zero origin requests on warm rerun is
not met on AKS. Demo recording can proceed without the warm phase.

**Observed numbers** on the 20-node AKS demo with the I-1 fix in place
(small ~16 MiB image):
- Cold-start: 7 blob + 9 manifest_by_digest origin requests, 5 origin pulls
  via Gantry, 17 peer-fetch hits.
- Warm: 7 blob + 3 manifest_by_digest leak to origin, 21 cluster-wide cache
  hits, 43 peer-fetch hits, 50 peer-fetch `notfound`.

**Root cause analysis**: Three interacting facts.

1. **Gantry's cache is sparse** by design — only the HRW-elected puller for
   each digest holds the bytes after a cold-start.
2. **cdsub announces every digest visible in containerd**, not just those held
   in Gantry's own cache. So nodes whose containerd has the bytes (because
   kubelet pulled them through Gantry as part of the cold-start Job) appear as
   DHT providers even though their Gantry cache is empty.
3. **Cache-purge directly invokes `ctr content rm`**, which removes content
   from the containerd content store but does **not** fire an `Image.Delete`
   event. The cdsub `EventDelete` handler is intentionally a no-op
   (`internal/cdsub/cdsub.go` package doc — relies on libp2p provider record
   TTL), and `ctr content rm` doesn't hit that handler anyway.

The combination produces stale DHT provider records pointing at nodes whose
containerd-only copy was just nuked. When `mirror.tryPeerFallback` walks those
providers, the transfer endpoint returns 404 (Gantry cache empty + secondary
containerd source empty) → mirror exhausts its provider list → coldstart
cascade → NF5 → origin.

`peer_fetch_total{outcome="notfound"} = 50` (vs `hit = 43`) is the smoking
gun — about half of all peer-fetch attempts during warm hit a stale provider
record.

**Possible fixes** (not implemented):

1. **Have cdsub only announce digests that are in Gantry's own cache.** Makes
   the secondary-blob-source path "best effort, not advertised." Loses some
   DHT coverage on cold cluster bootstrap but eliminates the stale-record
   problem.
2. **Have the transfer endpoint un-Provide a digest after responding 404.**
   Self-healing, but requires Gantry to track every 404 and propagate a
   delete to the DHT — adds complexity.
3. **Have cdsub watch for `Image.Delete` events AND periodically re-reconcile
   to detect direct content-store removals.** Already does periodic reconcile
   on reconnect; would need a wall-clock timer too.
4. **Change cache-purge to use `ctr images rm` instead of `ctr content rm`.**
   That fires `ImageDelete` events — but cdsub's delete handler is a no-op, so
   this only helps if cdsub is also taught to un-Provide on delete. And
   `ctr images rm` is image-granular, not digest-granular, so the demo's
   per-digest selectivity goes away.

**Workaround for the demo recording**: skip the warm-cache phase. The
baseline-vs-cold-start comparison alone proves F1.

## I-3. Demo `IMAGE_TAG="demo"` floating tag combined with `imagePullPolicy: IfNotPresent` silently runs old code *(fixed)*

**Severity**: MEDIUM. Caused a full diagnostic loop where new gantry binaries
were pushed to ACR but kubelet kept running the old cached `gantry:demo` image
on every node, even after `kubectl rollout restart`.

**Fix**: [deploy/demo/infra/env.local](../deploy/demo/infra/env.local) now
leaves `IMAGE_TAG` unset so [lib.sh](../deploy/demo/infra/lib.sh) derives it
from `git describe --tags --always --dirty`. Every build gets a unique
immutable tag, kubelet always pulls the new image. Original line preserved
as a commented-out override for operators who deliberately want to pin.

## I-4. Demo `gantry-demo-1` resource group / `gantry-demo-1` AKS name hard-coded in `env.local` *(fixed)*

**Severity**: LOW. Made "deploy a brand new cluster" require multi-line
edits and risked colliding with previous still-existing resources.

**Fix**: [deploy/demo/infra/env.example](../deploy/demo/infra/env.example) now
exports a single `DEMO_SESSION` knob and derives `AZ_RESOURCE_GROUP` and
`AKS_NAME` from it. Bump `DEMO_SESSION` for a fresh cluster. The current demo
uses `DEMO_SESSION="2"` → `gantry-demo-2`.

## I-5. Demo Makefile harness targets honour Go test cache, silently re-prints stale output *(fixed)*

**Severity**: MEDIUM. Made it appear that cdsub fixes had not landed when in
fact the test had been short-circuited by Go's test result cache (the
`(cached)` marker in `go test` output) and was replaying a previous run's
proxy-delta numbers.

**Fix**: [deploy/demo/Makefile](../deploy/demo/Makefile) `harness-baseline`,
`harness-gantry-cold`, `harness-gantry-warm` now pass `-count=1` so live
phases never read from the test cache.

## I-6. Containerd socket on AKS is `0660 root:root`, gantry pod runs as UID 65532 *(demo-only fix applied)*

**Severity**: LOW for the production install (operators set fsGroup or
loosen perms or disable cdsub per the [daemonset.yaml](../deploy/daemonset.yaml)
comment block), HIGH-blocking for the demo until worked around.

**Fix in the demo only**: [deploy/demo/infra/41-deploy-gantry.sh](../deploy/demo/infra/41-deploy-gantry.sh)
patches the gantry container to `runAsUser: 0` and adds `DAC_OVERRIDE` +
`DAC_READ_SEARCH` capabilities so root inside the container can open both
`containerd.sock` (root-owned) and `libp2p/identity.key` (65532-owned, mode
0600). Production should use option (a) from the daemonset comment instead.
