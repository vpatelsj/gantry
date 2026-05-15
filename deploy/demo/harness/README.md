# Gantry ACR demo harness

Build-plan step 4 skeleton for the ACR counting-proxy demo.

This is intentionally a separate Go module and every Go source file is
guarded by the `demo` build tag, so it is not picked up by normal repo
tests. Run it through the demo-local Makefile:

```bash
make -C deploy/demo harness
```

Current scope:

- Fetch and decode proxy `/debug/summary`
- Diff proxy summary snapshots
- Parse the first in-container RFC3339 timestamp from workload logs
- Compute simple P50/P95/P100 duration summaries

The baseline phase exists as an explicit live test and is skipped by
default. It builds and pushes a fresh workload image, installs baseline
hosts.toml, runs the pull Job, diffs proxy summaries, and reports
pod-start timestamps:

```bash
make -C deploy/demo harness-baseline
```

By default the live test reads proxy summaries through `kubectl get
--raw` against the Kubernetes Service proxy. Set `PROXY_SUMMARY_URL`
only when you want to use an already-running local port-forward.

Gantry cold-start, cache-purge, and warm-cache phases are still future
build-plan steps.