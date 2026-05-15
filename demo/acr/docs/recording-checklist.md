# Recording checklist

What to capture on screen during the demo, in order. Times are
approximate and assume the harness has already been validated end-to-end
in the developer-validation order (see
[../../../docs/demo-acr-harness.md](../../../docs/demo-acr-harness.md)).

## Pre-record

- [ ] Cluster is in baseline-clean state: gantry DaemonSet **deleted**
      and `hosts.toml` **absent** on every node. Verify with the
      `assert_no_hosts_toml` step that `40-baseline.sh` runs first; it
      will fail loud if either is wrong.
- [ ] Grafana dashboard tab open at `Gantry — ACR demo` (uid: `gantry-demo`),
      time range = `now-30m`. All eight panels render. If a panel is
      "No Data" *before* recording starts, swap or annotate it — the
      ConfigMap patch sets `containerd_socket: ""` so any cdsub-only
      series will be empty.
- [ ] Two terminals: one running scripts, one tailing
      `kubectl get pods -A -w`.
- [ ] Azure portal tabs:
        - ACR `Metrics` blade with `TotalPullCount` graphed.
        - Log Analytics workspace with the saved KQL queries
          (`acr-total-events.kql`, `acr-throttling.kql`).

## Recording flow

| step | what to show on screen                                                       | duration approx |
| ---- | ---------------------------------------------------------------------------- | --------------- |
| 1    | Setup intro + cluster size + ACR SKU. `kubectl get nodes` shows 20 Ready.    | 30 s            |
| 2    | `cat env.sh` (redact subscription/email).                                    | 15 s            |
| 3    | Run `./40-baseline.sh`. Show pods scheduling, image pulls in flight.         | until complete  |
| 4    | Wait for ACR ingest lag (skip in editing). Run `./41-record-baseline.sh`.    | ~5 min          |
| 5    | Switch to Azure portal: ACR `TotalPullCount` graph, KQL repo-events count.   | 30 s            |
| 6    | Switch to terminal: `cat artifacts/baseline-<RUN_ID>.json | jq`.             | 15 s            |
| 7    | `./50-build-gantry.sh && ./51a-deploy-gantry.sh && ./51b-install-hosts-toml.sh`. Show the preflight passing. | until complete  |
| 8    | Open Grafana — `p2p_dht_health_score` ≥ 0.7, all eight panels live.          | 20 s            |
| 9    | `./60-with-gantry.sh`. Show layer-digest zero-overlap assertion passing.     | until complete  |
| 10   | `./61-record-with-gantry.sh`. Same ingest-lag wait; in the meantime show    |                 |
|      | the Grafana dashboard (peer-fetch panel should be the dominant series).      | ~5 min          |
| 11   | Optional re-pull: `./61b-dashboard-replay.sh` (only if 61's numbers look    |                 |
|      | suspiciously low — Azure ingest occasionally lags > 5 min).                  | +5 min          |
| 12   | Switch to ACR portal: `TotalPullCount` for the cold-start window is much    |                 |
|      | lower; show the KQL repo-events count is 7–20× lower than baseline.          | 30 s            |
| 13   | `./62-cached-rerun.sh`. Show preflight cleanup logs (single node) then the  |                 |
|      | fleet-wide cleanup Job completing — call out that this is what makes the    |                 |
|      | warm-cache test honest (containerd's content store is empty per node).      | until complete  |
| 14   | `./63-record-cached.sh`. After ingest, show cache-hit panel dominating and  |                 |
|      | origin/peer panels essentially flat. Read the gate-pass message aloud.      | ~5 min          |
| 15   | `./70-compare.sh`. Read out the three-column table; emphasise the headline  |                 |
|      | repo-events ratio (baseline vs cold-start) and the warm-cache ≈ 0 ACR row.  | 1 min           |
| 16   | Outro: gantry CPU/mem footprint (last two rows of the table) — pre-empts    |                 |
|      | the "what does this cost" question.                                          | 30 s            |

## Cuts to make in editing

- The two ≥5-minute ingest waits between Phase 4 ↔ 5 and Phase 6 ↔ 6b
  cleanup. Replace each with a screen card "Waiting 5 min for Azure
  Monitor ingest…".
- The full image-build progress in `50-build-gantry.sh` (cut after the
  first layer pushes; reaffirm "image lands in ACR").
- The KQL editor scrolling — pre-pin both queries.

## Things to NOT cut

- The `assert_no_hosts_toml` early-fail check at the top of
  `40-baseline.sh`. This is the operator-visible proof that the
  baseline isn't quietly routing through gantry.
- The layer-digest zero-overlap assertion at the top of
  `60-with-gantry.sh`. This is the operator-visible proof that the new
  manifest *can't* be served from any local containerd cache.
- The single-node cleanup pre-flight at the top of `62-cached-rerun.sh`.
  This is the operator-visible proof that the warm-cache run can't
  silently look like "containerd as cache".
- The `./70-compare.sh` table read-out. The headline number is the
  ACR repo-events row.

## Post-record

- [ ] `./99-cleanup.sh` (off-camera). Confirm by typing `DELETE`.
- [ ] Verify the resource group is gone:
      `az group show -n ${RG_NAME}` returns 404.
- [ ] Stop billing — the $50/day budget alert subscription stays
      active until the RG is fully purged (~5 min async).
