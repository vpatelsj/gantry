//go:build demo

package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestFetchProxySummary(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"since":          "2026-05-15T12:00:00Z",
			"uptime_seconds": 42,
			"totals": map[string]any{
				"requests_completed": 3,
				"bytes_to_client":    99,
				"by_path_class": map[string]any{
					"blob": map[string]any{"requests": 2, "bytes": 90},
					"ping": map[string]any{"requests": 1, "bytes": 0},
				},
			},
		})
	}))
	defer server.Close()

	summary, err := FetchProxySummary(context.Background(), server.Client(), server.URL)
	if err != nil {
		t.Fatalf("FetchProxySummary: %v", err)
	}
	if summary.UptimeSeconds != 42 || summary.Totals.RequestsCompleted != 3 || summary.Totals.BytesToClient != 99 {
		t.Fatalf("unexpected summary: %+v", summary)
	}
	if got := summary.Totals.ByPathClass["blob"]; got.Requests != 2 || got.Bytes != 90 {
		t.Fatalf("blob totals = %+v", got)
	}
}

func TestDiffProxySummary(t *testing.T) {
	before := ProxySummary{Totals: ProxyTotals{RequestsCompleted: 10, BytesToClient: 100, ByPathClass: map[string]PathTotal{
		"blob": {Requests: 8, Bytes: 90},
		"ping": {Requests: 2, Bytes: 0},
	}}}
	after := ProxySummary{Totals: ProxyTotals{RequestsCompleted: 13, BytesToClient: 160, ByPathClass: map[string]PathTotal{
		"blob":            {Requests: 10, Bytes: 130},
		"manifest_by_tag": {Requests: 1, Bytes: 20},
		"ping":            {Requests: 2, Bytes: 0},
	}}}

	delta := DiffProxySummary(before, after)
	if delta.RequestsCompleted != 3 || delta.BytesToClient != 60 {
		t.Fatalf("delta totals = %+v", delta)
	}
	if got := delta.ByPathClass["blob"]; got.Requests != 2 || got.Bytes != 40 {
		t.Fatalf("blob delta = %+v", got)
	}
	if got := delta.ByPathClass["manifest_by_tag"]; got.Requests != 1 || got.Bytes != 20 {
		t.Fatalf("tag delta = %+v", got)
	}
}

func TestParseFirstContainerTimestamp(t *testing.T) {
	stamp, err := ParseFirstContainerTimestamp("\n2026-05-15T12:34:56.123456789Z\nready\n")
	if err != nil {
		t.Fatalf("ParseFirstContainerTimestamp: %v", err)
	}
	if stamp.Format(time.RFC3339Nano) != "2026-05-15T12:34:56.123456789Z" {
		t.Fatalf("timestamp = %s", stamp.Format(time.RFC3339Nano))
	}
}

func TestSummarizeLatencies(t *testing.T) {
	summary, err := SummarizeLatencies([]time.Duration{5 * time.Second, time.Second, 3 * time.Second, 2 * time.Second})
	if err != nil {
		t.Fatalf("SummarizeLatencies: %v", err)
	}
	if summary.P50 != 2*time.Second || summary.P95 != 5*time.Second || summary.P100 != 5*time.Second {
		t.Fatalf("latency summary = %+v", summary)
	}
}
