//go:build demo

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"
)

type PhaseName string

const (
	PhaseBaseline   PhaseName = "baseline"
	PhaseGantryCold PhaseName = "gantry_cold"
	PhaseGantryWarm PhaseName = "gantry_warm"
)

type ProxySummary struct {
	Since         time.Time            `json:"since"`
	UptimeSeconds int64                `json:"uptime_seconds"`
	Totals        ProxyTotals          `json:"totals"`
	RawClasses    map[string]PathTotal `json:"-"`
}

type ProxyTotals struct {
	RequestsCompleted uint64               `json:"requests_completed"`
	BytesToClient     uint64               `json:"bytes_to_client"`
	ByPathClass       map[string]PathTotal `json:"by_path_class"`
}

type PathTotal struct {
	Requests uint64 `json:"requests"`
	Bytes    uint64 `json:"bytes"`
}

type summaryWire struct {
	Since         string      `json:"since"`
	UptimeSeconds int64       `json:"uptime_seconds"`
	Totals        ProxyTotals `json:"totals"`
}

func FetchProxySummary(ctx context.Context, client *http.Client, summaryURL string) (ProxySummary, error) {
	if client == nil {
		client = http.DefaultClient
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, summaryURL, nil)
	if err != nil {
		return ProxySummary{}, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return ProxySummary{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return ProxySummary{}, fmt.Errorf("summary returned HTTP %d", resp.StatusCode)
	}

	var wire summaryWire
	if err := json.NewDecoder(resp.Body).Decode(&wire); err != nil {
		return ProxySummary{}, err
	}
	since, err := time.Parse(time.RFC3339, wire.Since)
	if err != nil {
		return ProxySummary{}, fmt.Errorf("parse since %q: %w", wire.Since, err)
	}
	return ProxySummary{
		Since:         since,
		UptimeSeconds: wire.UptimeSeconds,
		Totals:        wire.Totals,
		RawClasses:    wire.Totals.ByPathClass,
	}, nil
}

func DiffProxySummary(before, after ProxySummary) ProxyTotals {
	classes := map[string]struct{}{}
	for class := range before.Totals.ByPathClass {
		classes[class] = struct{}{}
	}
	for class := range after.Totals.ByPathClass {
		classes[class] = struct{}{}
	}

	delta := ProxyTotals{ByPathClass: make(map[string]PathTotal, len(classes))}
	delta.RequestsCompleted = subtractUint(after.Totals.RequestsCompleted, before.Totals.RequestsCompleted)
	delta.BytesToClient = subtractUint(after.Totals.BytesToClient, before.Totals.BytesToClient)
	for class := range classes {
		beforeClass := before.Totals.ByPathClass[class]
		afterClass := after.Totals.ByPathClass[class]
		delta.ByPathClass[class] = PathTotal{
			Requests: subtractUint(afterClass.Requests, beforeClass.Requests),
			Bytes:    subtractUint(afterClass.Bytes, beforeClass.Bytes),
		}
	}
	return delta
}

func ParseFirstContainerTimestamp(logText string) (time.Time, error) {
	for _, line := range strings.Split(logText, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		stamp, err := time.Parse(time.RFC3339Nano, line)
		if err != nil {
			return time.Time{}, fmt.Errorf("first non-empty log line is not RFC3339Nano: %w", err)
		}
		return stamp, nil
	}
	return time.Time{}, errors.New("no timestamp line found")
}

type LatencySummary struct {
	P50  time.Duration `json:"p50"`
	P95  time.Duration `json:"p95"`
	P100 time.Duration `json:"p100"`
}

func SummarizeLatencies(samples []time.Duration) (LatencySummary, error) {
	if len(samples) == 0 {
		return LatencySummary{}, errors.New("no latency samples")
	}
	sorted := append([]time.Duration(nil), samples...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	return LatencySummary{
		P50:  percentileNearestRank(sorted, 0.50),
		P95:  percentileNearestRank(sorted, 0.95),
		P100: sorted[len(sorted)-1],
	}, nil
}

func percentileNearestRank(sorted []time.Duration, quantile float64) time.Duration {
	if len(sorted) == 1 {
		return sorted[0]
	}
	index := int(quantile*float64(len(sorted))+0.999999999) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(sorted) {
		index = len(sorted) - 1
	}
	return sorted[index]
}

func subtractUint(after, before uint64) uint64 {
	if after < before {
		return 0
	}
	return after - before
}
