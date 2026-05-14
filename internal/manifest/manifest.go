// Package manifest parses OCI v1 / Docker v2 schema-2 image manifests
// just enough to extract the layer and config digests they reference.
//
// This is a deliberately narrow parser. We do NOT validate the full
// OCI schema, do NOT cross-check media types, and do NOT verify
// signatures. The bytes have already been digest-verified by the
// cache pipeline before they reach this code, and containerd is the
// authoritative consumer that performs full validation. The only
// consumer here is the mirror's speculative layer-prefetch path
// (§5.2 detailed-design.md L332 / archecture.md L180).
package manifest

import (
	"encoding/json"
	"fmt"

	"github.com/gantry/gantry/internal/digest"
)

// schema is the subset of the OCI / Docker schema-2 manifest layout
// the prefetch path needs.
type schema struct {
	SchemaVersion int          `json:"schemaVersion"`
	MediaType     string       `json:"mediaType"`
	Config        descriptor   `json:"config"`
	Layers        []descriptor `json:"layers"`
	// Manifests is populated for image indexes (multi-arch manifest
	// lists). When non-empty, the body is an index and we skip
	// prefetch: containerd will request the architecture-specific
	// manifest next.
	Manifests []descriptor `json:"manifests"`
}

type descriptor struct {
	MediaType string   `json:"mediaType"`
	Digest    string   `json:"digest"`
	Size      int64    `json:"size"`
	URLs      []string `json:"urls"`
}

// ChildDigests parses body as an OCI / Docker schema-2 image manifest
// and returns the digests of every content blob the manifest
// references — its config blob plus every layer descriptor. The
// returned slice preserves source order: config first, then layers
// top-to-bottom (which is also the order containerd will request
// them).
//
// When body is an image index (manifest list) the function returns
// (nil, nil): no prefetch can happen until containerd requests the
// architecture-specific manifest.
//
// Foreign-layer descriptors (those with a non-empty `urls` array) are
// skipped: they point at non-OCI hosts (Microsoft base layers) and
// MUST NOT be fetched through Gantry.
//
// The function does not error on individual malformed digest strings
// inside the manifest; those entries are silently skipped. A parse
// failure on the manifest envelope itself is returned as an error.
func ChildDigests(body []byte) ([]digest.Digest, error) {
	var m schema
	if err := json.Unmarshal(body, &m); err != nil {
		return nil, fmt.Errorf("manifest: parse: %w", err)
	}
	// Image index detection: index has .manifests, image manifest has
	// .layers. If both happen to be populated, prefer image-manifest
	// interpretation (defensive against weird hand-crafted bodies).
	if len(m.Manifests) > 0 && len(m.Layers) == 0 {
		return nil, nil
	}
	out := make([]digest.Digest, 0, 1+len(m.Layers))
	if m.Config.Digest != "" {
		if d, err := digest.Parse(m.Config.Digest); err == nil {
			out = append(out, d)
		}
	}
	for _, l := range m.Layers {
		if l.Digest == "" {
			continue
		}
		if len(l.URLs) > 0 {
			// Foreign layer (Windows base, Microsoft-hosted) — skip.
			continue
		}
		if d, err := digest.Parse(l.Digest); err == nil {
			out = append(out, d)
		}
	}
	return out, nil
}

// IsImageIndex reports whether body parses as an OCI / Docker schema-2
// image index (manifest list). Provided for callers that want to
// short-circuit before walking child digests.
func IsImageIndex(body []byte) bool {
	var m schema
	if err := json.Unmarshal(body, &m); err != nil {
		return false
	}
	return len(m.Manifests) > 0 && len(m.Layers) == 0
}
