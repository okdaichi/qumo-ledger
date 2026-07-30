package ledger

import (
	"encoding/json"
	"fmt"
)

// ManifestVersion is the format version written into every manifest. Readers
// refuse anything higher, so an old binary fails loudly instead of
// misinterpreting a newer track.
const ManifestVersion = 1

// Manifests are encoded as JSON. Manifests are small, are read far less often
// than payloads, and being able to inspect a broken track with a text editor is
// worth more during development than the bytes a binary encoding would save.
// The Version field is what allows that decision to be revisited.

// Object key layout. These functions are the single place the layout is
// defined; nothing else in the package builds a key by hand.
const (
	rootObject   = "root.manifest"
	headObject   = "delta/head"
	openPrefix   = "delta/open/"
	sealedPrefix = "delta/sealed-"
	groupPrefix  = "groups/"
)

// RootKey returns the key of a track's root manifest.
func RootKey(track TrackPath) string {
	return string(track) + "/" + rootObject
}

// HeadKey returns the key of a track's head pointer — the only mutable object
// the ledger writes.
func HeadKey(track TrackPath) string {
	return string(track) + "/" + headObject
}

// DeltaKey returns the key of the nth delta manifest in the open region.
// Delta numbering is assigned by the ledger and is contiguous, which is what
// lets a reader probe forward without listing.
func DeltaKey(track TrackPath, n uint64) string {
	return fmt.Sprintf("%s/%s%08d.manifest", track, openPrefix, n)
}

// SealedKey returns the key of the nth sealed manifest.
func SealedKey(track TrackPath, n uint64) string {
	return fmt.Sprintf("%s/%s%06d.manifest", track, sealedPrefix, n)
}

// GroupKey returns the storage key of a group's payload.
//
// Readers should take the key from [GroupMeta.Object] rather than calling this,
// because producer sequences are gappy and a derived key may name an object
// that was never written. It is exported for writers and for tooling that
// needs to reason about layout.
func GroupKey(track TrackPath, ref GroupRef) string {
	return string(track) + "/" + groupPrefix + ref.String()
}

// RootManifest describes a track. It changes only when a manifest is sealed or
// the producer's epoch advances, so it is cheap to cache.
type RootManifest struct {
	Version int       `json:"version"`
	Track   TrackPath `json:"track"`

	// Timescale, TimeSource, MIME and Encoding mirror TrackConfig.
	Timescale  uint32     `json:"timescale"`
	TimeSource TimeSource `json:"timeSource"`
	MIME       string     `json:"mime,omitempty"`
	Encoding   string     `json:"encoding,omitempty"`

	// Epoch is the producer lifetime currently being written.
	Epoch uint64 `json:"epoch"`

	// Sealed lists the immutable manifests covering the track's history, in
	// order. Each entry carries enough summary for a reader to skip it without
	// fetching it, which is what keeps a time seek from degrading into a scan
	// of the whole history.
	Sealed []SealedRef `json:"sealed"`

	// OpenFrom is the first delta number in the open region, that is, the
	// first delta not yet covered by a sealed manifest.
	OpenFrom uint64 `json:"openFrom"`

	CreatedAt int64 `json:"createdAt"`
}

// SealedRef summarizes a sealed manifest so the root can be searched without
// fetching its contents.
type SealedRef struct {
	Key        string `json:"key"`
	FirstDelta uint64 `json:"firstDelta"`
	LastDelta  uint64 `json:"lastDelta"`
	Groups     int    `json:"groups"`

	// The covered ranges, mirroring GroupMeta so a reader can binary-search
	// either timeline.
	T0 int64 `json:"t0"`
	T1 int64 `json:"t1"`
	W0 int64 `json:"w0"`
	W1 int64 `json:"w1"`

	First GroupRef `json:"first"`
	Last  GroupRef `json:"last"`
}

// DeltaManifest commits one or more groups. Writing it *is* the commit: it is
// immutable and written atomically, so a reader that finds one is reading
// committed state even if the head pointer has not caught up.
//
// Groups normally holds a single entry — one commit per sealed group keeps
// visibility latency at its floor. It is a slice so that bulk backfill can
// commit a batch without paying one round trip per group.
type DeltaManifest struct {
	Version int         `json:"version"`
	Seq     uint64      `json:"seq"`
	Groups  []GroupMeta `json:"groups"`

	CommittedAt int64 `json:"committedAt"`
}

// SealedManifest is a rotated run of delta manifests folded into one immutable
// object, so that reading history costs one request per sealed manifest rather
// than one per group.
type SealedManifest struct {
	Version    int         `json:"version"`
	Track      TrackPath   `json:"track"`
	Seq        uint64      `json:"seq"`
	FirstDelta uint64      `json:"firstDelta"`
	LastDelta  uint64      `json:"lastDelta"`
	Groups     []GroupMeta `json:"groups"`

	SealedAt int64 `json:"sealedAt"`
}

// Head points at the newest committed delta.
//
// It is a discovery cache, not a transaction boundary. A reader joining a track
// uses it to avoid probing from zero; a reader already tailing ignores it
// entirely. Because it may lag its deltas — or be lost outright — nothing may
// depend on it for correctness.
type Head struct {
	Version int    `json:"version"`
	Delta   uint64 `json:"delta"`

	// Latest is the newest group committed, so a joining reader knows where
	// the track stands without fetching a manifest.
	Latest GroupRef `json:"latest"`

	UpdatedAt int64 `json:"updatedAt"`
}

// Summarize reduces a sealed manifest to the reference stored in the root.
// It returns the zero value when the manifest has no groups.
func (m SealedManifest) Summarize(key string) SealedRef {
	ref := SealedRef{
		Key:        key,
		FirstDelta: m.FirstDelta,
		LastDelta:  m.LastDelta,
		Groups:     len(m.Groups),
	}
	if len(m.Groups) == 0 {
		return ref
	}

	first, last := m.Groups[0], m.Groups[len(m.Groups)-1]
	ref.First, ref.Last = first.GroupRef, last.GroupRef
	ref.T0, ref.T1 = first.T0, last.T1
	ref.W0, ref.W1 = first.W0, last.W1

	// Groups are appended in commit order, which is not necessarily time
	// order — a producer may backfill, and an epoch change resets media time
	// entirely. Widen the bounds so a range search cannot miss a group.
	for _, g := range m.Groups {
		ref.T0 = min(ref.T0, g.T0)
		ref.T1 = max(ref.T1, g.T1)
		ref.W0 = min(ref.W0, g.W0)
		ref.W1 = max(ref.W1, g.W1)
	}

	return ref
}

// Covers reports whether the sealed run may contain the given wallclock
// instant. It is deliberately inclusive: a false positive costs one wasted
// fetch, whereas a false negative silently loses data.
func (r SealedRef) Covers(unixNano int64) bool {
	return unixNano >= r.W0 && unixNano < r.W1
}

func encodeManifest(v any) ([]byte, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("ledger: encode manifest: %w", err)
	}

	return data, nil
}

func decodeManifest[T any](data []byte, version func(T) int) (T, error) {
	var out T
	if err := json.Unmarshal(data, &out); err != nil {
		return out, fmt.Errorf("ledger: decode manifest: %w", err)
	}
	if v := version(out); v > ManifestVersion {
		return out, fmt.Errorf("%w: found %d, understand up to %d", ErrUnsupportedVersion, v, ManifestVersion)
	}

	return out, nil
}
