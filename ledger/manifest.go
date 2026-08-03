package ledger

import (
	"encoding/json"
	"fmt"
)

// manifestVersion is the format version written into every manifest. Readers
// refuse anything higher, so an old binary fails loudly instead of
// misinterpreting a newer track.
const manifestVersion = 1

// Manifests are encoded as JSON. Manifests are small, are read far less often
// than payloads, and being able to inspect a broken track with a text editor is
// worth more during development than the bytes a binary encoding would save.
// The Version field is what allows that decision to be revisited.

// Object key layout. The functions below are the single place the layout is
// defined; nothing else in the package builds a key by hand.
//
// They are unexported: a Go consumer reaches a track through [Track], and
// tooling written against the raw store works from the layout documented in
// docs/ARCHITECTURE.md rather than from this package.
const (
	rootObject   = "root.manifest"
	headObject   = "delta/head"
	openPrefix   = "delta/open/"
	sealedPrefix = "delta/sealed-"
	groupPrefix  = "groups/"
)

// rootKey returns the key of a track's root manifest.
func rootKey(track TrackPath) string {
	return string(track) + "/" + rootObject
}

// headKey returns the key of a track's head pointer — the only mutable object
// the ledger writes.
func headKey(track TrackPath) string {
	return string(track) + "/" + headObject
}

// deltaKey returns the key of the nth delta manifest in the open region.
// Delta numbering is assigned by the ledger and is contiguous, which is what
// lets a reader probe forward without listing.
func deltaKey(track TrackPath, n uint64) string {
	return fmt.Sprintf("%s/%s%08d.manifest", track, openPrefix, n)
}

// sealedKey returns the key of the sealed manifest covering deltas first
// through last.
//
// The key names the range rather than a position deliberately. A seal whose
// root update fails leaves the manifest object behind; retrying it after more
// groups have arrived produces a *different* range, and so a different key. If
// the key were positional the retry would collide with the earlier object,
// ErrExist would be indistinguishable from success, and the root would end up
// pointing at a manifest missing the groups added since. Naming the range makes
// ErrExist mean exactly "this identical seal already landed".
func sealedKey(track TrackPath, first, last uint64) string {
	return fmt.Sprintf("%s/%s%08d-%08d.manifest", track, sealedPrefix, first, last)
}

// groupKey returns the storage key of a group's payload.
//
// Only the writer calls this. Readers take the key from [GroupInfo.ObjectKey]
// instead, because producer sequences are gappy and a derived key may name an
// object that was never written.
func groupKey(track TrackPath, ref GroupRef) string {
	return string(track) + "/" + groupPrefix + ref.String()
}

// rootManifest describes a track. It changes only when a manifest is sealed or
// the producer's epoch advances, so it is cheap to cache.
//
// It is the on-disk wire format and stays unexported: a Go consumer reads track
// metadata through [TrackMeta], and a reader in any language reads the JSON
// schema documented in docs/ARCHITECTURE.md. The layout is not part of the
// public Go API.
type rootManifest struct {
	Version int       `json:"version"`
	Track   TrackPath `json:"track"`

	// Timescale, TimeSource, MIME and Encoding mirror [TrackSchema]. They are
	// spelled out here rather than embedding it because these carry the wire
	// tags: the on-disk names are the stable contract, and an embedded public
	// type would marshal under its Go field names instead.
	Timescale  uint32     `json:"timescale"`
	TimeSource TimeSource `json:"timeSource"`
	MIME       string     `json:"mime,omitempty"`
	Encoding   string     `json:"encoding,omitempty"`

	// Epoch is the producer lifetime currently being written.
	Epoch uint64 `json:"epoch"`

	// Sealed lists the immutable manifests covering the track's history, in
	// commit order. Each entry summarizes its run, so a seek walking backwards
	// can identify the one run that may hold its answer and fetch only that —
	// rather than one request per run for the length of the recording.
	Sealed []sealedRef `json:"sealed"`

	// OpenFrom is the first delta number in the open region, that is, the
	// first delta not yet covered by a sealed manifest.
	OpenFrom uint64 `json:"openFrom"`

	CreatedAt int64 `json:"createdAt"`
}

// meta projects the root onto the public [TrackMeta]: the track's content
// schema and current epoch, with nothing about how the history is laid out on
// disk. Storage structure is not part of the public API.
func (m rootManifest) meta() TrackMeta {
	return TrackMeta{
		TrackSchema: TrackSchema{
			Timescale:  m.Timescale,
			TimeSource: m.TimeSource,
			MIME:       m.MIME,
			Encoding:   m.Encoding,
		},
		Track: m.Track,
		Epoch: m.Epoch,
	}
}

// sealedRef summarizes a sealed manifest so the root can be searched without
// fetching its contents.
//
// Every field is derived at seal time; nothing here is supplied by a caller.
type sealedRef struct {
	Key        string `json:"key"`
	FirstDelta uint64 `json:"firstDelta"`
	LastDelta  uint64 `json:"lastDelta"`
	Groups     int    `json:"groups"`

	// MediaStart and MediaEnd bound the run in media time. MediaEnd reaches
	// the end of the last group whose duration is known, and equals the final
	// anchor otherwise.
	MediaStart int64 `json:"mediaStart"`
	MediaEnd   int64 `json:"mediaEnd"`

	// WallclockStart and WallclockEnd bound the run across only those groups
	// that carry an anchor. Both are zero when none does.
	WallclockStart int64 `json:"wallclockStart,omitempty"`
	WallclockEnd   int64 `json:"wallclockEnd,omitempty"`

	First GroupRef `json:"first"`
	Last  GroupRef `json:"last"`
}

// deltaManifest commits one or more groups. Writing it *is* the commit: it is
// immutable and written atomically, so a reader that finds one is reading
// committed state even if the head pointer has not caught up.
//
// Groups normally holds a single entry — one commit per sealed group keeps
// visibility latency at its floor. It is a slice so that bulk backfill can
// commit a batch without paying one round trip per group.
type deltaManifest struct {
	Version int         `json:"version"`
	Seq     uint64      `json:"seq"`
	Groups  []GroupInfo `json:"groups"`

	CommittedAt int64 `json:"committedAt"`
}

// sealedManifest is a rotated run of delta manifests folded into one immutable
// object, so that reading history costs one request per sealed manifest rather
// than one per group.
type sealedManifest struct {
	Version    int         `json:"version"`
	Track      TrackPath   `json:"track"`
	Seq        uint64      `json:"seq"`
	FirstDelta uint64      `json:"firstDelta"`
	LastDelta  uint64      `json:"lastDelta"`
	Groups     []GroupInfo `json:"groups"`

	SealedAt int64 `json:"sealedAt"`
}

// head points at the newest committed delta.
//
// It is a discovery cache, not a transaction boundary. A reader joining a track
// uses it to avoid probing from zero; a reader already tailing ignores it
// entirely. Because it may lag its deltas — or be lost outright — nothing may
// depend on it for correctness.
type head struct {
	Version int    `json:"version"`
	Delta   uint64 `json:"delta"`

	// Latest is the newest group committed, so a joining reader knows where
	// the track stands without fetching a manifest.
	Latest GroupRef `json:"latest"`

	UpdatedAt int64 `json:"updatedAt"`
}

// summarize reduces a sealed manifest to the reference stored in the root.
// It returns the zero value when the manifest has no groups.
//
// Only the writer folds sealed manifests, so this stays unexported; readers
// consume the resulting [sealedRef] values from [rootManifest.Sealed].
func (m sealedManifest) summarize(key string) sealedRef {
	ref := sealedRef{
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
	ref.MediaStart, ref.MediaEnd = first.MediaTime, last.mediaEnd()

	// A run spans more than one epoch whenever a producer restarts mid-run,
	// and an epoch resets media time outright. Widen the bounds over every
	// group so a range search cannot step over one.
	for _, g := range m.Groups {
		ref.MediaStart = min(ref.MediaStart, g.MediaTime)
		ref.MediaEnd = max(ref.MediaEnd, g.mediaEnd())

		// Wallclock is optional, so the bounds cover only the groups that
		// carry an anchor. Zero means "no anchor", never "the epoch".
		if !g.hasWallclock() {
			continue
		}
		if ref.WallclockStart == 0 || g.Wallclock < ref.WallclockStart {
			ref.WallclockStart = g.Wallclock
		}
		ref.WallclockEnd = max(ref.WallclockEnd, g.Wallclock)
	}

	return ref
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
	if v := version(out); v > manifestVersion {
		return out, fmt.Errorf("%w: found %d, understand up to %d", ErrUnsupportedVersion, v, manifestVersion)
	}

	return out, nil
}
