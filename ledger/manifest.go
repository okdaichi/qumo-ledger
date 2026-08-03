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
// A track is a tree of epoch logs, one per producer lifetime:
//
//	<track>/root.manifest                  the track root (schema, LatestEpoch)
//	<track>/e000001/log.manifest           epoch 1's log root (Sealed, OpenFrom)
//	<track>/e000001/delta/head             epoch 1's head pointer (mutable)
//	<track>/e000001/delta/open/00000042.manifest
//	<track>/e000001/delta/sealed-00000001-00000003.manifest
//	<track>/e000001/groups/g00000042       epoch 1's payload (sequence in the key)
//
// They are unexported: a Go consumer reaches a track through [Track], and
// tooling written against the raw store works from the layout documented in
// docs/ARCHITECTURE.md rather than from this package.
const (
	rootObject   = "root.manifest"
	logObject    = "log.manifest"
	headObject   = "delta/head"
	openPrefix   = "delta/open/"
	sealedPrefix = "delta/sealed-"
	groupPrefix  = "groups/"
)

// epochDir is the per-epoch path segment under which an epoch's log, head, and
// groups live. Epoch is structural in the path, which is why a group's payload
// key carries only its sequence.
func epochDir(epoch uint64) string { return fmt.Sprintf("e%06d", epoch) }

// rootKey returns the key of a track's root manifest.
func rootKey(track TrackPath) string {
	return string(track) + "/" + rootObject
}

// epochLogKey returns the key of an epoch's log manifest — the per-epoch root
// that holds its sealed index and open region.
func epochLogKey(track TrackPath, epoch uint64) string {
	return fmt.Sprintf("%s/%s/%s", track, epochDir(epoch), logObject)
}

// headKey returns the key of an epoch's head pointer — the only mutable object
// the ledger writes within an epoch.
func headKey(track TrackPath, epoch uint64) string {
	return fmt.Sprintf("%s/%s/%s", track, epochDir(epoch), headObject)
}

// deltaKey returns the key of the nth delta manifest in an epoch's open region.
// Delta numbering is assigned by the ledger and is contiguous within an epoch,
// which is what lets a reader probe forward without listing.
func deltaKey(track TrackPath, epoch, n uint64) string {
	return fmt.Sprintf("%s/%s/%s%08d.manifest", track, epochDir(epoch), openPrefix, n)
}

// sealedKey returns the key of the sealed manifest covering deltas first
// through last within an epoch.
//
// The key names the range rather than a position deliberately. A seal whose
// root update fails leaves the manifest object behind; retrying it after more
// groups have arrived produces a *different* range, and so a different key. If
// the key were positional the retry would collide with the earlier object,
// ErrExist would be indistinguishable from success, and the root would end up
// pointing at a manifest missing the groups added since. Naming the range makes
// ErrExist mean exactly "this identical seal already landed".
func sealedKey(track TrackPath, epoch, first, last uint64) string {
	return fmt.Sprintf("%s/%s/%s%08d-%08d.manifest", track, epochDir(epoch), sealedPrefix, first, last)
}

// groupKey returns the storage key of a group's payload.
//
// Only the writer calls this. Readers take the key from [GroupInfo.ObjectKey]
// instead, because producer sequences are gappy and a derived key may name an
// object that was never written.
//
// The epoch is the path segment and the sequence is the file name, so the key
// no longer matches [GroupID.String] suffix for suffix. That form remains the
// cross-epoch identity for logs and persisted positions, not the storage key.
func groupKey(track TrackPath, id GroupID) string {
	return fmt.Sprintf("%s/%s/%sg%08d", track, epochDir(id.Epoch()), groupPrefix, id.Sequence())
}

// trackRoot describes a track: its immutable content schema and the latest
// producer epoch. It is the only track-level object, and it changes only when a
// new epoch is created, so it is cheap to cache and rarely contended.
//
// It is the on-disk wire format and stays unexported: a Go consumer reads track
// metadata through [TrackInfo], and a reader in any language reads the JSON
// schema documented in docs/ARCHITECTURE.md. The layout is not part of the
// public Go API.
type trackRoot struct {
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

	// LatestEpoch is the newest producer epoch the track has. Epochs are
	// created one at a time as LatestEpoch+1, so the set in existence is always
	// 1..LatestEpoch, modulo a transient orphan from a creation that did not
	// finish — which the next writer adopts.
	LatestEpoch uint64 `json:"latestEpoch"`

	CreatedAt int64 `json:"createdAt"`
}

// schema projects just the content schema off a track root, for handles that
// need the immutable schema but not the latest epoch.
func (m trackRoot) schema() TrackSchema {
	return TrackSchema{
		Timescale:  m.Timescale,
		TimeSource: m.TimeSource,
		MIME:       m.MIME,
		Encoding:   m.Encoding,
	}
}

// epochLogRoot describes one epoch's append-only log: its sealed index and open
// region. Each epoch is its own log under <track>/e%06d/, with its own head and
// delta numbering that starts at zero.
type epochLogRoot struct {
	Version int       `json:"version"`
	Track   TrackPath `json:"track"`

	// Epoch is the producer lifetime this log records. It must match the epoch
	// segment of the key the log was fetched from.
	Epoch uint64 `json:"epoch"`

	// Sealed lists the immutable manifests covering the epoch's history, in
	// commit order. Each entry summarizes its run, so a seek walking backwards
	// can identify the one run that may hold its answer and fetch only that.
	Sealed []sealedRef `json:"sealed"`

	// OpenFrom is the first delta number in the open region, that is, the
	// first delta not yet covered by a sealed manifest.
	OpenFrom uint64 `json:"openFrom"`

	CreatedAt int64 `json:"createdAt"`
}

// sealedRef summarizes a sealed manifest so the log root can be searched without
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

	First GroupID `json:"first"`
	Last  GroupID `json:"last"`
}

// deltaManifest commits one or more groups. Writing it *is* the commit: it is
// immutable and written atomically, so a reader that finds one is reading
// committed state even if the head pointer has not caught up.
//
// Groups normally holds a single entry — one commit per sealed group keeps
// visibility latency at its bottom. It is a slice so that bulk backfill can
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
//
// Epoch is recorded so a manifest filed under the wrong epoch directory is
// caught rather than trusted.
type sealedManifest struct {
	Version    int         `json:"version"`
	Track      TrackPath   `json:"track"`
	Epoch      uint64      `json:"epoch"`
	Seq        uint64      `json:"seq"`
	FirstDelta uint64      `json:"firstDelta"`
	LastDelta  uint64      `json:"lastDelta"`
	Groups     []GroupInfo `json:"groups"`

	SealedAt int64 `json:"sealedAt"`
}

// head points at the newest committed delta within an epoch.
//
// It is a discovery cache, not a transaction boundary. A reader joining an
// epoch uses it to avoid probing from zero; a reader already tailing ignores it
// entirely. Because it may lag its deltas — or be lost outright — nothing may
// depend on it for correctness.
type head struct {
	Version int    `json:"version"`
	Delta   uint64 `json:"delta"`

	// Latest is the newest group committed, so a joining reader knows where
	// the epoch stands without fetching a manifest.
	Latest GroupID `json:"latest"`

	UpdatedAt int64 `json:"updatedAt"`
}

// summarize reduces a sealed manifest to the reference stored in the log root.
// It returns the zero value when the manifest has no groups.
//
// Within one epoch, media time is monotonic — [Writer.checkOrder] enforces it —
// so the run's media bounds come straight from its first and last group.
// Wallclock is optional, so its bounds cover only the groups that carry an
// anchor.
//
// Only the writer folds sealed manifests, so this stays unexported; readers
// consume the resulting [sealedRef] values from [epochLogRoot.Sealed].
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
	ref.First = first.ID
	ref.Last = last.ID
	ref.MediaStart, ref.MediaEnd = first.MediaTime, last.mediaEnd()

	for _, g := range m.Groups {
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
