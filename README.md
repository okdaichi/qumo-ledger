# qumo-ledger

The temporal streaming platform. Store, replay, and stream time-oriented data.

> **Experimental.** The storage format is not stable and there is no
> compatibility promise yet.

qumo-ledger stores temporal data — video, audio, logs, sensor readings — as
immutable objects described by manifest objects. It takes the append-only
ordered log from Kafka and the independently-decodable segment from HLS, and
makes a **Group** both at once: the unit of independent decoding *and* the unit
of storage.

There is no database to run. The manifest *is* the source of truth, so a track
is complete and self-describing wherever you copy it, and a reader with nothing
but object-store credentials can seek and replay it with no ledger process
running anywhere.

## What it is not

It is not a live path. A group can only be stored once sealed, so replay through
the ledger is inherently behind a relay serving the same frames from memory.
Live delivery belongs to a relay such as [qumo](https://github.com/qumo-dev/qumo);
the ledger serves what has already happened. See
[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for why that boundary is drawn
where it is.

## Install

```bash
go get github.com/okdaichi/qumo-ledger
```

## Usage

```go
store, _ := fsstore.New("/var/lib/qumo-ledger")

writer, _ := ledger.CreateTrack(ctx, store, "live/cam1/video", ledger.TrackConfig{
    Timescale:  90000,                  // 90 kHz, the usual video timescale
    TimeSource: ledger.TimeSourceFrame, // timestamps came from the data itself
    MIME:       "video/mp4",
    Encoding:   "fmp4",
})

// A sealed group. Only T0 is required — Duration and W0 are optional.
writer.AppendGroup(ctx, ledger.GroupMeta{
    GroupRef:    ledger.GroupRef{Epoch: 1, Sequence: 42},
    T0:          7560000, // media anchor, in timescale units
    Duration:    180000,  // optional: two seconds at 90 kHz
    W0:          w0,      // optional: wallclock anchor, Unix nanoseconds
    ObjectCount: 60,
}, payload)

// Readers need only store access.
reader, _ := ledger.OpenReader(ctx, store, "live/cam1/video")

// A window, not a point. Run the same window over a video track and a sensor
// track and the two recordings line up.
for group, err := range reader.RangeWallclock(ctx, from, to) {
    if err != nil {
        return err   // the error is terminal; nothing follows it
    }
    frames, _ := reader.ReadGroup(ctx, group)
    _ = frames
}

// Following is polling: object stores do not push. Each update carries the
// cursor that resumes after it, so a follower can persist its position.
tip, _ := reader.Tip(ctx)
for update, err := range reader.Follow(ctx, tip, ledger.DefaultPollInterval) {
    if err != nil {
        return err
    }
    save(update.Cursor) // survives a restart: it marshals as text
}
```

A group is **anchored** on two timelines rather than described as an interval,
because groups are serial — the start of one is the end of the last. Media time
(`T0`) is exact and skew-free but relative to one track's origin; wallclock
(`W0`) is absolute and comparable across tracks, which is what makes *"the video
and the sensor readings at 14:32"* a question with an answer.

`Duration` and `W0` are optional. `Duration` is stored rather than derived from
the next group because that derivation breaks across a dropped group and is
undefined for the newest one — and it is exactly what HLS `EXTINF` and DASH `@d`
need. A group that contradicts its predecessor is refused at append time.

## Object layout

```
<track-path>/
  root.manifest                 track metadata; changes only on seal
  delta/
    head                        ← the only mutable object in the store
    open/00000042.manifest      immutable, one per commit
    sealed-000001.manifest      immutable, rotated by size
  groups/
    e000001-g00000042           immutable payload
```

Writing a delta manifest *is* the commit. `head` is a discovery cache that may
lag or vanish without affecting correctness — a reader that finds it stale
probes forward and catches up.

## CLI

```bash
mage build

./bin/qumo-ledger inspect -root /var/lib/qumo-ledger -track live/cam1/video -groups
./bin/qumo-ledger follow  -root /var/lib/qumo-ledger -track live/cam1/video
```

The CLI is a convenience, not a component: it uses the same public API any
reader would.

## Packages

| | |
|---|---|
| `ledger` | The core. Depends on no transport and no cloud SDK. |
| `ledger/store` | The storage contract: conditional create, compare-and-swap, optional listing and presigning. A leaf, so a backend never imports the ledger. |
| `ledger/store/memstore` | In-memory backend; also the reference implementation. |
| `ledger/store/fsstore` | Local filesystem backend. |
| `ledger/store/storetest` | Conformance suite every backend must pass. |

## Development

```bash
mage test     # go test -race ./...
mage cover    # coverage.out + summary
mage check    # tidy, lint, test
```

## Related

- [qumo](https://github.com/qumo-dev/qumo) — Media over QUIC relay
- [gomoqt](https://github.com/qumo-dev/gomoqt) — Media over QUIC Transport in Go

## License

Apache 2.0. See [LICENSE](LICENSE).
