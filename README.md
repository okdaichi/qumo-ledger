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
objects, _ := fsstore.New("/var/lib/qumo-ledger")

// Create establishes a new track, like os.Create: it fixes the schema once and
// returns a handle. Re-creating an existing track fails rather than truncating
// it — a track is an immutable, append-only log. The empty Config is the common
// case: every setting has a documented default and belongs to a deployment, not
// a track.
track, _ := ledger.Create(ctx, objects, "live/cam1/video", ledger.TrackSchema{
    Timescale:  90000,                  // 90 kHz, the usual video timescale
    TimeSource: ledger.TimeSourceFrame, // timestamps came from the data itself
    MIME:       "video/mp4",
    Encoding:   "fmp4",
}, ledger.Config{Logger: log})

writer, _ := track.Writer(ctx)

// Append is the common case: hand the ledger a duration and it derives
// sequence, media time, and wallclock for you.
writer.Append(ctx, 180000, payload) // two seconds at 90 kHz

// AppendGroup is the escape hatch — a producer's own numbering, a dropped
// group, or a media anchor that is not simply the previous group's end. The
// writer stamps the epoch; advance it with Writer.NewEpoch on a restart.
writer.AppendGroup(ctx, ledger.GroupInfo{
    ID:        ledger.NewGroupID(0, 42), // sequence; the epoch is stamped by the writer
    MediaTime: 7560000, // media anchor, in timescale units
    Duration:  180000,  // optional: two seconds at 90 kHz
    Wallclock: w0,      // optional: wallclock anchor, Unix nanoseconds
}, payload)

// Open references an existing track, like os.Open. Unlike os.Open the handle is
// both read- and write-capable, so a writer that crashed mid-append resumes here.
opened, _ := ledger.Open(ctx, objects, "live/cam1/video", ledger.Config{Logger: log})
reader, _ := opened.Reader(ctx)

// A window, not a point. Run the same window over a video track and a sensor
// track and the two recordings line up.
for group, err := range reader.RangeWallclock(ctx, from, to) {
    if err != nil {
        return err   // the error is terminal; nothing follows it
    }
    frames, _ := reader.ReadGroup(ctx, group)
    _ = frames
}

// Following is polling, because object stores do not push — but the poll loop
// is the caller's. The Reader streams the track in commit order and reports its
// position, so a follower can persist it and resume.
reader.SeekTip(ctx)
ticker := time.NewTicker(ledger.DefaultPollInterval)
defer ticker.Stop()
for {
    group, err := reader.Next(ctx)
    if errors.Is(err, io.EOF) {
        select {
        case <-ctx.Done():
            return ctx.Err()
        case <-ticker.C:
            continue
        }
    }
    if err != nil {
        return err
    }
    save(reader.Position()) // survives a restart: it is the GroupID text
}
```

A group is **anchored** on two timelines rather than described as an interval,
because groups are serial — the start of one is the end of the last. Media time
(`MediaTime`) is exact and skew-free but relative to one track's origin; wallclock
(`Wallclock`) is absolute and comparable across tracks, which is what makes *"the video
and the sensor readings at 14:32"* a question with an answer.

`Duration` and `Wallclock` are optional. `Duration` is stored rather than derived from
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

## Examples

This repository is a library and ships no binaries. Two runnable examples live
under `examples/` — reference code, not supported tools:

```bash
go run ./examples/inspect-follow inspect -root /var/lib/qumo-ledger -track live/cam1/video -groups
go run ./examples/stream-server    -root /var/lib/qumo-ledger -track live/cam1/video   # HLS (.m3u8) + DASH (.mpd)
```

A supported client CLI for accessing a ledger lives in a separate repository.

## Packages

| | |
|---|---|
| `ledger` | The core. Depends on no transport and no cloud SDK. |
| `ledger/store` | The storage contract: conditional create, compare-and-swap, optional listing. A leaf, so a backend never imports the ledger. |
| `ledger/store/memstore` | In-memory backend; also the reference implementation. |
| `ledger/store/fsstore` | Local filesystem backend. |
| `ledger/store/storetest` | Conformance suite every backend must pass. |
| `stream` | HLS and DASH renderers over a ledger track — derived views, served over HTTP. |
| `examples/` | Runnable examples (a reference reader; a dev HLS/DASH server). Not supported entrypoints. |

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
