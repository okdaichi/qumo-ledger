package ledger_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/okdaichi/qumo-ledger/ledger"
	"github.com/okdaichi/qumo-ledger/ledger/store/memstore"
)

// videoTimescale is the usual 90 kHz media clock; ticksPerGroup is two seconds
// of it. Examples use fixed numbers so their output is reproducible.
const (
	videoTimescale = 90000
	ticksPerGroup  = 2 * videoTimescale
)

// exampleStart anchors every example on the same instant so wallclock output is
// stable. A real producer takes these from its frames or its clock.
var exampleStart = time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)

// The ledger stores sealed groups and indexes them on two timelines, so a track
// can be replayed by its own media clock or lined up against other tracks by
// wallclock.
func Example() {
	ctx := context.Background()
	track := ledger.NewTrack(memstore.New(), "live/cam1/video", ledger.Config{})

	writer, err := track.Create(ctx, ledger.TrackConfig{
		Timescale:  videoTimescale,
		TimeSource: ledger.TimeSourceFrame,
		MIME:       "video/mp4",
		Encoding:   "fmp4",
	})
	if err != nil {
		log.Fatal(err)
	}

	// Only MediaTime is required. Duration and Wallclock are optional: without a duration a
	// derived view cannot emit a segment length, and without a wallclock anchor
	// the group cannot be correlated against another track — but either way it
	// still replays within its own track.
	for sequence := range uint64(3) {
		_, err := writer.AppendGroup(ctx, ledger.GroupInfo{
			GroupRef:    ledger.GroupRef{Epoch: 1, Sequence: sequence},
			MediaTime:   int64(sequence) * ticksPerGroup,
			Duration:    ticksPerGroup,
			Wallclock:   exampleStart.Add(time.Duration(sequence) * 2 * time.Second).UnixNano(),
			ObjectCount: 60,
		}, []byte("...frames..."))
		if err != nil {
			log.Fatal(err)
		}
	}

	// A reader needs only store access — no writer, and no server.
	reader, err := track.Reader(ctx)
	if err != nil {
		log.Fatal(err)
	}

	for group, err := range reader.Groups(ctx) {
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s  media %d..%d\n", group.GroupRef, group.MediaTime, group.MediaTime+group.Duration)
	}

	// Output:
	// e000001-g00000000  media 0..180000
	// e000001-g00000001  media 180000..360000
	// e000001-g00000002  media 360000..540000
}

// Wallclock is the cross-track key. Running the same window over two tracks
// recorded by different producers — with different timescales, and so with
// incomparable media clocks — is what lines the two recordings up.
func ExampleReader_RangeWallclock() {
	ctx := context.Background()
	store := memstore.New()

	// A 90 kHz video track in two-second groups.
	writeTrack(ctx, store, "live/cam1/video", videoTimescale, 2*time.Second, 4)
	// A 1 kHz sensor track in one-second groups. Its media clock shares nothing
	// with the video track's; only the wallclock anchors are comparable.
	writeTrack(ctx, store, "live/cam1/sensor", 1000, time.Second, 8)

	// "What was happening between four and six seconds in?"
	from := exampleStart.Add(4 * time.Second).UnixNano()
	to := exampleStart.Add(6 * time.Second).UnixNano()

	for _, track := range []ledger.TrackPath{"live/cam1/video", "live/cam1/sensor"} {
		reader, err := ledger.NewTrack(store, track, ledger.Config{}).Reader(ctx)
		if err != nil {
			log.Fatal(err)
		}

		for group, err := range reader.RangeWallclock(ctx, from, to) {
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("%s  %s  at %s\n", track, group.GroupRef,
				time.Unix(0, group.Wallclock).UTC().Format("15:04:05"))
		}
	}

	// Output:
	// live/cam1/video  e000001-g00000002  at 12:00:04
	// live/cam1/sensor  e000001-g00000004  at 12:00:04
	// live/cam1/sensor  e000001-g00000005  at 12:00:05
}

// A media-time window returns every group that overlaps it, including the one
// that starts before the window opens — a decoder needs that group to produce
// any frames inside the window at all.
func ExampleReader_RangeMedia() {
	ctx := context.Background()
	store := memstore.New()
	writeTrack(ctx, store, "live/cam1/video", videoTimescale, 2*time.Second, 4)

	reader, err := ledger.NewTrack(store, "live/cam1/video", ledger.Config{}).Reader(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// A window opening part-way through group 1 and closing inside group 2.
	for group, err := range reader.RangeMedia(ctx, 270_000, 450_000) {
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s starts at %d\n", group.GroupRef, group.MediaTime)
	}

	// Output:
	// e000001-g00000001 starts at 180000
	// e000001-g00000002 starts at 360000
}

// A seek resolves to the group anchored at or before the target rather than
// requiring an exact hit, which is what a player wants: land on or before, then
// decode forward.
//
// Groups dropped under congestion leave real gaps, and a seek into one reports
// that nothing covers the target rather than handing back the group before it.
// That distinction is only possible because Duration is stored: without it the
// ledger could not tell a gap from a group that simply runs long.
func ExampleReader_SeekMedia() {
	ctx := context.Background()
	track := ledger.NewTrack(memstore.New(), "live/cam1/video", ledger.Config{})

	writer, err := track.Create(ctx, ledger.TrackConfig{
		Timescale:  videoTimescale,
		TimeSource: ledger.TimeSourceFrame,
	})
	if err != nil {
		log.Fatal(err)
	}

	// Group 2 never arrives — dropped under congestion. The gap is real data,
	// not corruption, so the ledger records it rather than papering over it.
	for _, sequence := range []uint64{0, 1, 3} {
		_, err := writer.AppendGroup(ctx, ledger.GroupInfo{
			GroupRef:  ledger.GroupRef{Epoch: 1, Sequence: sequence},
			MediaTime: int64(sequence) * ticksPerGroup,
			Duration:  ticksPerGroup,
		}, []byte("...frames..."))
		if err != nil {
			log.Fatal(err)
		}
	}

	reader, err := track.Reader(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// 200000 lands inside group 1; 400000 falls in the hole group 2 left
	// behind; 600000 lands inside group 3; 999999 is past the end.
	for _, target := range []int64{200_000, 400_000, 600_000, 999_999} {
		group, err := reader.SeekMedia(ctx, target)
		switch {
		case errors.Is(err, ledger.ErrGroupNotFound):
			fmt.Printf("%7d -> nothing covers this instant\n", target)
		case err != nil:
			log.Fatal(err)
		default:
			fmt.Printf("%7d -> %s\n", target, group.GroupRef)
		}
	}

	// Output:
	//  200000 -> e000001-g00000001
	//  400000 -> nothing covers this instant
	//  600000 -> e000001-g00000003
	//  999999 -> nothing covers this instant
}

// Following a track is polling, because object stores do not push. Each update
// carries the cursor that resumes immediately after it, so a consumer can
// persist its position and pick up exactly where it stopped.
func ExampleReader_Follow() {
	ctx := context.Background()
	store := memstore.New()
	writeTrack(ctx, store, "live/cam1/video", videoTimescale, 2*time.Second, 4)

	reader, err := ledger.NewTrack(store, "live/cam1/video", ledger.Config{}).Reader(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// A consumer that processes two groups and then stops.
	var saved ledger.Cursor
	for update, err := range reader.Follow(ctx, ledger.Cursor{}, 10*time.Millisecond) {
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("processed %s\n", update.GroupRef)
		saved = update.Cursor
		if update.Sequence == 1 {
			break
		}
	}

	// The position survives a restart: it is text, so it fits in a state file
	// or a JSON document.
	encoded, err := saved.MarshalText()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("saved cursor %s\n", encoded)

	var resumed ledger.Cursor
	if err := resumed.UnmarshalText(encoded); err != nil {
		log.Fatal(err)
	}

	for update, err := range reader.Follow(ctx, resumed, 10*time.Millisecond) {
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("resumed at %s\n", update.GroupRef)
		break
	}

	// Output:
	// processed e000001-g00000000
	// processed e000001-g00000001
	// saved cursor 1/1
	// resumed at e000001-g00000002
}

// A producer that restarts resets its sequence numbering. Because group objects
// are immutable, reusing a sequence would collide rather than overwrite — so
// Epoch gives each producer lifetime its own keyspace, while the producer's own
// numbering survives for clients aligning replay against a live relay.
func ExampleWriter_AppendGroup() {
	ctx := context.Background()
	track := ledger.NewTrack(memstore.New(), "live/cam1/video", ledger.Config{})

	writer, err := track.Create(ctx, ledger.TrackConfig{
		Timescale:  videoTimescale,
		TimeSource: ledger.TimeSourceFrame,
	})
	if err != nil {
		log.Fatal(err)
	}

	first := ledger.GroupInfo{
		GroupRef:  ledger.GroupRef{Epoch: 1, Sequence: 7},
		MediaTime: 7 * ticksPerGroup,
		Duration:  ticksPerGroup,
	}
	if _, err := writer.AppendGroup(ctx, first, []byte("before restart")); err != nil {
		log.Fatal(err)
	}

	// Re-appending the same group is refused rather than silently replacing it.
	_, err = writer.AppendGroup(ctx, first, []byte("different bytes"))
	fmt.Println("duplicate:", errors.Is(err, ledger.ErrGroupExists))

	// A group that would start before its predecessor ended contradicts a
	// serial timeline, and is refused too. A gap would be fine.
	_, err = writer.AppendGroup(ctx, ledger.GroupInfo{
		GroupRef:  ledger.GroupRef{Epoch: 1, Sequence: 8},
		MediaTime: first.MediaTime + 1,
		Duration:  ticksPerGroup,
	}, []byte("overlapping"))
	fmt.Println("overlap:  ", errors.Is(err, ledger.ErrGroupOutOfOrder))

	// After a restart the producer starts over at sequence 1. The new epoch
	// keeps that from colliding with anything already stored.
	restarted, err := writer.AppendGroup(ctx, ledger.GroupInfo{
		GroupRef:  ledger.GroupRef{Epoch: 2, Sequence: 1},
		MediaTime: 0,
		Duration:  ticksPerGroup,
	}, []byte("after restart"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("restarted:", restarted.ObjectKey)

	// Output:
	// duplicate: true
	// overlap:   true
	// restarted: live/cam1/video/groups/e000002-g00000001
}

// A writer recovers from a crash without a repair pass. Writing a delta
// manifest is the commit, and a delta is immutable, so any delta that exists is
// committed. Recovery reads the head pointer for a hint and then probes forward
// until one is absent — that gap is the true tip.
//
// Head is only a cache, so even losing it costs nothing but a few probes.
func ExampleTrack_Writer() {
	ctx := context.Background()

	// Kept separately here only so the example can delete the head pointer
	// behind the ledger's back.
	objects := memstore.New()
	track := ledger.NewTrack(objects, "live/cam1/video", ledger.Config{})

	writer, err := track.Create(ctx, ledger.TrackConfig{
		Timescale:  videoTimescale,
		TimeSource: ledger.TimeSourceFrame,
	})
	if err != nil {
		log.Fatal(err)
	}

	for sequence := range uint64(2) {
		_, err := writer.AppendGroup(ctx, ledger.GroupInfo{
			GroupRef:  ledger.GroupRef{Epoch: 1, Sequence: sequence},
			MediaTime: int64(sequence) * ticksPerGroup,
			Duration:  ticksPerGroup,
		}, []byte("...frames..."))
		if err != nil {
			log.Fatal(err)
		}
	}

	// The process dies here, taking the writer's in-memory state with it — and
	// the head pointer with it too, which is the worst case.
	for key, err := range objects.List(ctx, "live/cam1/video/delta/head") {
		if err != nil {
			log.Fatal(err)
		}
		if err := objects.Delete(ctx, key); err != nil {
			log.Fatal(err)
		}
	}

	recovered, err := track.Writer(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// The track continues rather than restarting: nothing committed was lost.
	if _, err := recovered.AppendGroup(ctx, ledger.GroupInfo{
		GroupRef:  ledger.GroupRef{Epoch: 1, Sequence: 2},
		MediaTime: 2 * ticksPerGroup,
		Duration:  ticksPerGroup,
	}, []byte("...frames...")); err != nil {
		log.Fatal(err)
	}

	reader, err := track.Reader(ctx)
	if err != nil {
		log.Fatal(err)
	}

	for group, err := range reader.Groups(ctx) {
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(group.GroupRef)
	}

	// Output:
	// e000001-g00000000
	// e000001-g00000001
	// e000001-g00000002
}

// writeTrack creates a track and appends count groups of the given wallclock
// duration, with media time in the track's own timescale.
func writeTrack(ctx context.Context, store *memstore.Store, track ledger.TrackPath, timescale uint32, every time.Duration, count uint64) {
	writer, err := ledger.NewTrack(store, track, ledger.Config{}).Create(ctx, ledger.TrackConfig{
		Timescale:  timescale,
		TimeSource: ledger.TimeSourceFrame,
	})
	if err != nil {
		log.Fatal(err)
	}

	ticks := int64(every.Seconds() * float64(timescale))
	for sequence := range count {
		_, err := writer.AppendGroup(ctx, ledger.GroupInfo{
			GroupRef:    ledger.GroupRef{Epoch: 1, Sequence: sequence},
			MediaTime:   int64(sequence) * ticks,
			Duration:    ticks,
			Wallclock:   exampleStart.Add(time.Duration(sequence) * every).UnixNano(),
			ObjectCount: 60,
		}, []byte("...frames..."))
		if err != nil {
			log.Fatal(err)
		}
	}
}
