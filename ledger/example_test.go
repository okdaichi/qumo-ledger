package ledger_test

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/okdaichi/qumo-ledger/ledger"
	"github.com/okdaichi/qumo-ledger/objectstore/memstore"
)

// The ledger stores sealed groups and indexes them on two independent
// timelines, so a track can be replayed by its own media clock or correlated
// against other tracks by wallclock.
func Example() {
	ctx := context.Background()
	store := memstore.New()

	writer, err := ledger.CreateTrack(ctx, store, "live/cam1/video", ledger.TrackConfig{
		Timescale:  90000, // the usual video timescale: 90 kHz
		TimeSource: ledger.TimeSourceFrame,
		MIME:       "video/mp4",
		Encoding:   "fmp4",
	})
	if err != nil {
		log.Fatal(err)
	}

	// Two-second groups, starting at a fixed instant so the example is
	// reproducible. A real producer would take these from the frames.
	start := time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC)
	for sequence := range uint64(3) {
		meta := ledger.GroupMeta{
			GroupRef: ledger.GroupRef{Epoch: 1, Sequence: sequence},
			T0:       int64(sequence) * 180000,
			T1:       int64(sequence+1) * 180000,
			W0:       start.Add(time.Duration(sequence) * 2 * time.Second).UnixNano(),
			W1:       start.Add(time.Duration(sequence+1) * 2 * time.Second).UnixNano(),
		}

		if _, err := writer.AppendGroup(ctx, meta, []byte("...frames...")); err != nil {
			log.Fatal(err)
		}
	}

	// A reader needs only store access — no writer, and no server.
	reader, err := ledger.OpenReader(ctx, store, "live/cam1/video")
	if err != nil {
		log.Fatal(err)
	}

	group, err := reader.SeekWallclock(ctx, start.Add(5*time.Second).UnixNano())
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("group %s covers media %d..%d\n", group.GroupRef, group.T0, group.T1)

	// Output:
	// group e000001-g00000002 covers media 360000..540000
}
