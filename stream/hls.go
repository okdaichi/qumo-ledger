package stream

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/okdaichi/qumo-ledger/ledger"
)

// renderOpts carries the rendering inputs that come from neither the schema nor
// the groups themselves: the segment URL extension and the init-segment URI.
type renderOpts struct {
	segExt  string // e.g. ".m4s"
	initURI string // relative init URI, "" when there is none
}

// renderHLS builds an EVENT media playlist for groups in commit order.
//
// The playlist is the whole track and grows as groups land: EVENT means segments
// are only ever appended, never removed, and there is no #EXT-X-ENDLIST because
// the ledger has no notion of a finished track. The producer's own — gappy,
// epoch-resetting — sequence is not the media sequence: HLS requires consecutive
// media sequence numbers, so the sequence is synthetic (the segment's ordinal),
// and the stable per-segment address is the GroupID carried in the URL.
func renderHLS(schema ledger.TrackSchema, groups []ledger.GroupInfo, opts renderOpts) ([]byte, error) {
	for _, g := range groups {
		if g.Duration <= 0 {
			return nil, fmt.Errorf("stream: group %s has no duration; HLS needs an EXTINF", g.ID)
		}
	}

	var b strings.Builder
	b.WriteString("#EXTM3U\n")
	b.WriteString("#EXT-X-VERSION:6\n")
	fmt.Fprintf(&b, "#EXT-X-TARGETDURATION:%d\n", maxSegmentSeconds(schema, groups))
	b.WriteString("#EXT-X-MEDIA-SEQUENCE:0\n")
	b.WriteString("#EXT-X-PLAYLIST-TYPE:EVENT\n")
	if opts.initURI != "" {
		fmt.Fprintf(&b, "#EXT-X-MAP:URI=\"%s\"\n", opts.initURI)
	}

	var prevEpoch uint64
	for i, g := range groups {
		// A new producer lifetime is a discontinuity: the timeline reset.
		if i > 0 && g.ID.Epoch() != prevEpoch {
			b.WriteString("#EXT-X-DISCONTINUITY\n")
		}
		if g.Wallclock != 0 {
			fmt.Fprintf(&b, "#EXT-X-PROGRAM-DATE-TIME:%s\n",
				time.Unix(0, g.Wallclock).UTC().Format(time.RFC3339Nano))
		}
		fmt.Fprintf(&b, "#EXTINF:%s,\n", extinfSeconds(g.Duration, schema.Timescale))
		fmt.Fprintf(&b, "%s%s\n", g.ID, opts.segExt)
		prevEpoch = g.ID.Epoch()
	}

	return []byte(b.String()), nil
}

// maxSegmentSeconds is the ceiling, in whole seconds, of the longest segment —
// the bound HLS TARGETDURATION must honor and a sensible DASH minBufferTime.
func maxSegmentSeconds(schema ledger.TrackSchema, groups []ledger.GroupInfo) int {
	var max float64
	for _, g := range groups {
		if g.Duration <= 0 {
			continue
		}
		if s := float64(g.Duration) / float64(schema.Timescale); s > max {
			max = s
		}
	}
	return int(math.Ceil(max))
}

// extinfSeconds formats a media extent as the decimal seconds HLS carries in
// EXTINF. Three decimals is more than any player needs and keeps output stable.
func extinfSeconds(duration int64, timescale uint32) string {
	return strconv.FormatFloat(float64(duration)/float64(timescale), 'f', 3, 64)
}
