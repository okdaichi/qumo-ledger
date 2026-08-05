package stream_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/okdaichi/qumo-ledger/ledger"
	"github.com/okdaichi/qumo-ledger/ledger/store/memstore"
	"github.com/okdaichi/qumo-ledger/stream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testInit satisfies NewHandler's init requirement for the fmp4 fixture. Tests
// whose subject is not initialization pass it so they exercise the path they are
// actually about; see TestNewHandler_InitRequired for the guard itself.
var testInit = stream.InitSegment{Bytes: []byte("init-bytes")}

// fixtureEpoch is when the fixture's presentation starts on the wall clock.
var fixtureEpoch = time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)

// trackFixture is a track with two committed segments, plus the metadata and
// payloads a serving test wants to assert against.
type trackFixture struct {
	track   *ledger.Track
	metas   []ledger.GroupInfo
	payload [][]byte
}

func newTrackFixture(tb testing.TB) trackFixture {
	tb.Helper()
	return newTrackFixtureEncoding(tb, "fmp4")
}

func newTrackFixtureEncoding(tb testing.TB, encoding string) trackFixture {
	tb.Helper()
	return newTrackFixtureGroups(tb, encoding, 2)
}

func newTrackFixtureGroups(tb testing.TB, encoding string, groups int64) trackFixture {
	tb.Helper()
	ctx := context.Background()

	store := memstore.New()
	track, err := ledger.Create(ctx, store, "live/cam1/video", ledger.TrackSchema{
		Timescale: 90000, TimeSource: ledger.TimeSourceFrame,
		MIME: "video/mp4", Encoding: encoding,
	}, ledger.Config{})
	require.NoError(tb, err)

	writer, err := track.Writer(ctx)
	require.NoError(tb, err)

	fix := trackFixture{track: track}
	for sequence := range groups {
		payload := []byte("frames-" + string(rune('A'+sequence)))
		meta, err := writer.AppendGroup(ctx, ledger.GroupInfo{
			ID:        ledger.NewGroupID(0, uint64(sequence)),
			MediaTime: sequence * 180000,
			Duration:  180000,
			// Anchored on a fixed clock advancing with media time, so the
			// presentation start these imply is the same for every group — which
			// is what a stable DASH availabilityStartTime depends on.
			Wallclock: fixtureEpoch.Add(time.Duration(sequence) * 2 * time.Second).UnixNano(),
		}, payload)
		require.NoError(tb, err)
		fix.metas = append(fix.metas, meta)
		fix.payload = append(fix.payload, payload)
	}
	return fix
}

func TestHandler_HLSPlaylist(t *testing.T) {
	fix := newTrackFixture(t)
	handler, err := stream.NewHandler(fix.track, stream.Options{InitSegment: testInit})
	require.NoError(t, err)
	ts := httptest.NewServer(handler)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/playlist.m3u8")
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "application/vnd.apple.mpegurl", resp.Header.Get("Content-Type"))

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	got := string(body)

	assert.Contains(t, got, "#EXT-X-PLAYLIST-TYPE:EVENT")
	assert.Contains(t, got, fix.metas[0].ID.String()+".m4s", "the playlist points at segments by GroupID")
	assert.Contains(t, got, fix.metas[1].ID.String()+".m4s")
}

func TestHandler_DASHManifest(t *testing.T) {
	fix := newTrackFixture(t)
	handler, err := stream.NewHandler(fix.track, stream.Options{InitSegment: testInit})
	require.NoError(t, err)
	ts := httptest.NewServer(handler)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/manifest.mpd")
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "application/dash+xml", resp.Header.Get("Content-Type"))

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	got := string(body)

	assert.Contains(t, got, `type="dynamic"`)
	assert.Contains(t, got, `<SegmentURL media="`+fix.metas[0].ID.String()+`.m4s"/>`,
		"the MPD points at segments by GroupID, the same URLs as the HLS playlist")
}

// The default resolver proxies: a segment request returns the stored bytes.
func TestHandler_SegmentProxies(t *testing.T) {
	fix := newTrackFixture(t)
	handler, err := stream.NewHandler(fix.track, stream.Options{InitSegment: testInit})
	require.NoError(t, err)
	ts := httptest.NewServer(handler)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/" + fix.metas[0].ID.String() + ".m4s")
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "video/mp4", resp.Header.Get("Content-Type"))

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, fix.payload[0], body, "the proxy serves the exact bytes that were appended")
}

// A redirect resolver sends the player straight to the object: the handler asks
// the resolver and 302s to whatever URL it mints from ObjectKey.
func TestHandler_SegmentRedirect(t *testing.T) {
	fix := newTrackFixture(t)
	handler, err := stream.NewHandler(fix.track, stream.Options{
		InitSegment: testInit,
		Resolver: stream.RedirectResolver(func(g ledger.GroupInfo) string {
			return "https://objects.example/" + g.ObjectKey
		}),
	})
	require.NoError(t, err)
	ts := httptest.NewServer(handler)
	defer ts.Close()

	// Do not follow the redirect: the test asserts the 302, not the object store.
	client := &http.Client{
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	resp, err := client.Get(ts.URL + "/" + fix.metas[1].ID.String() + ".m4s")
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusFound, resp.StatusCode)
	loc, err := resp.Location()
	require.NoError(t, err)
	assert.Equal(t, "https://objects.example/"+fix.metas[1].ObjectKey, loc.String())
}

func TestHandler_UnknownSegmentNotFound(t *testing.T) {
	fix := newTrackFixture(t)
	handler, err := stream.NewHandler(fix.track, stream.Options{InitSegment: testInit})
	require.NoError(t, err)
	ts := httptest.NewServer(handler)
	defer ts.Close()

	// A sequence that was never committed.
	resp, err := http.Get(ts.URL + "/e000001-g00000099.m4s")
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

// Serving is concurrent-safe: many segment requests at once must not race. The
// race detector is the assertion.
func TestHandler_ConcurrentSegments(t *testing.T) {
	fix := newTrackFixture(t)
	handler, err := stream.NewHandler(fix.track, stream.Options{InitSegment: testInit})
	require.NoError(t, err)
	ts := httptest.NewServer(handler)
	defer ts.Close()

	var wg sync.WaitGroup
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := http.Get(ts.URL + "/" + fix.metas[0].ID.String() + ".m4s")
			if !assert.NoError(t, err) {
				return
			}
			defer resp.Body.Close()
			assert.Equal(t, http.StatusOK, resp.StatusCode)
		}()
	}
	wg.Wait()
}

// A fragmented-MP4 track cannot be played without an init segment, so building a
// handler without one fails rather than serving manifests that omit #EXT-X-MAP
// (and DASH @initialization) with a 200.
func TestNewHandler_InitRequired(t *testing.T) {
	tests := map[string]struct {
		encoding string
		init     stream.InitSegment
		wantErr  bool
	}{
		"fmp4 without init":    {encoding: "fmp4", wantErr: true},
		"fmp4 with init bytes": {encoding: "fmp4", init: stream.InitSegment{Bytes: []byte("init-bytes")}},
		// A URL satisfies the requirement: the segment exists, this handler just
		// does not serve its bytes.
		"fmp4 with init url": {encoding: "fmp4", init: stream.InitSegment{URL: "https://objects.example/init.m4s"}},
		// MPEG-TS repeats its PAT/PMT in every segment, so it needs no init.
		"mpegts without init": {encoding: "ts"},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			fix := newTrackFixtureEncoding(t, tt.encoding)

			handler, err := stream.NewHandler(fix.track, stream.Options{InitSegment: tt.init})
			if tt.wantErr {
				assert.ErrorIs(t, err, stream.ErrInitRequired)
				assert.Nil(t, handler)
				return
			}
			require.NoError(t, err)
			assert.NotNil(t, handler)
		})
	}
}

// A window caps the playlist at the newest segments and turns it into a sliding
// live playlist: EVENT forbids removing segments, and EXT-X-MEDIA-SEQUENCE must
// count the ones that rolled off.
func TestHandler_Window(t *testing.T) {
	const total, window = 10, 3
	fix := newTrackFixtureGroups(t, "fmp4", total)

	handler, err := stream.NewHandler(fix.track, stream.Options{
		InitSegment: testInit,
		Window:      window,
	})
	require.NoError(t, err)
	ts := httptest.NewServer(handler)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/playlist.m3u8")
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	got := string(body)

	assert.NotContains(t, got, "#EXT-X-PLAYLIST-TYPE:EVENT",
		"a playlist that drops segments cannot be EVENT")
	assert.Contains(t, got, "#EXT-X-MEDIA-SEQUENCE:7",
		"the first listed segment follows the 7 that rolled off")

	for _, meta := range fix.metas[:total-window] {
		assert.NotContains(t, got, meta.ID.String()+".m4s",
			"segments older than the window are not listed")
	}
	for _, meta := range fix.metas[total-window:] {
		assert.Contains(t, got, meta.ID.String()+".m4s",
			"the newest segments are listed, in commit order")
	}

	// Rolling out of the playlist is not deletion: the ledger keeps every group,
	// so an older segment a client already holds a URL for still resolves.
	old := fix.metas[0]
	oldResp, err := http.Get(ts.URL + "/" + old.ID.String() + ".m4s")
	require.NoError(t, err)
	defer oldResp.Body.Close()
	assert.Equal(t, http.StatusOK, oldResp.StatusCode)
}

// A window larger than the track lists everything and drops nothing.
func TestHandler_WindowLargerThanTrack(t *testing.T) {
	fix := newTrackFixtureGroups(t, "fmp4", 2)

	handler, err := stream.NewHandler(fix.track, stream.Options{
		InitSegment: testInit,
		Window:      10,
	})
	require.NoError(t, err)
	ts := httptest.NewServer(handler)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/playlist.m3u8")
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	got := string(body)

	assert.Contains(t, got, "#EXT-X-MEDIA-SEQUENCE:0")
	assert.Contains(t, got, fix.metas[0].ID.String()+".m4s")
	assert.Contains(t, got, fix.metas[1].ID.String()+".m4s")
}

// A window that has rolled past an epoch change must still tell the player the
// timeline reset. The reset belongs to the first listed segment, where nothing
// in the listed groups reveals it, and the resets already gone from the playlist
// have to be counted so discontinuity numbering survives them.
func TestHandler_WindowAcrossEpochs(t *testing.T) {
	ctx := context.Background()
	store := memstore.New()
	track, err := ledger.Create(ctx, store, "live/cam1/video", ledger.TrackSchema{
		Timescale: 90000, TimeSource: ledger.TimeSourceFrame,
		MIME: "video/mp4", Encoding: "fmp4",
	}, ledger.Config{})
	require.NoError(t, err)

	writer, err := track.Writer(ctx)
	require.NoError(t, err)

	// Three groups, then a producer restart, then three more. Windowing to the
	// last four puts the epoch boundary one segment into the window; windowing
	// to two puts it behind the window entirely.
	appendGroups := func(n int64) {
		for i := range n {
			_, err := writer.AppendGroup(ctx, ledger.GroupInfo{
				ID:        ledger.NewGroupID(0, uint64(i)),
				MediaTime: i * 180000,
				Duration:  180000,
			}, []byte("frames"))
			require.NoError(t, err)
		}
	}
	appendGroups(3)
	require.NoError(t, writer.NewEpoch(ctx))
	appendGroups(3)

	t.Run("boundary inside the window", func(t *testing.T) {
		got := playlistFor(t, track, 4)
		assert.Contains(t, got, "#EXT-X-DISCONTINUITY",
			"the epoch change is listed, so its reset is marked")
		assert.NotContains(t, got, "#EXT-X-DISCONTINUITY-SEQUENCE",
			"no reset has rolled off, and an absent tag already means zero")
	})

	t.Run("window opens on the boundary", func(t *testing.T) {
		// The last three segments are exactly the new epoch, so the reset sits
		// at the first listed segment.
		got := playlistFor(t, track, 3)
		assert.Contains(t, got, "#EXT-X-DISCONTINUITY",
			"a window opening on a new epoch still announces the reset")
	})

	t.Run("boundary behind the window", func(t *testing.T) {
		got := playlistFor(t, track, 2)
		assert.NotContains(t, got, "\n#EXT-X-DISCONTINUITY\n",
			"the reset is no longer between two listed segments")
		assert.Contains(t, got, "#EXT-X-DISCONTINUITY-SEQUENCE:1",
			"but the player is told one reset has rolled off")
	})
}

// playlistFor renders the HLS playlist for a track through a windowed handler.
func playlistFor(tb testing.TB, track *ledger.Track, window int) string {
	tb.Helper()

	handler, err := stream.NewHandler(track, stream.Options{
		InitSegment: testInit,
		Window:      window,
	})
	require.NoError(tb, err)
	ts := httptest.NewServer(handler)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/playlist.m3u8")
	require.NoError(tb, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(tb, err)
	return string(body)
}

// A windowed MPD lists only the window, and says how far back it reaches. Its
// availabilityStartTime anchors the presentation, so it must name where the
// presentation began rather than where the window currently starts — otherwise
// it moves on every refresh and drags each client's timeline with it.
func TestHandler_WindowDASH(t *testing.T) {
	const total = 10
	fix := newTrackFixtureGroups(t, "fmp4", total)

	manifestFor := func(window int) string {
		handler, err := stream.NewHandler(fix.track, stream.Options{
			InitSegment: testInit,
			Window:      window,
		})
		require.NoError(t, err)
		ts := httptest.NewServer(handler)
		defer ts.Close()

		resp, err := http.Get(ts.URL + "/manifest.mpd")
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		return string(body)
	}

	got := manifestFor(3)

	assert.Contains(t, got, `timeShiftBufferDepth="PT6S"`,
		"three two-second segments is how far back the window reaches")
	for _, meta := range fix.metas[:total-3] {
		assert.NotContains(t, got, `media="`+meta.ID.String()+`.m4s"`,
			"segments older than the window are not listed")
	}
	for _, meta := range fix.metas[total-3:] {
		assert.Contains(t, got, `media="`+meta.ID.String()+`.m4s"`)
	}

	// The presentation did not move, so neither may its anchor.
	assert.Equal(t,
		availabilityStartTime(t, manifestFor(0)),
		availabilityStartTime(t, got),
		"availabilityStartTime names the presentation start, not the window start")
}

// availabilityStartTime pulls the MPD attribute out of a rendered manifest.
func availabilityStartTime(tb testing.TB, mpd string) string {
	tb.Helper()

	const attr = `availabilityStartTime="`
	at := strings.Index(mpd, attr)
	require.GreaterOrEqual(tb, at, 0, "the MPD carries an availabilityStartTime")
	rest := mpd[at+len(attr):]
	end := strings.Index(rest, `"`)
	require.GreaterOrEqual(tb, end, 0)
	return rest[:end]
}

// A supplied init segment is served at /init.<ext> and referenced from both
// manifests.
func TestHandler_InitSegment(t *testing.T) {
	fix := newTrackFixture(t)
	handler, err := stream.NewHandler(fix.track, stream.Options{
		InitSegment: stream.InitSegment{Bytes: []byte("init-bytes")},
	})
	require.NoError(t, err)
	ts := httptest.NewServer(handler)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/init.m4s")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, []byte("init-bytes"), body)

	// Both manifests reference it.
	for _, name := range []string{"playlist.m3u8", "manifest.mpd"} {
		resp, err := http.Get(ts.URL + "/" + name)
		require.NoError(t, err)
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		require.NoError(t, err)
		assert.Contains(t, string(body), "init.m4s", "%s must reference the init segment", name)
	}
}
