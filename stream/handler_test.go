package stream_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/okdaichi/qumo-ledger/ledger"
	"github.com/okdaichi/qumo-ledger/ledger/store/memstore"
	"github.com/okdaichi/qumo-ledger/stream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// trackFixture is a track with two committed segments, plus the metadata and
// payloads a serving test wants to assert against.
type trackFixture struct {
	track   *ledger.Track
	metas   []ledger.GroupInfo
	payload [][]byte
}

func newTrackFixture(tb testing.TB) trackFixture {
	tb.Helper()
	ctx := context.Background()

	store := memstore.New()
	track, err := ledger.Create(ctx, store, "live/cam1/video", ledger.TrackSchema{
		Timescale: 90000, TimeSource: ledger.TimeSourceFrame,
		MIME: "video/mp4", Encoding: "fmp4",
	}, ledger.Config{})
	require.NoError(tb, err)

	writer, err := track.Writer(ctx)
	require.NoError(tb, err)

	fix := trackFixture{track: track}
	for sequence := range int64(2) {
		payload := []byte("frames-" + string(rune('A'+sequence)))
		meta, err := writer.AppendGroup(ctx, ledger.GroupInfo{
			ID:        ledger.NewGroupID(0, uint64(sequence)),
			MediaTime: sequence * 180000,
			Duration:  180000,
		}, payload)
		require.NoError(tb, err)
		fix.metas = append(fix.metas, meta)
		fix.payload = append(fix.payload, payload)
	}
	return fix
}

func TestHandler_HLSPlaylist(t *testing.T) {
	fix := newTrackFixture(t)
	handler, err := stream.NewHandler(fix.track, stream.Options{})
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
	handler, err := stream.NewHandler(fix.track, stream.Options{})
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
	handler, err := stream.NewHandler(fix.track, stream.Options{})
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
	handler, err := stream.NewHandler(fix.track, stream.Options{})
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
	handler, err := stream.NewHandler(fix.track, stream.Options{})
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
