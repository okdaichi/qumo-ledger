package ingest_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/okdaichi/qumo-ledger/ingest"
	"github.com/okdaichi/qumo-ledger/ledger"
	"github.com/okdaichi/qumo-ledger/ledger/store"
	"github.com/okdaichi/qumo-ledger/ledger/store/memstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	chatTrack        = ledger.TrackPath("room/123/chat")
	chatAnnouncement = `{"broadcast_path":"/room/123","track_name":"chat"}`
)

func newHandler(tb testing.TB, s store.Store, opts ingest.Options) http.Handler {
	tb.Helper()
	h, err := ingest.NewHandler(s, opts)
	require.NoError(tb, err)
	return http.StripPrefix("/ingest", h)
}

// send sends body to the handler at path, below its mount point, with the
// given headers as name-value pairs.
func send(h http.Handler, method, path, body string, header ...string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, "/ingest/"+path, strings.NewReader(body))
	for i := 0; i+1 < len(header); i += 2 {
		req.Header.Set(header[i], header[i+1])
	}
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	return rr
}

// announce starts a contribution to the chat track and returns its ID.
func announce(tb testing.TB, h http.Handler) string {
	tb.Helper()
	rr := send(h, http.MethodPost, "announce", chatAnnouncement)
	require.Equal(tb, http.StatusCreated, rr.Code, rr.Body.String())
	var response struct {
		ID string `json:"id"`
	}
	require.NoError(tb, json.Unmarshal(rr.Body.Bytes(), &response))
	require.Equal(tb, "contributions/"+response.ID, rr.Header().Get("Location"))
	return response.ID
}

func record(h http.Handler, id, payload string, header ...string) *httptest.ResponseRecorder {
	return send(h, http.MethodPost, "contributions/"+id+"/records", payload, header...)
}

// stored reads every payload committed to the chat track, in commit order.
func stored(tb testing.TB, s store.Store) []string {
	tb.Helper()
	ctx := context.Background()
	track, err := ledger.Open(ctx, s, chatTrack, ledger.Config{})
	if errors.Is(err, ledger.ErrTrackNotFound) {
		return nil
	}
	require.NoError(tb, err)
	reader, err := track.Reader(ctx)
	require.NoError(tb, err)
	reader.SeekStart()

	var payloads []string
	for {
		group, err := reader.Next(ctx)
		if errors.Is(err, io.EOF) {
			return payloads
		}
		require.NoError(tb, err)
		data, err := reader.ReadGroup(ctx, group.ObjectKey)
		require.NoError(tb, err)
		payloads = append(payloads, string(data))
	}
}

func TestTrack_Path(t *testing.T) {
	tests := map[string]struct {
		track ingest.Track
		want  ledger.TrackPath
	}{
		"rooted":     {track: ingest.Track{BroadcastPath: "/room/123", TrackName: "chat"}, want: "room/123/chat"},
		"not rooted": {track: ingest.Track{BroadcastPath: "room/123", TrackName: "chat"}, want: "room/123/chat"},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.track.Path())
		})
	}
}

func TestHandler_Announce_CreatesTheTrackOnce(t *testing.T) {
	s := memstore.New()
	h := newHandler(t, s, ingest.Options{})

	first := send(h, http.MethodPost, "announce", chatAnnouncement)
	second := send(h, http.MethodPost, "announce", chatAnnouncement)

	require.Equal(t, http.StatusCreated, first.Code)
	assert.Contains(t, first.Body.String(), `"track":"room/123/chat"`)
	require.Equal(t, http.StatusCreated, second.Code, "another contribution to a track that exists")
	assert.NotEqual(t, first.Header().Get("Location"), second.Header().Get("Location"))

	track, err := ledger.Open(context.Background(), s, chatTrack, ledger.Config{})
	require.NoError(t, err)
	root := track.Root()
	assert.Equal(t, ledger.TimeSourceIngest, root.TimeSource)
	assert.Equal(t, ingest.Encoding, root.Encoding)
	assert.Equal(t, ingest.MIME, root.MIME)
}

func TestHandler_Announce_CanonicalBroadcastPath(t *testing.T) {
	var announced []ingest.Track
	h := newHandler(t, memstore.New(), ingest.Options{
		OnAnnounce: func(_ context.Context, tr ingest.Track) { announced = append(announced, tr) },
	})

	for _, path := range []string{"room/123", "/room/123/", "/room/123"} {
		rr := send(h, http.MethodPost, "announce", `{"broadcast_path":"`+path+`","track_name":"chat"}`)
		require.Equal(t, http.StatusCreated, rr.Code, path)
	}

	require.Len(t, announced, 3)
	for _, tr := range announced {
		assert.Equal(t, ingest.Track{BroadcastPath: "/room/123", TrackName: "chat"}, tr)
	}
}

func TestHandler_Record_StoresThePayloadInCommitOrder(t *testing.T) {
	s := memstore.New()
	h := newHandler(t, s, ingest.Options{})
	first, second := announce(t, h), announce(t, h)

	one := record(h, first, `{"user":"alice","text":"hello"}`)
	two := record(h, second, `"hi"`)

	require.Equal(t, http.StatusCreated, one.Code)
	require.Equal(t, http.StatusCreated, two.Code)
	assert.Equal(t, []string{`{"user":"alice","text":"hello"}`, `"hi"`}, stored(t, s),
		"the payload is stored as it was sent")

	var response struct {
		Group     string `json:"group"`
		Wallclock int64  `json:"wallclock"`
	}
	require.NoError(t, json.Unmarshal(two.Body.Bytes(), &response))
	id, err := ledger.ParseGroupID(response.Group)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), id.Sequence(), "the second record is the track's second group")
	assert.NotZero(t, response.Wallclock)
}

func TestHandler_Record_UnknownContributionIsNotFound(t *testing.T) {
	s := memstore.New()
	h := newHandler(t, s, ingest.Options{})

	rr := record(h, "no-such-contribution", `"hello"`)

	assert.Equal(t, http.StatusNotFound, rr.Code)
	assert.Empty(t, stored(t, s))
}

func TestHandler_End_StopsTheContribution(t *testing.T) {
	s := memstore.New()
	h := newHandler(t, s, ingest.Options{})
	ended, other := announce(t, h), announce(t, h)

	end := send(h, http.MethodDelete, "contributions/"+ended, "")

	assert.Equal(t, http.StatusNoContent, end.Code)
	assert.Equal(t, http.StatusGone, record(h, ended, `"late"`).Code)
	assert.Equal(t, http.StatusGone, send(h, http.MethodDelete, "contributions/"+ended, "").Code)
	assert.Equal(t, http.StatusCreated, record(h, other, `"still here"`).Code, "other contributions are unaffected")
	assert.Equal(t, []string{`"still here"`}, stored(t, s))
}

func TestHandler_IdleContributionEnds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const idle = time.Minute
		h := newHandler(t, memstore.New(), ingest.Options{IdleTimeout: idle})
		quiet, busy := announce(t, h), announce(t, h)

		// The busy one records every half idle timeout; the quiet one does not.
		for range 3 {
			time.Sleep(idle / 2)
			synctest.Wait()
			require.Equal(t, http.StatusCreated, record(h, busy, `"still here"`).Code)
		}

		// At 1.5 idle timeouts the quiet one has ended; it is kept so its URLs
		// answer 410.
		assert.Equal(t, http.StatusGone, record(h, quiet, `"back"`).Code)

		// One idle timeout after it ended (at 2 idle timeouts) it is forgotten,
		// while the busy one, last heard at 1.5, is still live.
		time.Sleep(idle/2 + time.Second)
		synctest.Wait()
		assert.Equal(t, http.StatusNotFound, record(h, quiet, `"back"`).Code)
		assert.Equal(t, http.StatusCreated, record(h, busy, `"still here"`).Code)
	})
}

func TestHandler_IdleTimeoutDefault(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHandler(t, memstore.New(), ingest.Options{})
		id := announce(t, h)

		time.Sleep(ingest.DefaultIdleTimeout - time.Second)
		synctest.Wait()
		assert.Equal(t, http.StatusCreated, record(h, id, `"just in time"`).Code)

		time.Sleep(ingest.DefaultIdleTimeout + time.Second)
		synctest.Wait()
		assert.Equal(t, http.StatusGone, record(h, id, `"too late"`).Code)
	})
}

func TestHandler_Record_IdempotencyKey(t *testing.T) {
	s := memstore.New()
	var delivered int
	h := newHandler(t, s, ingest.Options{
		OnRecord: func(context.Context, ingest.Track, ledger.GroupInfo, []byte) { delivered++ },
	})
	first, second := announce(t, h), announce(t, h)

	original := record(h, first, `"hello"`, "Idempotency-Key", "k1")
	retry := record(h, first, `"hello"`, "Idempotency-Key", "k1")
	otherKey := record(h, first, `"again"`, "Idempotency-Key", "k2")
	otherContribution := record(h, second, `"hello"`, "Idempotency-Key", "k1")
	unkeyed := record(h, first, `"unkeyed"`)
	unkeyedAgain := record(h, first, `"unkeyed"`)

	for _, rr := range []*httptest.ResponseRecorder{original, retry, otherKey, otherContribution, unkeyed, unkeyedAgain} {
		require.Equal(t, http.StatusCreated, rr.Code)
	}
	assert.JSONEq(t, original.Body.String(), retry.Body.String(), "a retry is answered as the first was")
	assert.Equal(t, []string{`"hello"`, `"again"`, `"hello"`, `"unkeyed"`, `"unkeyed"`}, stored(t, s),
		"a key is scoped to its contribution, and records without one are never merged")
	assert.Equal(t, 5, delivered, "a retry is not delivered again")
}

func TestHandler_Record_IdempotencyKeysAreBounded(t *testing.T) {
	s := memstore.New()
	h := newHandler(t, s, ingest.Options{})
	id := announce(t, h)
	const remembered = 256

	for i := range remembered + 1 {
		require.Equal(t, http.StatusCreated, record(h, id, fmt.Sprintf("%d", i), "Idempotency-Key", fmt.Sprintf("k%d", i)).Code)
	}
	newest := record(h, id, "0", "Idempotency-Key", fmt.Sprintf("k%d", remembered))
	oldest := record(h, id, "0", "Idempotency-Key", "k0")

	require.Equal(t, http.StatusCreated, newest.Code)
	require.Equal(t, http.StatusCreated, oldest.Code)
	assert.Len(t, stored(t, s), remembered+2, "the oldest key was forgotten, so its retry is stored again")
}

func TestHandler_Record_ConcurrentContributionsAllCommit(t *testing.T) {
	s := memstore.New()
	h := newHandler(t, s, ingest.Options{})
	const contributions, each = 8, 10
	ids := make([]string, contributions)
	for c := range ids {
		ids[c] = announce(t, h)
	}

	var wg sync.WaitGroup
	for c, id := range ids {
		wg.Go(func() {
			for i := range each {
				rr := record(h, id, fmt.Sprintf(`{"c":%d,"i":%d}`, c, i))
				assert.Equal(t, http.StatusCreated, rr.Code)
			}
		})
	}
	wg.Wait()

	payloads := stored(t, s)
	require.Len(t, payloads, contributions*each)
	// Each contribution's own records keep the order it sent them in.
	next := make(map[int]int)
	for _, payload := range payloads {
		var p struct{ C, I int }
		require.NoError(t, json.Unmarshal([]byte(payload), &p))
		assert.Equal(t, next[p.C], p.I)
		next[p.C]++
	}
}

func TestHandler_OnRecord_SeesCommittedRecordsInOrder(t *testing.T) {
	type delivery struct {
		track   ingest.Track
		group   ledger.GroupInfo
		payload string
	}
	var seen []delivery
	h := newHandler(t, memstore.New(), ingest.Options{
		OnRecord: func(_ context.Context, tr ingest.Track, g ledger.GroupInfo, payload []byte) {
			seen = append(seen, delivery{track: tr, group: g, payload: string(payload)})
		},
	})
	id := announce(t, h)

	record(h, id, `"one"`)
	record(h, id, `"two"`)

	require.Len(t, seen, 2)
	assert.Equal(t, ingest.Track{BroadcastPath: "/room/123", TrackName: "chat"}, seen[0].track)
	assert.Equal(t, `"one"`, seen[0].payload)
	assert.Equal(t, uint64(0), seen[0].group.ID.Sequence())
	assert.Equal(t, uint64(1), seen[1].group.ID.Sequence())
	assert.NotEmpty(t, seen[1].group.ObjectKey, "the hook sees the group as committed")
}

func TestHandler_Authorize(t *testing.T) {
	s := memstore.New()
	var asked []ingest.Track
	allowed := true
	h := newHandler(t, s, ingest.Options{
		Authorize: func(r *http.Request, tr ingest.Track) error {
			asked = append(asked, tr)
			switch {
			case r.Header.Get("Authorization") == "":
				return fmt.Errorf("no credential: %w", ingest.ErrUnauthenticated)
			case !allowed:
				return errors.New("not allowed")
			}
			return nil
		},
		Challenge: "Bearer",
	})
	const credential = "Bearer credential"

	unauthenticated := send(h, http.MethodPost, "announce", chatAnnouncement)
	assert.Equal(t, http.StatusUnauthorized, unauthenticated.Code)
	assert.Equal(t, "Bearer", unauthenticated.Header().Get("WWW-Authenticate"), "a 401 names the scheme")

	signed := send(h, http.MethodPost, "announce", chatAnnouncement, "Authorization", credential)
	require.Equal(t, http.StatusCreated, signed.Code)
	location := signed.Header().Get("Location")

	// Every record and end is authorized again, on the contribution's track.
	assert.Equal(t, http.StatusUnauthorized, send(h, http.MethodPost, location+"/records", `"unsigned"`).Code)
	allowed = false
	assert.Equal(t, http.StatusForbidden, send(h, http.MethodPost, location+"/records", `"revoked"`, "Authorization", credential).Code)
	assert.Equal(t, http.StatusForbidden, send(h, http.MethodDelete, location, "", "Authorization", credential).Code)
	assert.Equal(t, http.StatusForbidden, send(h, http.MethodPost, "announce", chatAnnouncement, "Authorization", credential).Code)

	assert.Empty(t, stored(t, s))
	require.Len(t, asked, 6)
	for _, tr := range asked {
		assert.Equal(t, ingest.Track{BroadcastPath: "/room/123", TrackName: "chat"}, tr)
	}
}

func TestHandler_RejectsUnusableRequests(t *testing.T) {
	tests := map[string]struct {
		method string
		path   string // "{id}" stands for an announced contribution
		body   string
		header []string
		want   int
	}{
		"unknown endpoint":         {method: http.MethodPost, path: "playlist.m3u8", body: "{}", want: http.StatusNotFound},
		"announce not a POST":      {method: http.MethodGet, path: "announce", want: http.StatusMethodNotAllowed},
		"record not a POST":        {method: http.MethodPut, path: "contributions/{id}/records", body: `"x"`, want: http.StatusMethodNotAllowed},
		"end not a DELETE":         {method: http.MethodPost, path: "contributions/{id}", want: http.StatusMethodNotAllowed},
		"body is not JSON":         {method: http.MethodPost, path: "announce", body: "hello", want: http.StatusBadRequest},
		"no broadcast path":        {method: http.MethodPost, path: "announce", body: `{"track_name":"chat"}`, want: http.StatusBadRequest},
		"empty path segment":       {method: http.MethodPost, path: "announce", body: `{"broadcast_path":"/room//123","track_name":"chat"}`, want: http.StatusBadRequest},
		"dot-dot path segment":     {method: http.MethodPost, path: "announce", body: `{"broadcast_path":"/room/../123","track_name":"chat"}`, want: http.StatusBadRequest},
		"no track name":            {method: http.MethodPost, path: "announce", body: `{"broadcast_path":"/room/123"}`, want: http.StatusBadRequest},
		"track name has a slash":   {method: http.MethodPost, path: "announce", body: `{"broadcast_path":"/room/123","track_name":"a/b"}`, want: http.StatusBadRequest},
		"announce over the limit":  {method: http.MethodPost, path: "announce", body: `{"broadcast_path":"/` + strings.Repeat("b", 200) + `","track_name":"chat"}`, want: http.StatusRequestEntityTooLarge},
		"record with no body":      {method: http.MethodPost, path: "contributions/{id}/records", want: http.StatusBadRequest},
		"record body not JSON":     {method: http.MethodPost, path: "contributions/{id}/records", body: "hello", want: http.StatusBadRequest},
		"record body two values":   {method: http.MethodPost, path: "contributions/{id}/records", body: "1 2", want: http.StatusBadRequest},
		"record body not UTF-8":    {method: http.MethodPost, path: "contributions/{id}/records", body: "\"\x82\xb1\x82\xf1\"", want: http.StatusBadRequest},
		"record over the limit":    {method: http.MethodPost, path: "contributions/{id}/records", body: `"` + strings.Repeat("a", 200) + `"`, want: http.StatusRequestEntityTooLarge},
		"idempotency key too long": {method: http.MethodPost, path: "contributions/{id}/records", body: `"x"`, header: []string{"Idempotency-Key", strings.Repeat("k", 256)}, want: http.StatusBadRequest},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			s := memstore.New()
			h := newHandler(t, s, ingest.Options{MaxBodyBytes: 128})
			id := announce(t, h)

			rr := send(h, tt.method, strings.ReplaceAll(tt.path, "{id}", id), tt.body, tt.header...)

			assert.Equal(t, tt.want, rr.Code)
			assert.Empty(t, stored(t, s))
		})
	}
}

func TestNewHandler_NilStore(t *testing.T) {
	h, err := ingest.NewHandler(nil, ingest.Options{})

	assert.Error(t, err)
	assert.Nil(t, h)
}
