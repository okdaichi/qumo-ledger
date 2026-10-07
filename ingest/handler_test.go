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

	"github.com/okdaichi/qumo-ledger/ingest"
	"github.com/okdaichi/qumo-ledger/ledger"
	"github.com/okdaichi/qumo-ledger/ledger/store"
	"github.com/okdaichi/qumo-ledger/ledger/store/mem"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	chatTrack = ledger.TrackPath("room/123/chat")
	chatURL   = "tracks/room/123/chat"
)

var chat = ingest.Track{BroadcastPath: "/room/123", TrackName: "chat"}

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

func record(h http.Handler, payload string, header ...string) *httptest.ResponseRecorder {
	return send(h, http.MethodPost, chatURL, payload, header...)
}

// stored reads every record committed to the chat track, in commit order.
func stored(tb testing.TB, s store.Store) []ingest.Record {
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

	var records []ingest.Record
	for {
		group, err := reader.Next(ctx)
		if errors.Is(err, io.EOF) {
			return records
		}
		require.NoError(tb, err)
		data, err := reader.ReadGroup(ctx, group.ObjectKey)
		require.NoError(tb, err)
		var rec ingest.Record
		require.NoError(tb, json.Unmarshal(data, &rec))
		records = append(records, rec)
	}
}

// payloads returns the payloads of records as strings.
func payloads(records []ingest.Record) []string {
	if records == nil {
		return nil
	}
	out := make([]string, len(records))
	for i, rec := range records {
		out[i] = string(rec.Payload)
	}
	return out
}

func TestTrack_Path(t *testing.T) {
	tests := map[string]struct {
		track ingest.Track
		want  ledger.TrackPath
	}{
		"rooted":     {track: chat, want: chatTrack},
		"not rooted": {track: ingest.Track{BroadcastPath: "room/123", TrackName: "chat"}, want: chatTrack},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.track.Path())
		})
	}
}

func TestHandler_Create(t *testing.T) {
	s := mem.New()
	var opened []ingest.Track
	h := newHandler(t, s, ingest.Options{
		OnOpen: func(_ context.Context, tr ingest.Track) { opened = append(opened, tr) },
	})

	first := send(h, http.MethodPut, chatURL, "")
	again := send(h, http.MethodPut, chatURL, "")

	assert.Equal(t, http.StatusCreated, first.Code)
	assert.Equal(t, http.StatusNoContent, again.Code, "creating an existing track succeeds")
	assert.Equal(t, []ingest.Track{chat}, opened, "a track is opened once")

	track, err := ledger.Open(context.Background(), s, chatTrack, ledger.Config{})
	require.NoError(t, err)
	root := track.Root()
	assert.Equal(t, ledger.TimeSourceIngest, root.TimeSource)
	assert.Equal(t, ingest.Encoding, root.Encoding)
	assert.Equal(t, ingest.MIME, root.MIME)
	assert.Empty(t, stored(t, s))
}

func TestHandler_Create_ExistingTrackInAnotherHandler(t *testing.T) {
	s := mem.New()
	require.Equal(t, http.StatusCreated, send(newHandler(t, s, ingest.Options{}), http.MethodPut, chatURL, "").Code)

	rr := send(newHandler(t, s, ingest.Options{}), http.MethodPut, chatURL, "")

	assert.Equal(t, http.StatusNoContent, rr.Code, "a track another run created exists")
}

func TestHandler_Record_StoresThePayloadInCommitOrder(t *testing.T) {
	s := mem.New()
	var opened []ingest.Track
	h := newHandler(t, s, ingest.Options{
		OnOpen: func(_ context.Context, tr ingest.Track) { opened = append(opened, tr) },
	})

	one := record(h, `{"text":"hello"}`)
	two := record(h, `"hi"`)

	require.Equal(t, http.StatusCreated, one.Code)
	require.Equal(t, http.StatusCreated, two.Code)
	assert.Equal(t, []ingest.Record{
		{Payload: json.RawMessage(`{"text":"hello"}`)},
		{Payload: json.RawMessage(`"hi"`)},
	}, stored(t, s), "the payload is stored as it was sent, with no sender when none was named")
	assert.Equal(t, []ingest.Track{chat}, opened, "the first record creates and opens the track")

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

func TestHandler_Record_CarriesTheSenderAuthorizeNames(t *testing.T) {
	s := mem.New()
	var delivered []string
	h := newHandler(t, s, ingest.Options{
		Authorize: func(r *http.Request, _ ingest.Track, _ ingest.Access) (string, error) {
			return r.Header.Get("X-Sender"), nil
		},
		OnRecord: func(_ context.Context, _ ingest.Track, _ ledger.GroupInfo, rec []byte) {
			delivered = append(delivered, string(rec))
		},
	})

	require.Equal(t, http.StatusCreated, record(h, `{"user":"mallory","text":"hi"}`, "X-Sender", "user-42").Code)
	require.Equal(t, http.StatusCreated, record(h, `{"type":"delete"}`).Code)

	assert.Equal(t, []ingest.Record{
		{Sender: "user-42", Payload: json.RawMessage(`{"user":"mallory","text":"hi"}`)},
		{Payload: json.RawMessage(`{"type":"delete"}`)},
	}, stored(t, s), "the sender comes from Authorize, whatever the payload claims")
	assert.Equal(t, []string{
		`{"sender":"user-42","payload":{"user":"mallory","text":"hi"}}`,
		`{"payload":{"type":"delete"}}`,
	}, delivered, "OnRecord gets the record as stored")
}

func TestHandler_Record_TracksAreSeparate(t *testing.T) {
	s := mem.New()
	h := newHandler(t, s, ingest.Options{})

	require.Equal(t, http.StatusCreated, record(h, `"chat"`).Code)
	require.Equal(t, http.StatusCreated, send(h, http.MethodPost, "tracks/room/123/reactions", `"like"`).Code)
	require.Equal(t, http.StatusCreated, send(h, http.MethodPost, "tracks/room/9/chat", `"elsewhere"`).Code)

	assert.Equal(t, []string{`"chat"`}, payloads(stored(t, s)))
}

func TestHandler_Record_IdempotencyKey(t *testing.T) {
	s := mem.New()
	var delivered int
	h := newHandler(t, s, ingest.Options{
		OnRecord: func(context.Context, ingest.Track, ledger.GroupInfo, []byte) { delivered++ },
	})

	original := record(h, `"hello"`, "Idempotency-Key", "k1")
	retry := record(h, `"hello"`, "Idempotency-Key", "k1")
	otherKey := record(h, `"again"`, "Idempotency-Key", "k2")
	otherTrack := send(h, http.MethodPost, "tracks/room/9/chat", `"hello"`, "Idempotency-Key", "k1")
	unkeyed := record(h, `"unkeyed"`)
	unkeyedAgain := record(h, `"unkeyed"`)

	for _, rr := range []*httptest.ResponseRecorder{original, retry, otherKey, otherTrack, unkeyed, unkeyedAgain} {
		require.Equal(t, http.StatusCreated, rr.Code)
	}
	assert.JSONEq(t, original.Body.String(), retry.Body.String(), "a retry is answered as the first was")
	assert.Equal(t, []string{`"hello"`, `"again"`, `"unkeyed"`, `"unkeyed"`}, payloads(stored(t, s)),
		"records without a key are never merged")
	assert.Equal(t, 5, delivered, "a retry is not delivered again; the other track's record is")
}

func TestHandler_Record_IdempotencyKeysAreBounded(t *testing.T) {
	s := mem.New()
	h := newHandler(t, s, ingest.Options{})
	const remembered = 1024

	for i := range remembered + 1 {
		require.Equal(t, http.StatusCreated, record(h, fmt.Sprintf("%d", i), "Idempotency-Key", fmt.Sprintf("k%d", i)).Code)
	}
	newest := record(h, "0", "Idempotency-Key", fmt.Sprintf("k%d", remembered))
	oldest := record(h, "0", "Idempotency-Key", "k0")

	require.Equal(t, http.StatusCreated, newest.Code)
	require.Equal(t, http.StatusCreated, oldest.Code)
	assert.Len(t, stored(t, s), remembered+2, "the oldest key was forgotten, so its retry is stored again")
}

func TestHandler_Record_ConcurrentSendersAllCommit(t *testing.T) {
	s := mem.New()
	h := newHandler(t, s, ingest.Options{})
	const senders, each = 8, 10

	var wg sync.WaitGroup
	for c := range senders {
		wg.Go(func() {
			for i := range each {
				rr := record(h, fmt.Sprintf(`{"c":%d,"i":%d}`, c, i))
				assert.Equal(t, http.StatusCreated, rr.Code)
			}
		})
	}
	wg.Wait()

	records := stored(t, s)
	require.Len(t, records, senders*each)
	// Each sender's own records keep the order it sent them in.
	next := make(map[int]int)
	for _, rec := range records {
		var p struct{ C, I int }
		require.NoError(t, json.Unmarshal(rec.Payload, &p))
		assert.Equal(t, next[p.C], p.I)
		next[p.C]++
	}
}

func TestHandler_OnRecord_SeesCommittedRecordsInOrder(t *testing.T) {
	type delivery struct {
		track  ingest.Track
		group  ledger.GroupInfo
		record string
	}
	var seen []delivery
	h := newHandler(t, mem.New(), ingest.Options{
		OnRecord: func(_ context.Context, tr ingest.Track, g ledger.GroupInfo, rec []byte) {
			seen = append(seen, delivery{track: tr, group: g, record: string(rec)})
		},
	})

	record(h, `"one"`)
	record(h, `"two"`)

	require.Len(t, seen, 2)
	assert.Equal(t, chat, seen[0].track)
	assert.Equal(t, `{"payload":"one"}`, seen[0].record)
	assert.Equal(t, uint64(0), seen[0].group.ID.Sequence())
	assert.Equal(t, uint64(1), seen[1].group.ID.Sequence())
	assert.NotEmpty(t, seen[1].group.ObjectKey, "the hook sees the group as committed")
}

func TestHandler_Authorize(t *testing.T) {
	s := mem.New()
	type ask struct {
		track  ingest.Track
		access ingest.Access
	}
	var asked []ask
	allowed := true
	h := newHandler(t, s, ingest.Options{
		Authorize: func(r *http.Request, tr ingest.Track, access ingest.Access) (string, error) {
			asked = append(asked, ask{track: tr, access: access})
			switch {
			case r.Header.Get("Authorization") == "":
				return "", fmt.Errorf("no credential: %w", ingest.ErrUnauthenticated)
			case !allowed:
				return "", errors.New("not allowed")
			}
			return "", nil
		},
		Challenge: "Bearer",
	})
	const credential = "Bearer credential"

	unauthenticated := record(h, `"unsigned"`)
	assert.Equal(t, http.StatusUnauthorized, unauthenticated.Code)
	assert.Equal(t, "Bearer", unauthenticated.Header().Get("WWW-Authenticate"), "a 401 names the scheme")
	assert.Equal(t, http.StatusUnauthorized, send(h, http.MethodPut, chatURL, "").Code)
	assert.Equal(t, http.StatusUnauthorized, send(h, http.MethodGet, chatURL, "").Code)

	allowed = false
	assert.Equal(t, http.StatusForbidden, record(h, `"refused"`, "Authorization", credential).Code)
	assert.Equal(t, http.StatusForbidden, send(h, http.MethodPut, chatURL, "", "Authorization", credential).Code)
	assert.Equal(t, http.StatusForbidden, send(h, http.MethodGet, chatURL, "", "Authorization", credential).Code)

	allowed = true
	assert.Equal(t, http.StatusCreated, record(h, `"signed"`, "Authorization", credential).Code)
	assert.Equal(t, http.StatusOK, send(h, http.MethodGet, chatURL, "", "Authorization", credential).Code)

	assert.Equal(t, []string{`"signed"`}, payloads(stored(t, s)), "a refused request creates and stores nothing")
	assert.Equal(t, []ask{
		{chat, ingest.Write}, {chat, ingest.Write}, {chat, ingest.Read},
		{chat, ingest.Write}, {chat, ingest.Write}, {chat, ingest.Read},
		{chat, ingest.Write}, {chat, ingest.Read},
	}, asked, "records and creates are writes, history is a read")
}

func TestHandler_RejectsUnusableRequests(t *testing.T) {
	tests := map[string]struct {
		method string
		path   string
		body   string
		header []string
		want   int
	}{
		"unknown endpoint":         {method: http.MethodPost, path: "playlist.m3u8", body: `"x"`, want: http.StatusNotFound},
		"not PUT, POST or GET":     {method: http.MethodDelete, path: chatURL, want: http.StatusMethodNotAllowed},
		"no track":                 {method: http.MethodPost, path: "tracks/", body: `"x"`, want: http.StatusBadRequest},
		"no broadcast path":        {method: http.MethodPost, path: "tracks/chat", body: `"x"`, want: http.StatusBadRequest},
		"a trailing slash":         {method: http.MethodPost, path: "tracks/room/123/chat/", body: `"x"`, want: http.StatusBadRequest},
		"no body":                  {method: http.MethodPost, path: chatURL, want: http.StatusBadRequest},
		"body not JSON":            {method: http.MethodPost, path: chatURL, body: "hello", want: http.StatusBadRequest},
		"body two values":          {method: http.MethodPost, path: chatURL, body: "1 2", want: http.StatusBadRequest},
		"body not UTF-8":           {method: http.MethodPost, path: chatURL, body: "\"\x82\xb1\x82\xf1\"", want: http.StatusBadRequest},
		"body over the limit":      {method: http.MethodPost, path: chatURL, body: `"` + strings.Repeat("a", 200) + `"`, want: http.StatusRequestEntityTooLarge},
		"idempotency key too long": {method: http.MethodPost, path: chatURL, body: `"x"`, header: []string{"Idempotency-Key", strings.Repeat("k", 256)}, want: http.StatusBadRequest},
		"history of no track":      {method: http.MethodGet, path: chatURL, want: http.StatusNotFound},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			s := mem.New()
			h := newHandler(t, s, ingest.Options{MaxBodyBytes: 128})

			rr := send(h, tt.method, tt.path, tt.body, tt.header...)

			assert.Equal(t, tt.want, rr.Code)
			assert.Empty(t, stored(t, s))
			_, err := ledger.Open(context.Background(), s, chatTrack, ledger.Config{})
			assert.ErrorIs(t, err, ledger.ErrTrackNotFound, "an unusable request creates no track")
		})
	}
}

func TestNewHandler_NilStore(t *testing.T) {
	h, err := ingest.NewHandler(nil, ingest.Options{})

	assert.Error(t, err)
	assert.Nil(t, h)
}
