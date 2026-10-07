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
	"github.com/okdaichi/qumo-ledger/ledger/store/memstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const chatTrack = ledger.TrackPath("room/123/chat")

func newHandler(tb testing.TB, s store.Store, opts ingest.Options) *ingest.Handler {
	tb.Helper()
	h, err := ingest.NewHandler(s, opts)
	require.NoError(tb, err)
	return h
}

// post sends body to the handler's endpoint and returns the response.
func post(h http.Handler, endpoint, body string) *httptest.ResponseRecorder {
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/ingest/"+endpoint, strings.NewReader(body)))
	return rr
}

func request(name, payload string) string {
	body := `{"broadcast_path":"/room/123","track_name":"chat","name":"` + name + `"`
	if payload != "" {
		body += `,"payload":` + payload
	}
	return body + "}"
}

// stored reads every record committed to the chat track, in commit order.
func stored(tb testing.TB, s store.Store) []ingest.Record {
	tb.Helper()
	ctx := context.Background()
	track, err := ledger.Open(ctx, s, chatTrack, ledger.Config{})
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
		var record ingest.Record
		require.NoError(tb, json.Unmarshal(data, &record))
		records = append(records, record)
	}
}

func TestHandler_Announce_CreatesTheTrackOnce(t *testing.T) {
	s := memstore.New()
	h := newHandler(t, s, ingest.Options{})

	first := post(h, "announce", request("alice", ""))
	second := post(h, "announce", request("bob", ""))

	require.Equal(t, http.StatusCreated, first.Code)
	assert.JSONEq(t, `{"track":"room/123/chat","created":true}`, first.Body.String())
	require.Equal(t, http.StatusOK, second.Code, "another contributor announces a track that exists")
	assert.JSONEq(t, `{"track":"room/123/chat","created":false}`, second.Body.String())

	track, err := ledger.Open(context.Background(), s, chatTrack, ledger.Config{})
	require.NoError(t, err)
	root := track.Root()
	assert.Equal(t, ledger.TimeSourceIngest, root.TimeSource)
	assert.Equal(t, ingest.Encoding, root.Encoding)
	assert.Equal(t, ingest.MIME, root.MIME)
}

func TestHandler_Record_AppendsInCommitOrder(t *testing.T) {
	s := memstore.New()
	h := newHandler(t, s, ingest.Options{})
	require.Equal(t, http.StatusCreated, post(h, "announce", request("alice", "")).Code)

	first := post(h, "record", request("alice", `{"text":"hello"}`))
	second := post(h, "record", request("bob", `"hi"`))

	require.Equal(t, http.StatusCreated, first.Code)
	require.Equal(t, http.StatusCreated, second.Code)
	assert.Equal(t, []ingest.Record{
		{Name: "alice", Payload: json.RawMessage(`{"text":"hello"}`)},
		{Name: "bob", Payload: json.RawMessage(`"hi"`)},
	}, stored(t, s))

	var response struct {
		Track     string `json:"track"`
		Group     string `json:"group"`
		Wallclock int64  `json:"wallclock"`
	}
	require.NoError(t, json.Unmarshal(second.Body.Bytes(), &response))
	assert.Equal(t, "room/123/chat", response.Track)
	id, err := ledger.ParseGroupID(response.Group)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), id.Sequence(), "the second record is the track's second group")
	assert.NotZero(t, response.Wallclock)
}

func TestHandler_Record_UnannouncedTrackIsNotFound(t *testing.T) {
	h := newHandler(t, memstore.New(), ingest.Options{})

	rr := post(h, "record", request("alice", `"hello"`))

	assert.Equal(t, http.StatusNotFound, rr.Code)
}

func TestHandler_Record_ConcurrentContributorsAllCommit(t *testing.T) {
	s := memstore.New()
	h := newHandler(t, s, ingest.Options{})
	require.Equal(t, http.StatusCreated, post(h, "announce", request("alice", "")).Code)
	const contributors, each = 8, 10

	var wg sync.WaitGroup
	for c := range contributors {
		wg.Go(func() {
			for i := range each {
				rr := post(h, "record", request(fmt.Sprintf("user-%d", c), fmt.Sprintf(`"%d"`, i)))
				assert.Equal(t, http.StatusCreated, rr.Code)
			}
		})
	}
	wg.Wait()

	records := stored(t, s)
	require.Len(t, records, contributors*each)
	// Each contributor's own records keep the order it sent them in.
	next := make(map[string]int)
	for _, record := range records {
		assert.JSONEq(t, fmt.Sprintf(`"%d"`, next[record.Name]), string(record.Payload))
		next[record.Name]++
	}
}

func TestHandler_OnRecord_SeesCommittedRecordsInOrder(t *testing.T) {
	s := memstore.New()
	var seen []ingest.Recorded
	h := newHandler(t, s, ingest.Options{
		OnRecord: func(_ context.Context, rec ingest.Recorded) { seen = append(seen, rec) },
	})
	require.Equal(t, http.StatusCreated, post(h, "announce", request("alice", "")).Code)

	post(h, "record", request("alice", `"one"`))
	post(h, "record", request("bob", `"two"`))

	require.Len(t, seen, 2)
	assert.Equal(t, chatTrack, seen[0].Track)
	assert.Equal(t, "alice", seen[0].Name)
	assert.JSONEq(t, `"one"`, string(seen[0].Payload))
	assert.Equal(t, uint64(0), seen[0].Group.ID.Sequence())
	assert.Equal(t, uint64(1), seen[1].Group.ID.Sequence())
	assert.NotEmpty(t, seen[1].Group.ObjectKey, "the hook sees the group as committed")
}

func TestHandler_OnAnnounce_ReportsWhetherTheTrackIsNew(t *testing.T) {
	var seen []ingest.Announced
	h := newHandler(t, memstore.New(), ingest.Options{
		OnAnnounce: func(_ context.Context, a ingest.Announced) { seen = append(seen, a) },
	})

	post(h, "announce", request("alice", ""))
	post(h, "announce", request("bob", ""))

	assert.Equal(t, []ingest.Announced{
		{Track: chatTrack, Name: "alice", Created: true},
		{Track: chatTrack, Name: "bob", Created: false},
	}, seen)
}

func TestHandler_Authorize_RefusalStoresNothing(t *testing.T) {
	s := memstore.New()
	var asked []ingest.Request
	h := newHandler(t, s, ingest.Options{
		Authorize: func(_ *http.Request, req *ingest.Request) error {
			asked = append(asked, *req)
			if req.Name == "mallory" {
				return errors.New("not allowed")
			}
			return nil
		},
	})
	require.Equal(t, http.StatusCreated, post(h, "announce", request("alice", "")).Code)

	announce := post(h, "announce", request("mallory", ""))
	record := post(h, "record", request("mallory", `"spam"`))

	assert.Equal(t, http.StatusForbidden, announce.Code)
	assert.Equal(t, http.StatusForbidden, record.Code)
	assert.Empty(t, stored(t, s))
	require.Len(t, asked, 3)
	assert.Equal(t, "/room/123", asked[2].BroadcastPath)
	assert.Equal(t, "chat", asked[2].TrackName)
}

func TestHandler_RejectsUnusableRequests(t *testing.T) {
	tests := map[string]struct {
		method   string
		endpoint string
		body     string
		want     int
	}{
		"unknown endpoint":       {method: http.MethodPost, endpoint: "playlist.m3u8", body: request("alice", ""), want: http.StatusNotFound},
		"not a POST":             {method: http.MethodGet, endpoint: "record", want: http.StatusMethodNotAllowed},
		"body is not JSON":       {method: http.MethodPost, endpoint: "announce", body: "hello", want: http.StatusBadRequest},
		"no broadcast path":      {method: http.MethodPost, endpoint: "announce", body: `{"track_name":"chat","name":"alice"}`, want: http.StatusBadRequest},
		"no track name":          {method: http.MethodPost, endpoint: "announce", body: `{"broadcast_path":"/room/123","name":"alice"}`, want: http.StatusBadRequest},
		"track name has a slash": {method: http.MethodPost, endpoint: "announce", body: `{"broadcast_path":"/room/123","track_name":"a/b","name":"alice"}`, want: http.StatusBadRequest},
		"no name":                {method: http.MethodPost, endpoint: "announce", body: `{"broadcast_path":"/room/123","track_name":"chat"}`, want: http.StatusBadRequest},
		"record with no payload": {method: http.MethodPost, endpoint: "record", body: request("alice", ""), want: http.StatusBadRequest},
		"body over the limit":    {method: http.MethodPost, endpoint: "record", body: request("alice", `"`+strings.Repeat("a", 200)+`"`), want: http.StatusRequestEntityTooLarge},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			s := memstore.New()
			h := newHandler(t, s, ingest.Options{MaxBodyBytes: 128})
			require.Equal(t, http.StatusCreated, post(h, "announce", request("alice", "")).Code)
			rr := httptest.NewRecorder()

			h.ServeHTTP(rr, httptest.NewRequest(tt.method, "/ingest/"+tt.endpoint, strings.NewReader(tt.body)))

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
