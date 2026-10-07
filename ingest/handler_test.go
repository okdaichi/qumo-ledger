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
	"time"

	"github.com/okdaichi/qumo-ledger/ingest"
	"github.com/okdaichi/qumo-ledger/ledger"
	"github.com/okdaichi/qumo-ledger/ledger/store"
	"github.com/okdaichi/qumo-ledger/ledger/store/memstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const chatTrack = ledger.TrackPath("room/123/chat")

func newHandler(tb testing.TB, s store.Store, opts ingest.Options) http.Handler {
	tb.Helper()
	h, err := ingest.NewHandler(s, opts)
	require.NoError(tb, err)
	return http.StripPrefix("/ingest", h)
}

// send sends body to the handler at path, below its mount point.
func send(h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(method, "/ingest/"+path, strings.NewReader(body)))
	return rr
}

func announcement(name string) string {
	return `{"broadcast_path":"/room/123","track_name":"chat","name":"` + name + `"`
}

// announce starts a contribution for name and returns its ID.
func announce(tb testing.TB, h http.Handler, name string) string {
	tb.Helper()
	rr := send(h, http.MethodPost, "announce", announcement(name)+"}")
	require.Equal(tb, http.StatusCreated, rr.Code, rr.Body.String())
	var response struct {
		ID string `json:"id"`
	}
	require.NoError(tb, json.Unmarshal(rr.Body.Bytes(), &response))
	require.Equal(tb, "contributions/"+response.ID, rr.Header().Get("Location"))
	return response.ID
}

func record(h http.Handler, id, payload string) *httptest.ResponseRecorder {
	return send(h, http.MethodPost, "contributions/"+id+"/records", payload)
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
		var record ingest.Record
		require.NoError(tb, json.Unmarshal(data, &record))
		records = append(records, record)
	}
}

func TestHandler_Announce_CreatesTheTrackOnce(t *testing.T) {
	s := memstore.New()
	h := newHandler(t, s, ingest.Options{})

	first := send(h, http.MethodPost, "announce", announcement("alice")+"}")
	second := send(h, http.MethodPost, "announce", announcement("bob")+"}")

	require.Equal(t, http.StatusCreated, first.Code)
	assert.Contains(t, first.Body.String(), `"track":"room/123/chat","created":true`)
	require.Equal(t, http.StatusCreated, second.Code, "another contributor starts its own contribution")
	assert.Contains(t, second.Body.String(), `"created":false`)
	assert.NotEqual(t, first.Header().Get("Location"), second.Header().Get("Location"))

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
	alice := announce(t, h, "alice")
	bob := announce(t, h, "bob")

	first := record(h, alice, `{"text":"hello"}`)
	second := record(h, bob, `"hi"`)

	require.Equal(t, http.StatusCreated, first.Code)
	require.Equal(t, http.StatusCreated, second.Code)
	assert.Equal(t, []ingest.Record{
		{Name: "alice", Payload: json.RawMessage(`{"text":"hello"}`)},
		{Name: "bob", Payload: json.RawMessage(`"hi"`)},
	}, stored(t, s))

	var response struct {
		Group     string `json:"group"`
		Wallclock int64  `json:"wallclock"`
	}
	require.NoError(t, json.Unmarshal(second.Body.Bytes(), &response))
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

func TestHandler_AnnounceAgain_EndsThePreviousContribution(t *testing.T) {
	s := memstore.New()
	h := newHandler(t, s, ingest.Options{})
	old := announce(t, h, "alice")
	current := announce(t, h, "alice")

	assert.Equal(t, http.StatusGone, record(h, old, `"stale"`).Code)
	assert.Equal(t, http.StatusCreated, record(h, current, `"fresh"`).Code)
	assert.Equal(t, []ingest.Record{{Name: "alice", Payload: json.RawMessage(`"fresh"`)}}, stored(t, s))
}

func TestHandler_End_StopsTheContribution(t *testing.T) {
	h := newHandler(t, memstore.New(), ingest.Options{})
	alice := announce(t, h, "alice")

	end := send(h, http.MethodDelete, "contributions/"+alice, "")

	assert.Equal(t, http.StatusNoContent, end.Code)
	assert.Equal(t, http.StatusGone, record(h, alice, `"late"`).Code)
	assert.Equal(t, http.StatusGone, send(h, http.MethodDelete, "contributions/"+alice, "").Code)
}

func TestHandler_IdleContributionEnds(t *testing.T) {
	const idle = 20 * time.Millisecond
	h := newHandler(t, memstore.New(), ingest.Options{IdleTimeout: idle})
	alice := announce(t, h, "alice")
	bob := announce(t, h, "bob")

	// Bob keeps recording; alice goes quiet.
	for range 4 {
		time.Sleep(idle / 2)
		require.Equal(t, http.StatusCreated, record(h, bob, `"still here"`).Code)
	}

	// Alice has ended (410), or already been forgotten after a second idle
	// timeout (404).
	assert.Contains(t, []int{http.StatusGone, http.StatusNotFound}, record(h, alice, `"back"`).Code)
	assert.Eventually(t, func() bool {
		return record(h, alice, `"back"`).Code == http.StatusNotFound
	}, time.Second, idle/4, "an ended contribution is forgotten after another idle timeout")
}

func TestHandler_Record_ConcurrentContributorsAllCommit(t *testing.T) {
	s := memstore.New()
	h := newHandler(t, s, ingest.Options{})
	const contributors, each = 8, 10
	ids := make([]string, contributors)
	for c := range contributors {
		ids[c] = announce(t, h, fmt.Sprintf("user-%d", c))
	}

	var wg sync.WaitGroup
	for _, id := range ids {
		wg.Go(func() {
			for i := range each {
				rr := record(h, id, fmt.Sprintf(`"%d"`, i))
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
	alice := announce(t, h, "alice")
	bob := announce(t, h, "bob")

	record(h, alice, `"one"`)
	record(h, bob, `"two"`)

	require.Len(t, seen, 2)
	assert.Equal(t, alice, seen[0].ID)
	assert.Equal(t, chatTrack, seen[0].Track())
	assert.Equal(t, "/room/123", seen[0].BroadcastPath)
	assert.Equal(t, "chat", seen[0].TrackName)
	assert.Equal(t, "alice", seen[0].Name)
	assert.JSONEq(t, `{"name":"alice","payload":"one"}`, string(seen[0].Data))
	assert.Equal(t, uint64(0), seen[0].Group.ID.Sequence())
	assert.Equal(t, uint64(1), seen[1].Group.ID.Sequence())
	assert.NotEmpty(t, seen[1].Group.ObjectKey, "the hook sees the group as committed")
}

func TestHandler_OnAnnounce_ReportsWhetherTheTrackIsNew(t *testing.T) {
	var seen []ingest.Announced
	h := newHandler(t, memstore.New(), ingest.Options{
		OnAnnounce: func(_ context.Context, a ingest.Announced) { seen = append(seen, a) },
	})

	alice := announce(t, h, "alice")
	bob := announce(t, h, "bob")

	require.Len(t, seen, 2)
	assert.Equal(t, ingest.Announced{
		Contribution: ingest.Contribution{
			ID:           alice,
			Announcement: ingest.Announcement{BroadcastPath: "/room/123", TrackName: "chat", Name: "alice"},
		},
		Created: true,
	}, seen[0])
	assert.Equal(t, bob, seen[1].ID)
	assert.False(t, seen[1].Created)
}

func TestHandler_Authorize(t *testing.T) {
	s := memstore.New()
	var asked []ingest.Announcement
	allowed := map[string]bool{"alice": true}
	h := newHandler(t, s, ingest.Options{
		Authorize: func(r *http.Request, a ingest.Announcement) error {
			asked = append(asked, a)
			switch {
			case r.Header.Get("Authorization") == "":
				return fmt.Errorf("no credential: %w", ingest.ErrUnauthenticated)
			case !allowed[a.Name]:
				return errors.New("not allowed")
			}
			return nil
		},
	})
	signed := func(method, path, body string) int {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(method, "/ingest/"+path, strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer credential")
		h.ServeHTTP(rr, req)
		return rr.Code
	}

	assert.Equal(t, http.StatusUnauthorized, send(h, http.MethodPost, "announce", announcement("alice")+"}").Code)
	assert.Equal(t, http.StatusForbidden, signed(http.MethodPost, "announce", announcement("mallory")+"}"))

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/ingest/announce", strings.NewReader(announcement("alice")+"}"))
	req.Header.Set("Authorization", "Bearer credential")
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusCreated, rr.Code)
	var response struct {
		ID string `json:"id"`
	}
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &response))

	// Every record is authorized again, as the contribution's announcement.
	assert.Equal(t, http.StatusUnauthorized, record(h, response.ID, `"unsigned"`).Code)
	allowed["alice"] = false
	assert.Equal(t, http.StatusForbidden, signed(http.MethodPost, "contributions/"+response.ID+"/records", `"revoked"`))
	assert.Equal(t, http.StatusForbidden, signed(http.MethodDelete, "contributions/"+response.ID, ""))

	assert.Empty(t, stored(t, s))
	require.Len(t, asked, 6)
	assert.Equal(t, ingest.Announcement{BroadcastPath: "/room/123", TrackName: "chat", Name: "alice"}, asked[5])
}

func TestHandler_RejectsUnusableRequests(t *testing.T) {
	tests := map[string]struct {
		method string
		path   string // "{id}" stands for alice's contribution
		body   string
		want   int
	}{
		"unknown endpoint":       {method: http.MethodPost, path: "playlist.m3u8", body: "{}", want: http.StatusNotFound},
		"announce not a POST":    {method: http.MethodGet, path: "announce", want: http.StatusMethodNotAllowed},
		"record not a POST":      {method: http.MethodPut, path: "contributions/{id}/records", body: `"x"`, want: http.StatusMethodNotAllowed},
		"body is not JSON":       {method: http.MethodPost, path: "announce", body: "hello", want: http.StatusBadRequest},
		"no broadcast path":      {method: http.MethodPost, path: "announce", body: `{"track_name":"chat","name":"bob"}`, want: http.StatusBadRequest},
		"no track name":          {method: http.MethodPost, path: "announce", body: `{"broadcast_path":"/room/123","name":"bob"}`, want: http.StatusBadRequest},
		"track name has a slash": {method: http.MethodPost, path: "announce", body: `{"broadcast_path":"/room/123","track_name":"a/b","name":"bob"}`, want: http.StatusBadRequest},
		"no name":                {method: http.MethodPost, path: "announce", body: `{"broadcast_path":"/room/123","track_name":"chat"}`, want: http.StatusBadRequest},
		"name has a slash":       {method: http.MethodPost, path: "announce", body: announcement("bob/carol") + "}", want: http.StatusBadRequest},
		"name is a dot segment":  {method: http.MethodPost, path: "announce", body: announcement("..") + "}", want: http.StatusBadRequest},
		"record with no body":    {method: http.MethodPost, path: "contributions/{id}/records", want: http.StatusBadRequest},
		"record body not JSON":   {method: http.MethodPost, path: "contributions/{id}/records", body: "hello", want: http.StatusBadRequest},
		"record over the limit":  {method: http.MethodPost, path: "contributions/{id}/records", body: `"` + strings.Repeat("a", 200) + `"`, want: http.StatusRequestEntityTooLarge},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			s := memstore.New()
			h := newHandler(t, s, ingest.Options{MaxBodyBytes: 128})
			alice := announce(t, h, "alice")

			rr := send(h, tt.method, strings.ReplaceAll(tt.path, "{id}", alice), tt.body)

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
