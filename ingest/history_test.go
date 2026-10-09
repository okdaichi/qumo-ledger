package ingest_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"testing/synctest"
	"time"

	"github.com/okdaichi/qumo-ledger/ingest"
	"github.com/okdaichi/qumo-ledger/ledger/store/mem"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// historyPage is the body of a history response.
type historyPage struct {
	Records []struct {
		Group     string          `json:"group"`
		Wallclock int64           `json:"wallclock"`
		Sender    string          `json:"sender"`
		Payload   json.RawMessage `json:"payload"`
		Redacted  bool            `json:"redacted"`
		Redacts   string          `json:"redacts"`
	} `json:"records"`
	Before string `json:"before"`
}

func history(tb testing.TB, h http.Handler, query string) historyPage {
	tb.Helper()
	rr := send(h, http.MethodGet, chatURL+query, "")
	require.Equal(tb, http.StatusOK, rr.Code, rr.Body.String())
	var page historyPage
	require.NoError(tb, json.Unmarshal(rr.Body.Bytes(), &page))
	return page
}

func (p historyPage) payloads() []string {
	out := make([]string, len(p.Records))
	for i, rec := range p.Records {
		out[i] = string(rec.Payload)
	}
	return out
}

func TestHandler_History_PagesBackwards(t *testing.T) {
	h := newHandler(t, mem.New(), ingest.Options{})
	for i := range 5 {
		require.Equal(t, http.StatusCreated, record(h, fmt.Sprintf("%d", i)).Code)
	}

	newest := history(t, h, "?limit=2")
	middle := history(t, h, "?limit=2&before="+newest.Before)
	oldest := history(t, h, "?limit=2&before="+middle.Before)
	last := history(t, h, "?limit=2&before="+oldest.Records[0].Group)

	assert.Equal(t, []string{"3", "4"}, newest.payloads(), "a page is oldest first")
	assert.Equal(t, []string{"1", "2"}, middle.payloads())
	assert.Equal(t, []string{"0"}, oldest.payloads())
	assert.Empty(t, oldest.Before, "a short page is the last")
	assert.Empty(t, last.Records)
	assert.Equal(t, newest.Records[0].Group, newest.Before, "the cursor is the page's first group")
	assert.NotZero(t, newest.Records[0].Wallclock)
}

func TestHandler_History_DefaultPageAndSenders(t *testing.T) {
	h := newHandler(t, mem.New(), ingest.Options{
		Authorize: func(r *http.Request, _ ingest.Track, _ ingest.Access) (string, error) {
			return r.Header.Get("X-Sender"), nil
		},
	})
	for i := range ingest.DefaultPageSize + 1 {
		require.Equal(t, http.StatusCreated, record(h, fmt.Sprintf("%d", i), "X-Sender", "user-42").Code)
	}
	require.Equal(t, http.StatusCreated, record(h, `"system"`).Code)

	page := history(t, h, "")

	require.Len(t, page.Records, ingest.DefaultPageSize)
	assert.Equal(t, "user-42", page.Records[0].Sender)
	assert.Empty(t, page.Records[len(page.Records)-1].Sender)
	assert.Equal(t, `"system"`, string(page.Records[len(page.Records)-1].Payload))
	assert.NotEmpty(t, page.Before)
}

func TestHandler_History_EmptyTrack(t *testing.T) {
	h := newHandler(t, mem.New(), ingest.Options{})
	require.Equal(t, http.StatusCreated, send(h, http.MethodPut, chatURL, "").Code)

	page := history(t, h, "")

	assert.Empty(t, page.Records)
	assert.Empty(t, page.Before)
}

func TestHandler_History_RejectsUnusableQueries(t *testing.T) {
	h := newHandler(t, mem.New(), ingest.Options{})
	require.Equal(t, http.StatusCreated, record(h, `"x"`).Code)

	for name, query := range map[string]string{
		"a zero limit":         "?limit=0",
		"a limit too high":     fmt.Sprintf("?limit=%d", ingest.MaxPageSize+1),
		"a limit not a number": "?limit=many",
		"a cursor not a group": "?before=yesterday",
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, http.StatusBadRequest, send(h, http.MethodGet, chatURL+query, "").Code)
		})
	}
}

func TestHandler_Limits(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := mem.New()
		h := newHandler(t, s, ingest.Options{
			Authorize: func(r *http.Request, _ ingest.Track, _ ingest.Access) (string, error) {
				return r.Header.Get("X-Sender"), nil
			},
			SenderLimit: ingest.Limit{Rate: 1, Burst: 2},
			TrackLimit:  ingest.Limit{Rate: 10, Burst: 4},
		})
		as := func(sender, payload string) int {
			return record(h, payload, "X-Sender", sender).Code
		}

		// Alice spends her burst of two; the third is refused.
		assert.Equal(t, http.StatusCreated, as("alice", `"a1"`))
		assert.Equal(t, http.StatusCreated, as("alice", `"a2"`))
		refused := record(h, `"a3"`, "X-Sender", "alice")
		assert.Equal(t, http.StatusTooManyRequests, refused.Code)
		assert.Equal(t, "2", refused.Header().Get("Retry-After"), "a token is a second away, rounded up")

		// Bob has his own bucket; the track's burst of four runs out after two
		// more records.
		assert.Equal(t, http.StatusCreated, as("bob", `"b1"`))
		assert.Equal(t, http.StatusCreated, as("bob", `"b2"`))
		assert.Equal(t, http.StatusTooManyRequests, as("carol", `"c1"`), "the track's limit holds for every sender")

		// A second later both buckets have refilled enough.
		time.Sleep(time.Second)
		assert.Equal(t, http.StatusCreated, as("alice", `"a3"`))

		assert.Equal(t, []string{`"a1"`, `"a2"`, `"b1"`, `"b2"`, `"a3"`}, payloads(stored(t, s)),
			"a refused record is not stored")
	})
}

func TestHandler_Limits_SystemRecordsCountOnlyTowardTheTrack(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHandler(t, mem.New(), ingest.Options{
			SenderLimit: ingest.Limit{Rate: 1, Burst: 1},
			TrackLimit:  ingest.Limit{Rate: 1, Burst: 3},
		})

		assert.Equal(t, http.StatusCreated, record(h, `"1"`).Code)
		assert.Equal(t, http.StatusCreated, record(h, `"2"`).Code, "no sender, no sender limit")
		assert.Equal(t, http.StatusCreated, record(h, `"3"`).Code)
		assert.Equal(t, http.StatusTooManyRequests, record(h, `"4"`).Code)
	})
}

func TestHandler_Limits_RetryDoesNotSpend(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHandler(t, mem.New(), ingest.Options{TrackLimit: ingest.Limit{Rate: 1, Burst: 1}})

		assert.Equal(t, http.StatusCreated, record(h, `"x"`, "Idempotency-Key", "k").Code)
		assert.Equal(t, http.StatusCreated, record(h, `"x"`, "Idempotency-Key", "k").Code,
			"a retry is answered from the first reply without a token")
		assert.Equal(t, http.StatusTooManyRequests, record(h, `"y"`).Code)
	})
}

func TestHandler_Limits_ForgetRefilledSenders(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHandler(t, mem.New(), ingest.Options{
			Authorize: func(r *http.Request, _ ingest.Track, _ ingest.Access) (string, error) {
				return r.Header.Get("X-Sender"), nil
			},
			SenderLimit: ingest.Limit{Rate: 1, Burst: 1},
		})

		// Many senders, each spending its bucket, then all refilling.
		for i := range 1100 {
			require.Equal(t, http.StatusCreated, record(h, `"x"`, "X-Sender", fmt.Sprintf("user-%d", i)).Code)
		}
		time.Sleep(2 * time.Second)

		// A sender forgotten while its bucket was full starts full again.
		assert.Equal(t, http.StatusCreated, record(h, `"x"`, "X-Sender", "user-0").Code)
		assert.Equal(t, http.StatusTooManyRequests, record(h, `"y"`, "X-Sender", "user-0").Code)
	})
}
