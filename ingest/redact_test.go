package ingest_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"testing"

	"github.com/okdaichi/qumo-ledger/ingest"
	"github.com/okdaichi/qumo-ledger/ledger"
	"github.com/okdaichi/qumo-ledger/ledger/store/mem"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// posted records payload and returns the group it was committed as.
func posted(tb testing.TB, h http.Handler, payload string, header ...string) string {
	tb.Helper()
	rr := record(h, payload, header...)
	require.Equal(tb, http.StatusCreated, rr.Code, rr.Body.String())
	var reply struct {
		Group string `json:"group"`
	}
	require.NoError(tb, json.Unmarshal(rr.Body.Bytes(), &reply))
	return reply.Group
}

func redact(h http.Handler, group string, header ...string) int {
	return send(h, http.MethodDelete, chatURL+"?group="+group, "", header...).Code
}

func TestHandler_Redact(t *testing.T) {
	var (
		mu        sync.Mutex
		delivered []string
		accesses  []ingest.Access
	)
	h := newHandler(t, mem.New(), ingest.Options{
		Authorize: func(r *http.Request, _ ingest.Track, access ingest.Access) (string, error) {
			mu.Lock()
			defer mu.Unlock()
			accesses = append(accesses, access)
			return r.Header.Get("X-Sender"), nil
		},
		OnRecord: func(_ context.Context, _ ingest.Track, _ ledger.GroupInfo, rec []byte) {
			mu.Lock()
			defer mu.Unlock()
			delivered = append(delivered, string(rec))
		},
	})
	hello := posted(t, h, `"hello"`, "X-Sender", "user-42")
	posted(t, h, `"world"`, "X-Sender", "user-7")

	rr := send(h, http.MethodDelete, chatURL+"?group="+hello, "", "X-Sender", "moderator")

	require.Equal(t, http.StatusCreated, rr.Code, rr.Body.String())
	assert.Equal(t, ingest.Redact, accesses[len(accesses)-1])
	page := history(t, h, "")
	require.Len(t, page.Records, 3)
	assert.Equal(t, hello, page.Records[0].Group, "a redacted record keeps its place")
	assert.True(t, page.Records[0].Redacted)
	assert.Empty(t, page.Records[0].Sender, "nor does it keep its sender")
	assert.Empty(t, page.Records[0].Payload)
	assert.Equal(t, `"world"`, string(page.Records[1].Payload))
	assert.Equal(t, hello, page.Records[2].Redacts, "the redaction is a record of the track")
	assert.Equal(t, "moderator", page.Records[2].Sender)
	assert.Equal(t, `{"sender":"moderator","redacts":"`+hello+`"}`, delivered[len(delivered)-1],
		"the redaction reaches OnRecord, so live subscribers learn of it")
}

func TestHandler_Redact_Twice(t *testing.T) {
	s := mem.New()
	h := newHandler(t, s, ingest.Options{})
	hello := posted(t, h, `"hello"`)
	require.Equal(t, http.StatusCreated, redact(h, hello))

	assert.Equal(t, http.StatusNoContent, redact(h, hello))
	assert.Len(t, history(t, h, "").Records, 2, "a second redaction commits nothing")
}

func TestHandler_Redact_Concurrent(t *testing.T) {
	h := newHandler(t, mem.New(), ingest.Options{})
	hello := posted(t, h, `"hello"`)

	const n = 8
	codes := make(chan int, n)
	var wg sync.WaitGroup
	for range n {
		wg.Go(func() { codes <- redact(h, hello) })
	}
	wg.Wait()
	close(codes)

	counts := map[int]int{}
	for code := range codes {
		counts[code]++
	}
	assert.Equal(t, map[int]int{http.StatusCreated: 1, http.StatusNoContent: n - 1}, counts)
	assert.Len(t, history(t, h, "").Records, 2, "one redaction, however many at once")
}

func TestHandler_Redact_Refused(t *testing.T) {
	h := newHandler(t, mem.New(), ingest.Options{})
	hello := posted(t, h, `"hello"`)
	redaction := func() string {
		rr := send(h, http.MethodDelete, chatURL+"?group="+hello, "")
		require.Equal(t, http.StatusCreated, rr.Code)
		var reply struct {
			Group string `json:"group"`
		}
		require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &reply))
		return reply.Group
	}()

	tests := map[string]struct {
		target string
		want   int
	}{
		"a redaction":           {target: chatURL + "?group=" + redaction, want: http.StatusBadRequest},
		"no group":              {target: chatURL, want: http.StatusBadRequest},
		"not a group":           {target: chatURL + "?group=yesterday", want: http.StatusBadRequest},
		"a group never written": {target: chatURL + "?group=e000001-g00000099", want: http.StatusNotFound},
		"a later epoch":         {target: chatURL + "?group=e000009-g00000000", want: http.StatusNotFound},
		"no such track":         {target: "tracks/room/9/chat?group=" + hello, want: http.StatusNotFound},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tt.want, send(h, http.MethodDelete, tt.target, "").Code)
		})
	}
}

func TestHandler_Redact_Unauthorized(t *testing.T) {
	errNotYours := errors.New("not yours")
	h := newHandler(t, mem.New(), ingest.Options{
		Authorize: func(_ *http.Request, _ ingest.Track, access ingest.Access) (string, error) {
			if access == ingest.Redact {
				return "", errNotYours
			}
			return "", nil
		},
	})
	hello := posted(t, h, `"hello"`)

	assert.Equal(t, http.StatusForbidden, redact(h, hello))
	assert.Equal(t, `"hello"`, string(history(t, h, "").Records[0].Payload))
}
