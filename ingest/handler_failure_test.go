package ingest

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/okdaichi/qumo-ledger/ledger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const chatAnnouncement = `{"broadcast_path":"/room/123","track_name":"chat"}`

// serve sends one request to h and returns the response.
func serve(h http.Handler, method, target, body string) *httptest.ResponseRecorder {
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(method, target, strings.NewReader(body)))
	return rr
}

func TestHandler_StoreFailures(t *testing.T) {
	errDown := errors.New("store is down")
	tests := map[string]struct {
		createErr    map[string]error
		failAnnounce bool
		wantRecord   int
		wantLogged   string
	}{
		"announce cannot create the track": {
			createErr:    map[string]error{"root.manifest": errDown},
			failAnnounce: true,
			wantLogged:   "create track",
		},
		"record cannot store its group": {
			createErr:  map[string]error{"/groups/": errDown},
			wantRecord: http.StatusInternalServerError,
			wantLogged: "append record",
		},
		"record committed but the seal failed": {
			createErr:  map[string]error{"/sealed": errDown},
			wantRecord: http.StatusCreated,
			wantLogged: "record committed with an error",
		},
		"a canceled store call is not logged": {
			createErr:  map[string]error{"/groups/": context.Canceled},
			wantRecord: http.StatusInternalServerError,
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			var logs bytes.Buffer
			var delivered []string
			h, err := NewHandler(&fakeStore{createErr: tt.createErr}, Options{
				// A threshold of one byte seals after every record.
				Config: ledger.Config{SealThreshold: 1},
				Logger: slog.New(slog.NewTextHandler(&logs, nil)),
				OnRecord: func(_ context.Context, _ Track, _ ledger.GroupInfo, payload []byte) {
					delivered = append(delivered, string(payload))
				},
			})
			require.NoError(t, err)

			announce := serve(h, http.MethodPost, "/announce", chatAnnouncement)
			if tt.failAnnounce {
				assert.Equal(t, http.StatusInternalServerError, announce.Code)
				assert.Contains(t, logs.String(), tt.wantLogged)
				assert.NotContains(t, announce.Body.String(), errDown.Error(), "the cause is logged, not answered")
				return
			}
			require.Equal(t, http.StatusCreated, announce.Code, announce.Body.String())

			rr := serve(h, http.MethodPost, "/"+announce.Header().Get("Location")+"/records", `"hello"`)

			assert.Equal(t, tt.wantRecord, rr.Code)
			if tt.wantLogged == "" {
				assert.Empty(t, logs.String())
			} else {
				assert.Contains(t, logs.String(), tt.wantLogged)
			}
			if tt.wantRecord == http.StatusCreated {
				require.Len(t, delivered, 1, "a committed record is delivered even when the seal failed")
				assert.Equal(t, `"hello"`, delivered[0])
			} else {
				assert.Empty(t, delivered, "a record that was not committed is not delivered")
			}
		})
	}
}
