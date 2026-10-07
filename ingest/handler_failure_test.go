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

// serve sends one request to h and returns the response.
func serve(h http.Handler, method, target, body string) *httptest.ResponseRecorder {
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(method, target, strings.NewReader(body)))
	return rr
}

func TestHandler_StoreFailures(t *testing.T) {
	errDown := errors.New("store is down")
	tests := map[string]struct {
		createErr  map[string]error
		wantCreate int
		wantRecord int
		wantLogged string
	}{
		"the track cannot be created": {
			createErr:  map[string]error{"root.manifest": errDown},
			wantCreate: http.StatusInternalServerError,
			wantRecord: http.StatusInternalServerError,
			wantLogged: "open track",
		},
		"record cannot store its group": {
			createErr:  map[string]error{"/groups/": errDown},
			wantCreate: http.StatusCreated,
			wantRecord: http.StatusInternalServerError,
			wantLogged: "append record",
		},
		"record committed but the seal failed": {
			createErr:  map[string]error{"/sealed": errDown},
			wantCreate: http.StatusCreated,
			wantRecord: http.StatusCreated,
			wantLogged: "record committed with an error",
		},
		"a canceled store call is not logged": {
			createErr:  map[string]error{"/groups/": context.Canceled},
			wantCreate: http.StatusCreated,
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

			create := serve(h, http.MethodPut, "/tracks/room/123/chat", "")
			rr := serve(h, http.MethodPost, "/tracks/room/123/chat", `"hello"`)

			assert.Equal(t, tt.wantCreate, create.Code)
			assert.Equal(t, tt.wantRecord, rr.Code)
			assert.NotContains(t, rr.Body.String(), errDown.Error(), "the cause is logged, not answered")
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
