package ingest

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/okdaichi/qumo-ledger/ledger"
	"github.com/okdaichi/qumo-ledger/ledger/store/mem"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_IdleTracksAreClosedAndReopened(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const idle = time.Minute
		var opened int
		var sequences []uint64
		h, err := NewHandler(mem.New(), Options{
			TrackIdleTimeout: idle,
			OnOpen:           func(context.Context, Track) { opened++ },
			OnRecord: func(_ context.Context, _ Track, g ledger.GroupInfo, _ []byte) {
				sequences = append(sequences, g.ID.Sequence())
			},
		})
		require.NoError(t, err)

		require.Equal(t, http.StatusCreated, serve(h, http.MethodPost, "/tracks/room/1/chat", `"first"`).Code)
		time.Sleep(idle / 2)
		require.Equal(t, http.StatusCreated, serve(h, http.MethodPost, "/tracks/room/1/chat", `"still open"`).Code)
		assert.Equal(t, 1, opened, "a track in use stays open")

		time.Sleep(2 * idle)
		require.Equal(t, http.StatusCreated, serve(h, http.MethodPost, "/tracks/room/1/chat", `"after a while"`).Code)

		assert.Equal(t, 2, opened, "an idle track is closed and opened again on its next use")
		assert.Equal(t, []uint64{0, 1, 2}, sequences, "a reopened track continues where it left off")
	})
}

func TestHandler_CloseIdle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const idle = time.Minute
		h, err := NewHandler(mem.New(), Options{TrackIdleTimeout: idle})
		require.NoError(t, err)
		held, _, err := h.open(t.Context(), Track{BroadcastPath: "/room/1", TrackName: "chat"})
		require.NoError(t, err)
		quiet, _, err := h.open(t.Context(), Track{BroadcastPath: "/room/2", TrackName: "chat"})
		require.NoError(t, err)
		h.release(quiet)

		time.Sleep(2 * idle)
		h.mu.Lock()
		h.closeIdle(time.Now())
		_, heldOpen := h.tracks[held.Path()]
		_, quietOpen := h.tracks[quiet.Path()]
		h.mu.Unlock()

		assert.True(t, heldOpen, "a track a request holds is never closed")
		assert.False(t, quietOpen)

		h.release(held)
		time.Sleep(2 * idle)
		h.mu.Lock()
		h.closeIdle(time.Now())
		_, heldOpen = h.tracks[held.Path()]
		h.mu.Unlock()
		assert.False(t, heldOpen, "once let go and idle, it is closed")
	})
}

func TestHandler_CloseIdle_AtMostEveryHalfTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const idle = time.Minute
		h, err := NewHandler(mem.New(), Options{TrackIdleTimeout: idle})
		require.NoError(t, err)
		h.mu.Lock()
		h.closeIdle(time.Now())
		h.mu.Unlock()
		tr, _, err := h.open(t.Context(), Track{BroadcastPath: "/room/1", TrackName: "chat"})
		require.NoError(t, err)
		h.release(tr)

		h.mu.Lock()
		tr.lastUsed = time.Now().Add(-2 * idle)
		h.closeIdle(time.Now())
		_, open := h.tracks[tr.Path()]
		h.mu.Unlock()

		assert.True(t, open, "the sweep waits half a timeout after the last")
	})
}

func TestHandler_IdleTracksAreClosedByReads(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const idle = time.Minute
		h, err := NewHandler(mem.New(), Options{TrackIdleTimeout: idle})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, serve(h, http.MethodPost, "/tracks/room/1/chat", `"first"`).Code)

		time.Sleep(2 * idle)
		require.Equal(t, http.StatusOK, serve(h, http.MethodGet, "/tracks/room/1/chat", "").Code)

		h.mu.Lock()
		defer h.mu.Unlock()
		assert.Empty(t, h.tracks, "a handler that is only read from still closes idle tracks")
	})
}

func TestHandler_OnOpenComesBeforeTheFirstRecord(t *testing.T) {
	var mu sync.Mutex
	var events []string
	logEvent := func(e string) {
		mu.Lock()
		defer mu.Unlock()
		events = append(events, e)
	}
	opening := make(chan struct{})
	h, err := NewHandler(mem.New(), Options{
		OnOpen: func(context.Context, Track) {
			<-opening
			logEvent("open")
		},
		OnRecord: func(context.Context, Track, ledger.GroupInfo, []byte) { logEvent("record") },
	})
	require.NoError(t, err)
	tr := Track{BroadcastPath: "/room/1", TrackName: "chat"}

	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() { serve(h, http.MethodPost, "/tracks/room/1/chat", `"hello"`) })
	}
	// Both requests hold the track while the first is still in OnOpen.
	require.Eventually(t, func() bool {
		h.mu.Lock()
		defer h.mu.Unlock()
		opened, ok := h.tracks[tr.Path()]
		return ok && opened.users == 2
	}, time.Second, time.Millisecond)
	assert.Never(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(events) > 0
	}, 50*time.Millisecond, time.Millisecond, "no record is delivered before OnOpen returns")
	close(opening)
	wg.Wait()

	assert.Equal(t, []string{"open", "record", "record"}, events)
}

func TestNewHandler_TrackIdleTimeout(t *testing.T) {
	tests := map[string]struct {
		timeout time.Duration
		want    time.Duration
		wantErr bool
	}{
		"zero is the default": {timeout: 0, want: DefaultTrackIdleTimeout},
		"positive":            {timeout: time.Second, want: time.Second},
		"negative":            {timeout: -time.Second, wantErr: true},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			h, err := NewHandler(mem.New(), Options{TrackIdleTimeout: tt.timeout})
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, h.idle)
		})
	}
}
