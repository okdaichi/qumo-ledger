package ingest

import (
	"context"
	"net/http"
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
		tr.lastUsed = time.Now().Add(-2 * idle)

		h.mu.Lock()
		h.closeIdle(time.Now())
		_, open := h.tracks[tr.Path()]
		h.mu.Unlock()

		assert.True(t, open, "the sweep waits half a timeout after the last")
	})
}

func TestNewHandler_TrackIdleTimeoutDefault(t *testing.T) {
	h, err := NewHandler(mem.New(), Options{})
	require.NoError(t, err)

	assert.Equal(t, DefaultTrackIdleTimeout, h.idle)
}
