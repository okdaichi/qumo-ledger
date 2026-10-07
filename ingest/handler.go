package ingest

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/okdaichi/qumo-ledger/ledger"
	"github.com/okdaichi/qumo-ledger/ledger/store"
)

// DefaultMaxBodyBytes is the largest request body a [Handler] reads when
// [Options.MaxBodyBytes] is zero.
const DefaultMaxBodyBytes = 64 << 10

// DefaultIdleTimeout is how long a contribution lasts without a record when
// [Options.IdleTimeout] is zero.
const DefaultIdleTimeout = 5 * time.Minute

// Encoding and MIME of the tracks a [Handler] creates: every group is one JSON
// [Record].
const (
	Encoding = "json"
	MIME     = "application/json"
)

// timescale is the media timescale of the tracks a Handler creates.
const timescale = 1000

// ErrUnauthenticated is what [Options.Authorize] returns, or wraps, to refuse
// a request with 401 Unauthorized rather than 403 Forbidden.
var ErrUnauthenticated = errors.New("ingest: unauthenticated")

// Announcement is the body of an announce request: the track to record into
// and the contributor's name within it.
type Announcement struct {
	// BroadcastPath and TrackName together name the track.
	BroadcastPath string `json:"broadcast_path"`
	TrackName     string `json:"track_name"`

	// Name identifies the contributor within the track. It is one path
	// segment.
	Name string `json:"name"`
}

// Track returns the ledger track the announcement names.
func (a Announcement) Track() ledger.TrackPath {
	return trackPath(a.BroadcastPath, a.TrackName)
}

// trackPath joins a broadcast path and a track name into the single key a
// ledger track is stored under: the broadcast path without its leading slash,
// followed by the track name.
func trackPath(broadcastPath, trackName string) ledger.TrackPath {
	return ledger.TrackPath(strings.Trim(broadcastPath, "/") + "/" + trackName)
}

// Contribution is an announced contributor's standing to record into a track.
// Its ID addresses it in the URLs of its records.
type Contribution struct {
	ID string
	Announcement
}

// Record is what one group of an ingested track stores: the contributor and
// the payload it sent.
type Record struct {
	Name    string          `json:"name"`
	Payload json.RawMessage `json:"payload"`
}

// Announced describes a contribution an announce request started.
type Announced struct {
	Contribution

	// Created reports whether this request created the track.
	Created bool
}

// Recorded describes a committed record.
type Recorded struct {
	Contribution

	Group ledger.GroupInfo

	// Data is the committed group's content: the JSON encoding of a [Record].
	Data []byte
}

// Options configures a [Handler]. The zero value accepts every request.
type Options struct {
	// Authorize decides whether a request on behalf of an announcement may
	// proceed: the announce itself, and every record and end of the
	// contribution it starts. A non-nil error refuses the request with 403
	// Forbidden, or 401 Unauthorized when it is [ErrUnauthenticated]. Nil
	// allows every request.
	Authorize func(r *http.Request, a Announcement) error

	// OnAnnounce is called after an announce request started a contribution.
	OnAnnounce func(ctx context.Context, a Announced)

	// OnRecord is called after a record was committed, before the request is
	// answered. Records of one track are delivered in commit order.
	OnRecord func(ctx context.Context, rec Recorded)

	// IdleTimeout ends a contribution that has recorded nothing for this
	// long. Zero means [DefaultIdleTimeout].
	IdleTimeout time.Duration

	// MaxBodyBytes caps the request body. Zero means [DefaultMaxBodyBytes].
	MaxBodyBytes int64

	// Config is passed to the tracks the handler creates and opens.
	Config ledger.Config

	// Logger receives internal errors before they are answered with a generic
	// 500. Nil means no logging.
	Logger *slog.Logger
}

// Handler starts contributions and appends their records to the tracks of one
// store. It implements [http.Handler]; mount it under a prefix with
// [http.StripPrefix]:
//
//	mux.Handle("/ingest/", http.StripPrefix("/ingest", handler))
//
//	POST   /announce                    start a contribution: 201 with its Location
//	POST   /contributions/{id}/records  append the body, any JSON value, as one record
//	DELETE /contributions/{id}          end the contribution
//
// One contributor holds one contribution per track: announcing again ends the
// previous one, whose URLs then answer 410 Gone, as do those of a contribution
// that ended or went idle.
type Handler struct {
	store store.Store
	opts  Options

	maxBody int64
	idle    time.Duration
	logger  *slog.Logger
	mux     *http.ServeMux

	mu            sync.Mutex
	tracks        map[ledger.TrackPath]*trackWriter
	contributions map[string]*contribution
	current       map[contributor]*contribution
}

// trackWriter serializes the records of one track.
type trackWriter struct {
	mu     sync.Mutex
	writer *ledger.Writer
}

// contributor identifies a contributor within a track.
type contributor struct {
	track ledger.TrackPath
	name  string
}

// contribution is a Contribution as the handler tracks it. An ended one is
// kept for one idle timeout so that its URLs answer 410 rather than 404.
type contribution struct {
	Contribution
	track *trackWriter

	ended    bool
	deadline time.Time
	timer    *time.Timer
}

// NewHandler builds a [Handler] over s.
func NewHandler(s store.Store, opts Options) (*Handler, error) {
	if s == nil {
		return nil, errors.New("ingest: nil store")
	}
	h := &Handler{
		store:         s,
		opts:          opts,
		maxBody:       opts.MaxBodyBytes,
		idle:          opts.IdleTimeout,
		logger:        opts.Logger,
		mux:           http.NewServeMux(),
		tracks:        make(map[ledger.TrackPath]*trackWriter),
		contributions: make(map[string]*contribution),
		current:       make(map[contributor]*contribution),
	}
	if h.maxBody == 0 {
		h.maxBody = DefaultMaxBodyBytes
	}
	if h.idle == 0 {
		h.idle = DefaultIdleTimeout
	}
	if h.logger == nil {
		h.logger = slog.New(slog.DiscardHandler)
	}
	h.mux.HandleFunc("POST /announce", h.serveAnnounce)
	h.mux.HandleFunc("POST /contributions/{id}/records", h.serveRecord)
	h.mux.HandleFunc("DELETE /contributions/{id}", h.serveEnd)
	return h, nil
}

// ServeHTTP implements [http.Handler].
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mux.ServeHTTP(w, r)
}

// announceResponse is the body of a successful announce.
type announceResponse struct {
	ID    string           `json:"id"`
	Track ledger.TrackPath `json:"track"`
	// Created reports whether the announce created the track.
	Created bool `json:"created"`
}

// serveAnnounce starts a contribution to the announced track, creating the
// track when it does not exist.
func (h *Handler) serveAnnounce(w http.ResponseWriter, r *http.Request) {
	var a Announcement
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, h.maxBody)).Decode(&a); err != nil {
		badBody(w, err)
		return
	}
	if msg := validate(a); msg != "" {
		http.Error(w, msg, http.StatusBadRequest)
		return
	}
	if !h.authorize(w, r, a) {
		return
	}

	track := a.Track()
	created := true
	_, err := ledger.Create(r.Context(), h.store, track, ledger.TrackSchema{
		Timescale:  timescale,
		TimeSource: ledger.TimeSourceIngest,
		MIME:       MIME,
		Encoding:   Encoding,
	}, h.opts.Config)
	switch {
	case errors.Is(err, ledger.ErrTrackExists):
		created = false
	case errors.Is(err, ledger.ErrInvalidTrackPath):
		http.Error(w, "invalid track path", http.StatusBadRequest)
		return
	case err != nil:
		h.internalError(w, r, "create track", err)
		return
	}
	tw, err := h.writerFor(r.Context(), track)
	if err != nil {
		h.internalError(w, r, "open track writer", err)
		return
	}

	c := h.start(a, tw)
	if h.opts.OnAnnounce != nil {
		h.opts.OnAnnounce(r.Context(), Announced{Contribution: c, Created: created})
	}
	// A relative reference resolves against the announce URL, wherever the
	// handler is mounted.
	w.Header().Set("Location", "contributions/"+c.ID)
	writeJSON(w, http.StatusCreated, announceResponse{ID: c.ID, Track: track, Created: created})
}

// validate returns why an announcement is unusable, or "".
func validate(a Announcement) string {
	switch {
	case strings.Trim(a.BroadcastPath, "/") == "":
		return "broadcast_path is required"
	case a.TrackName == "" || strings.Contains(a.TrackName, "/"):
		return "track_name is required and must not contain a slash"
	case a.Name == "" || a.Name == "." || a.Name == ".." || strings.Contains(a.Name, "/"):
		return "name is required and must be one path segment"
	}
	return ""
}

// recordResponse is the body of a successful record.
type recordResponse struct {
	// Group is the committed group's ID in its text form.
	Group string `json:"group"`
	// Wallclock is when the record was committed, in Unix nanoseconds.
	Wallclock int64 `json:"wallclock"`
}

// serveRecord appends the body to the contribution's track as one group and
// answers once it is committed. The contribution is resolved and authorized
// before the body is read.
func (h *Handler) serveRecord(w http.ResponseWriter, r *http.Request) {
	c, ok := h.lookup(w, r)
	if !ok || !h.authorize(w, r, c.Announcement) {
		return
	}
	payload, err := io.ReadAll(http.MaxBytesReader(w, r.Body, h.maxBody))
	if err != nil {
		badBody(w, err)
		return
	}
	if !json.Valid(payload) {
		http.Error(w, "the body must be one JSON value", http.StatusBadRequest)
		return
	}
	data, err := json.Marshal(Record{Name: c.Name, Payload: payload})
	if err != nil {
		h.internalError(w, r, "encode record", err)
		return
	}

	// Committing and notifying under one lock delivers a track's records to
	// OnRecord in commit order.
	c.track.mu.Lock()
	group, err := c.track.writer.Append(r.Context(), 0, data)
	if err == nil && h.opts.OnRecord != nil {
		h.opts.OnRecord(r.Context(), Recorded{Contribution: c.Contribution, Group: group, Data: data})
	}
	c.track.mu.Unlock()
	if err != nil {
		h.internalError(w, r, "append record", err)
		return
	}

	writeJSON(w, http.StatusCreated, recordResponse{
		Group:     group.ID.String(),
		Wallclock: group.Wallclock,
	})
}

// serveEnd ends a contribution.
func (h *Handler) serveEnd(w http.ResponseWriter, r *http.Request) {
	c, ok := h.lookup(w, r)
	if !ok || !h.authorize(w, r, c.Announcement) {
		return
	}
	h.mu.Lock()
	h.end(c)
	h.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

// authorize applies Options.Authorize, answering the request when it refuses.
func (h *Handler) authorize(w http.ResponseWriter, r *http.Request, a Announcement) bool {
	if h.opts.Authorize == nil {
		return true
	}
	err := h.opts.Authorize(r, a)
	switch {
	case err == nil:
		return true
	case errors.Is(err, ErrUnauthenticated):
		http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
	default:
		http.Error(w, http.StatusText(http.StatusForbidden), http.StatusForbidden)
	}
	return false
}

// start registers a new contribution, ending the contributor's previous one in
// the same track.
func (h *Handler) start(a Announcement, tw *trackWriter) Contribution {
	c := &contribution{
		Contribution: Contribution{ID: rand.Text(), Announcement: a},
		track:        tw,
		deadline:     time.Now().Add(h.idle),
	}
	key := contributor{track: a.Track(), name: a.Name}

	h.mu.Lock()
	defer h.mu.Unlock()
	if previous, ok := h.current[key]; ok {
		h.end(previous)
	}
	h.contributions[c.ID] = c
	h.current[key] = c
	c.timer = time.AfterFunc(h.idle, func() { h.expire(c) })
	return c.Contribution
}

// lookup resolves the request's contribution and extends it, answering 404 for
// an unknown one and 410 for one that ended.
func (h *Handler) lookup(w http.ResponseWriter, r *http.Request) (*contribution, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	c, ok := h.contributions[r.PathValue("id")]
	switch {
	case !ok:
		http.Error(w, "unknown contribution", http.StatusNotFound)
		return nil, false
	case c.ended:
		http.Error(w, "the contribution has ended", http.StatusGone)
		return nil, false
	}
	c.deadline = time.Now().Add(h.idle)
	return c, true
}

// end ends a contribution and keeps it for one idle timeout. h.mu is held.
func (h *Handler) end(c *contribution) {
	if c.ended {
		return
	}
	c.ended = true
	key := contributor{track: c.Track(), name: c.Name}
	if h.current[key] == c {
		delete(h.current, key)
	}
	c.deadline = time.Now().Add(h.idle)
	c.timer.Reset(h.idle)
}

// expire runs when a contribution's timer fires: it waits again while the
// deadline is ahead, then ends a live contribution or forgets an ended one.
func (h *Handler) expire(c *contribution) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if wait := time.Until(c.deadline); wait > 0 {
		c.timer.Reset(wait)
		return
	}
	if !c.ended {
		h.end(c)
		return
	}
	delete(h.contributions, c.ID)
}

// writerFor returns the track's writer, opening it on first use.
func (h *Handler) writerFor(ctx context.Context, track ledger.TrackPath) (*trackWriter, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if tw, ok := h.tracks[track]; ok {
		return tw, nil
	}
	t, err := ledger.Open(ctx, h.store, track, h.opts.Config)
	if err != nil {
		return nil, err
	}
	writer, err := t.Writer(ctx)
	if err != nil {
		return nil, fmt.Errorf("ingest: open writer of %s: %w", track, err)
	}
	tw := &trackWriter{writer: writer}
	h.tracks[track] = tw
	return tw, nil
}

// badBody answers a request whose body could not be read or decoded.
func badBody(w http.ResponseWriter, err error) {
	if _, tooLarge := errors.AsType[*http.MaxBytesError](err); tooLarge {
		http.Error(w, http.StatusText(http.StatusRequestEntityTooLarge), http.StatusRequestEntityTooLarge)
		return
	}
	http.Error(w, "invalid request body", http.StatusBadRequest)
}

// internalError answers a generic 500 and logs what failed. A canceled context
// is the client leaving and is not logged.
func (h *Handler) internalError(w http.ResponseWriter, r *http.Request, op string, err error) {
	if !errors.Is(err, context.Canceled) {
		h.logger.Error(op, "method", r.Method, "url", r.URL.Path, "error", err)
	}
	http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
