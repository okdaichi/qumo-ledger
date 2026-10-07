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
	"path"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

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
// value, the payload of one record.
const (
	Encoding = "json"
	MIME     = "application/json"
)

// timescale is the media timescale of the tracks a Handler creates.
const timescale = 1000

// Idempotency keys: a contribution remembers the replies to this many of its
// most recent keyed records, and a key is at most this long.
const (
	idempotencyKeys      = 256
	maxIdempotencyKeyLen = 255
)

// ErrUnauthenticated is what [Options.Authorize] returns, or wraps, to refuse
// a request with 401 Unauthorized rather than 403 Forbidden.
var ErrUnauthenticated = errors.New("ingest: unauthenticated")

// Track names a track by its broadcast path and track name. It is the body of
// an announce request.
type Track struct {
	BroadcastPath string `json:"broadcast_path"`
	TrackName     string `json:"track_name"`
}

// Path returns the single key the ledger stores the track under: the broadcast
// path without its leading slash, followed by the track name.
func (t Track) Path() ledger.TrackPath {
	return ledger.TrackPath(strings.Trim(t.BroadcastPath, "/") + "/" + t.TrackName)
}

// Options configures a [Handler]. The zero value accepts every request.
type Options struct {
	// Authorize decides whether a request on a track may proceed: an
	// announce, and every record and end of the contribution it starts. A
	// non-nil error refuses the request with 403 Forbidden, or 401
	// Unauthorized when it is [ErrUnauthenticated]. Nil allows every request.
	Authorize func(r *http.Request, t Track) error

	// Challenge is the WWW-Authenticate header sent with a 401, naming the
	// scheme Authorize expects, such as "Bearer". Empty sends none.
	Challenge string

	// OnAnnounce is called after an announce started a contribution to t.
	OnAnnounce func(ctx context.Context, t Track)

	// OnRecord is called after payload was committed to t as group g, before
	// the request is answered. Records of one track are delivered in commit
	// order.
	OnRecord func(ctx context.Context, t Track, g ledger.GroupInfo, payload []byte)

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
//	POST   /contributions/{id}/records  append the body, one JSON value, as one record
//	DELETE /contributions/{id}          end the contribution
//
// Any number of contributions record into one track. A contribution ends when
// it is deleted or has recorded nothing for [Options.IdleTimeout]; its URLs
// then answer 410 Gone.
//
// A record sent with an Idempotency-Key header is stored once per
// contribution: a retry with the same key is answered as the first was.
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
}

// trackWriter serializes the records of one track.
type trackWriter struct {
	mu     sync.Mutex
	writer *ledger.Writer
}

// contribution is one announce's standing to record into a track. An ended one
// is kept for one idle timeout so that its URLs answer 410 rather than 404.
type contribution struct {
	id    string
	track Track
	tw    *trackWriter

	// replies holds the answers to keyed records, oldest key first in keys.
	// Guarded by tw.mu.
	replies map[string]recordResponse
	keys    []string

	// Guarded by Handler.mu.
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
}

// serveAnnounce starts a contribution to the announced track, creating the
// track when it does not exist.
func (h *Handler) serveAnnounce(w http.ResponseWriter, r *http.Request) {
	var t Track
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, h.maxBody)).Decode(&t); err != nil {
		badBody(w, err)
		return
	}
	// One track has one broadcast path: "room/123" and "/room/123/" are
	// "/room/123", so its contributors and hooks agree on the path.
	t.BroadcastPath = "/" + strings.Trim(t.BroadcastPath, "/")
	if msg := validate(t); msg != "" {
		http.Error(w, msg, http.StatusBadRequest)
		return
	}
	if !h.authorize(w, r, t) {
		return
	}

	_, err := ledger.Create(r.Context(), h.store, t.Path(), ledger.TrackSchema{
		Timescale:  timescale,
		TimeSource: ledger.TimeSourceIngest,
		MIME:       MIME,
		Encoding:   Encoding,
	}, h.opts.Config)
	switch {
	case errors.Is(err, ledger.ErrTrackExists):
	case errors.Is(err, ledger.ErrInvalidTrackPath):
		http.Error(w, "invalid track path", http.StatusBadRequest)
		return
	case err != nil:
		h.internalError(w, r, "create track", err)
		return
	}
	tw, err := h.writerFor(r.Context(), t.Path())
	if err != nil {
		h.internalError(w, r, "open track writer", err)
		return
	}

	id := h.start(t, tw)
	if h.opts.OnAnnounce != nil {
		h.opts.OnAnnounce(r.Context(), t)
	}
	// A relative reference resolves against the announce URL, wherever the
	// handler is mounted.
	w.Header().Set("Location", "contributions/"+id)
	writeJSON(w, http.StatusCreated, announceResponse{ID: id, Track: t.Path()})
}

// validate returns why a track name is unusable, or "".
func validate(t Track) string {
	switch {
	case t.BroadcastPath == "/":
		return "broadcast_path is required"
	case path.Clean(t.BroadcastPath) != t.BroadcastPath:
		return "broadcast_path must be clean: no empty, \".\" or \"..\" segments"
	case t.TrackName == "" || strings.Contains(t.TrackName, "/"):
		return "track_name is required and must not contain a slash"
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
	if !ok || !h.authorize(w, r, c.track) {
		return
	}
	key := r.Header.Get("Idempotency-Key")
	if len(key) > maxIdempotencyKeyLen {
		http.Error(w, "Idempotency-Key is too long", http.StatusBadRequest)
		return
	}
	payload, err := io.ReadAll(http.MaxBytesReader(w, r.Body, h.maxBody))
	if err != nil {
		badBody(w, err)
		return
	}
	// encoding/json accepts invalid UTF-8 inside strings; JSON text is UTF-8,
	// and a record is forwarded as it is stored.
	if !json.Valid(payload) || !utf8.Valid(payload) {
		http.Error(w, "the body must be one JSON value in UTF-8", http.StatusBadRequest)
		return
	}

	// Committing and notifying under one lock delivers a track's records to
	// OnRecord in commit order, and lets a retry find the reply to its key.
	c.tw.mu.Lock()
	if reply, seen := c.replies[key]; seen && key != "" {
		c.tw.mu.Unlock()
		writeJSON(w, http.StatusCreated, reply)
		return
	}
	// The append outlives a client that leaves mid-request: an append cut
	// short between storing the group and committing it leaves the group's
	// object behind, and the next append of the track would collide with it.
	group, err := c.tw.writer.Append(context.WithoutCancel(r.Context()), 0, payload)
	// An append that committed but could not seal returns the group with its
	// error: the record is stored, so it is delivered and answered as such.
	committed := group.ObjectKey != ""
	reply := recordResponse{Group: group.ID.String(), Wallclock: group.Wallclock}
	if committed {
		if key != "" {
			c.remember(key, reply)
		}
		if h.opts.OnRecord != nil {
			h.opts.OnRecord(r.Context(), c.track, group, payload)
		}
	}
	c.tw.mu.Unlock()
	switch {
	case !committed:
		h.internalError(w, r, "append record", err)
		return
	case err != nil:
		h.logger.Warn("record committed with an error", "method", r.Method, "url", r.URL.Path, "error", err)
	}
	writeJSON(w, http.StatusCreated, reply)
}

// remember keeps the reply to a keyed record, forgetting the oldest key past
// the limit. c.tw.mu is held.
func (c *contribution) remember(key string, reply recordResponse) {
	if c.replies == nil {
		c.replies = make(map[string]recordResponse)
	}
	if len(c.keys) == idempotencyKeys {
		delete(c.replies, c.keys[0])
		c.keys = c.keys[1:]
	}
	c.replies[key] = reply
	c.keys = append(c.keys, key)
}

// serveEnd ends a contribution.
func (h *Handler) serveEnd(w http.ResponseWriter, r *http.Request) {
	c, ok := h.lookup(w, r)
	if !ok || !h.authorize(w, r, c.track) {
		return
	}
	h.mu.Lock()
	h.end(c)
	h.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

// authorize applies Options.Authorize, answering the request when it refuses.
func (h *Handler) authorize(w http.ResponseWriter, r *http.Request, t Track) bool {
	if h.opts.Authorize == nil {
		return true
	}
	err := h.opts.Authorize(r, t)
	switch {
	case err == nil:
		return true
	case errors.Is(err, ErrUnauthenticated):
		if h.opts.Challenge != "" {
			w.Header().Set("WWW-Authenticate", h.opts.Challenge)
		}
		http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
	default:
		http.Error(w, http.StatusText(http.StatusForbidden), http.StatusForbidden)
	}
	return false
}

// start registers a new contribution to t and returns its ID.
func (h *Handler) start(t Track, tw *trackWriter) string {
	c := &contribution{
		id:       rand.Text(),
		track:    t,
		tw:       tw,
		deadline: time.Now().Add(h.idle),
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.contributions[c.id] = c
	c.timer = time.AfterFunc(h.idle, func() { h.expire(c) })
	return c.id
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
	delete(h.contributions, c.id)
}

// writerFor returns the track's writer, opening it on first use. Opening reads
// the store, so it runs without h.mu; when two requests open the same track at
// once, the first to finish is kept.
func (h *Handler) writerFor(ctx context.Context, track ledger.TrackPath) (*trackWriter, error) {
	h.mu.Lock()
	tw, ok := h.tracks[track]
	h.mu.Unlock()
	if ok {
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
	h.mu.Lock()
	defer h.mu.Unlock()
	if tw, ok := h.tracks[track]; ok {
		return tw, nil
	}
	tw = &trackWriter{writer: writer}
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
	// not actionable: the status is sent, and a client that stopped reading
	// cannot be answered differently.
	_ = json.NewEncoder(w).Encode(v)
}
