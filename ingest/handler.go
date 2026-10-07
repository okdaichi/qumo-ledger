package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"path"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/okdaichi/qumo-ledger/ledger"
	"github.com/okdaichi/qumo-ledger/ledger/store"
)

// DefaultMaxBodyBytes is the largest request body a [Handler] reads when
// [Options.MaxBodyBytes] is zero.
const DefaultMaxBodyBytes = 64 << 10

// Encoding and MIME of the tracks a [Handler] creates: every group is one JSON
// value, the payload of one record.
const (
	Encoding = "json"
	MIME     = "application/json"
)

// timescale is the media timescale of the tracks a Handler creates.
const timescale = 1000

// Idempotency keys: a track remembers the replies to this many of its most
// recent keyed records, and a key is at most this long.
const (
	idempotencyKeys      = 1024
	maxIdempotencyKeyLen = 255
)

// ErrUnauthenticated is what [Options.Authorize] returns, or wraps, to refuse
// a request with 401 Unauthorized rather than 403 Forbidden.
var ErrUnauthenticated = errors.New("ingest: unauthenticated")

// Track names a track by its broadcast path and track name.
type Track struct {
	BroadcastPath string
	TrackName     string
}

// Path returns the single key the ledger stores the track under: the broadcast
// path without its leading slash, followed by the track name.
func (t Track) Path() ledger.TrackPath {
	return ledger.TrackPath(strings.Trim(t.BroadcastPath, "/") + "/" + t.TrackName)
}

// trackFromPath reads a track from its ledger key: every segment but the last
// is the broadcast path, and the last is the track name.
func trackFromPath(p string) (Track, error) {
	if p == "" || path.Clean("/"+p) != "/"+p {
		return Track{}, errors.New("the track must be a clean path: no empty, \".\" or \"..\" segments")
	}
	i := strings.LastIndexByte(p, '/')
	if i < 0 {
		return Track{}, errors.New("the track needs a broadcast path and a track name, as room/123/chat")
	}
	return Track{BroadcastPath: "/" + p[:i], TrackName: p[i+1:]}, nil
}

// Options configures a [Handler]. The zero value accepts every request.
type Options struct {
	// Authorize decides whether a request on a track may proceed. A non-nil
	// error refuses it with 403 Forbidden, or 401 Unauthorized when it is
	// [ErrUnauthenticated]. Nil allows every request.
	Authorize func(r *http.Request, t Track) error

	// Challenge is the WWW-Authenticate header sent with a 401, naming the
	// scheme Authorize expects, such as "Bearer". Empty sends none.
	Challenge string

	// OnOpen is called once per track, when the handler first opens it for
	// writing, before any of its records reach OnRecord.
	OnOpen func(ctx context.Context, t Track)

	// OnRecord is called after payload was committed to t as group g, before
	// the request is answered. Records of one track are delivered in commit
	// order.
	OnRecord func(ctx context.Context, t Track, g ledger.GroupInfo, payload []byte)

	// MaxBodyBytes caps the request body. Zero means [DefaultMaxBodyBytes].
	MaxBodyBytes int64

	// Config is passed to the tracks the handler creates and opens.
	Config ledger.Config

	// Logger receives internal errors before they are answered with a generic
	// 500. Nil means no logging.
	Logger *slog.Logger
}

// Handler appends records to the tracks of one store, addressed by the URL. It
// implements [http.Handler]; mount it under a prefix with [http.StripPrefix]:
//
//	mux.Handle("/ingest/", http.StripPrefix("/ingest", handler))
//
//	PUT  /tracks/{broadcast path}/{track name}  create the track when it does not exist
//	POST /tracks/{broadcast path}/{track name}  append the body, one JSON value, as one record
//
// A record creates its track too; PUT is for a track that should exist before
// its first record. A record sent with an Idempotency-Key header is stored once
// per track: a retry with the same key is answered as the first was.
type Handler struct {
	store store.Store
	opts  Options

	maxBody int64
	logger  *slog.Logger
	mux     *http.ServeMux

	mu     sync.Mutex
	tracks map[ledger.TrackPath]*track
}

// track is one track the handler writes to.
type track struct {
	Track

	// mu serializes the track's records and guards replies and keys.
	mu     sync.Mutex
	writer *ledger.Writer
	// replies holds the answers to keyed records, oldest key first in keys.
	replies map[string]recordResponse
	keys    []string
}

// NewHandler builds a [Handler] over s.
func NewHandler(s store.Store, opts Options) (*Handler, error) {
	if s == nil {
		return nil, errors.New("ingest: nil store")
	}
	h := &Handler{
		store:   s,
		opts:    opts,
		maxBody: opts.MaxBodyBytes,
		logger:  opts.Logger,
		mux:     http.NewServeMux(),
		tracks:  make(map[ledger.TrackPath]*track),
	}
	if h.maxBody == 0 {
		h.maxBody = DefaultMaxBodyBytes
	}
	if h.logger == nil {
		h.logger = slog.New(slog.DiscardHandler)
	}
	h.mux.HandleFunc("PUT /tracks/{track...}", h.serveCreate)
	h.mux.HandleFunc("POST /tracks/{track...}", h.serveRecord)
	return h, nil
}

// ServeHTTP implements [http.Handler].
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mux.ServeHTTP(w, r)
}

// serveCreate creates the request's track when it does not exist: 201 when it
// did not, 204 when it did.
func (h *Handler) serveCreate(w http.ResponseWriter, r *http.Request) {
	t, ok := h.resolve(w, r)
	if !ok {
		return
	}
	_, created, err := h.open(r.Context(), t)
	if err != nil {
		h.internalError(w, r, "open track", err)
		return
	}
	if created {
		w.WriteHeader(http.StatusCreated)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// recordResponse is the body of a successful record.
type recordResponse struct {
	// Group is the committed group's ID in its text form.
	Group string `json:"group"`
	// Wallclock is when the record was committed, in Unix nanoseconds.
	Wallclock int64 `json:"wallclock"`
}

// serveRecord appends the body to the request's track as one group and answers
// once it is committed. The track is resolved and authorized before the body
// is read.
func (h *Handler) serveRecord(w http.ResponseWriter, r *http.Request) {
	t, ok := h.resolve(w, r)
	if !ok {
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
	tr, _, err := h.open(r.Context(), t)
	if err != nil {
		h.internalError(w, r, "open track", err)
		return
	}

	// Committing and notifying under one lock delivers a track's records to
	// OnRecord in commit order, and lets a retry find the reply to its key.
	tr.mu.Lock()
	if reply, seen := tr.replies[key]; seen && key != "" {
		tr.mu.Unlock()
		writeJSON(w, http.StatusCreated, reply)
		return
	}
	// The append outlives a client that leaves mid-request: an append cut
	// short between storing the group and committing it leaves the group's
	// object behind, and the next append of the track would collide with it.
	group, err := tr.writer.Append(context.WithoutCancel(r.Context()), 0, payload)
	// An append that committed but could not seal returns the group with its
	// error: the record is stored, so it is delivered and answered as such.
	committed := group.ObjectKey != ""
	reply := recordResponse{Group: group.ID.String(), Wallclock: group.Wallclock}
	if committed {
		if key != "" {
			tr.remember(key, reply)
		}
		if h.opts.OnRecord != nil {
			h.opts.OnRecord(r.Context(), tr.Track, group, payload)
		}
	}
	tr.mu.Unlock()
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
// the limit. tr.mu is held.
func (tr *track) remember(key string, reply recordResponse) {
	if tr.replies == nil {
		tr.replies = make(map[string]recordResponse)
	}
	if len(tr.keys) == idempotencyKeys {
		delete(tr.replies, tr.keys[0])
		tr.keys = tr.keys[1:]
	}
	tr.replies[key] = reply
	tr.keys = append(tr.keys, key)
}

// resolve reads the request's track and authorizes the request on it,
// answering the request when either fails.
func (h *Handler) resolve(w http.ResponseWriter, r *http.Request) (Track, bool) {
	t, err := trackFromPath(r.PathValue("track"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return Track{}, false
	}
	if h.opts.Authorize == nil {
		return t, true
	}
	err = h.opts.Authorize(r, t)
	switch {
	case err == nil:
		return t, true
	case errors.Is(err, ErrUnauthenticated):
		if h.opts.Challenge != "" {
			w.Header().Set("WWW-Authenticate", h.opts.Challenge)
		}
		http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
	default:
		http.Error(w, http.StatusText(http.StatusForbidden), http.StatusForbidden)
	}
	return Track{}, false
}

// open returns the track's writer, creating the track when it does not exist
// and reporting whether it did. Opening reads the store, so it runs without
// h.mu; when two requests open the same track at once, the first to finish is
// kept.
func (h *Handler) open(ctx context.Context, t Track) (*track, bool, error) {
	key := t.Path()
	h.mu.Lock()
	tr, ok := h.tracks[key]
	h.mu.Unlock()
	if ok {
		return tr, false, nil
	}

	created := true
	_, err := ledger.Create(ctx, h.store, key, ledger.TrackSchema{
		Timescale:  timescale,
		TimeSource: ledger.TimeSourceIngest,
		MIME:       MIME,
		Encoding:   Encoding,
	}, h.opts.Config)
	switch {
	case errors.Is(err, ledger.ErrTrackExists):
		created = false
	case err != nil:
		return nil, false, err
	}
	lt, err := ledger.Open(ctx, h.store, key, h.opts.Config)
	if err != nil {
		return nil, false, err
	}
	writer, err := lt.Writer(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("ingest: open writer of %s: %w", key, err)
	}

	h.mu.Lock()
	if tr, ok := h.tracks[key]; ok {
		h.mu.Unlock()
		return tr, false, nil
	}
	tr = &track{Track: t, writer: writer}
	h.tracks[key] = tr
	h.mu.Unlock()
	if h.opts.OnOpen != nil {
		h.opts.OnOpen(ctx, t)
	}
	return tr, created, nil
}

// badBody answers a request whose body could not be read.
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
