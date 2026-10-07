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
	"strconv"
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

// DefaultTrackIdleTimeout is how long a [Handler] keeps a track it has not
// written to when [Options.TrackIdleTimeout] is zero.
const DefaultTrackIdleTimeout = 10 * time.Minute

// Encoding and MIME of the tracks a [Handler] creates: every group is one JSON
// [Record].
const (
	Encoding = "json"
	MIME     = "application/json"
)

// History pages: how many records a GET returns when it asks for none, and at
// most.
const (
	DefaultPageSize = 50
	MaxPageSize     = 200
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

// Record is one group of an ingested track: the payload a sender posted, and
// the sender as [Options.Authorize] named it. A record with no sender was
// written by a party trusted with the whole track.
type Record struct {
	Sender  string          `json:"sender,omitempty"`
	Payload json.RawMessage `json:"payload"`
}

// Access is what a request does to a track.
type Access int

const (
	// Write creates a track or records into it.
	Write Access = iota
	// Read reads a track's history.
	Read
)

// Limit bounds how fast records arrive: Rate per second on average, and up to
// Burst at once. The zero Limit is no limit.
type Limit struct {
	Rate  float64
	Burst int
}

// Options configures a [Handler]. The zero value accepts every request.
type Options struct {
	// Authorize decides whether a request may have access to a track, and for
	// a write names its sender, which every record it makes carries. A
	// non-nil error refuses the request with 403 Forbidden, or 401
	// Unauthorized when it is [ErrUnauthenticated]. Nil allows every request,
	// with no sender.
	Authorize func(r *http.Request, t Track, access Access) (sender string, err error)

	// Challenge is the WWW-Authenticate header sent with a 401, naming the
	// scheme Authorize expects, such as "Bearer". Empty sends none.
	Challenge string

	// SenderLimit bounds each sender's records into one track, and TrackLimit
	// all of a track's. A record past either is refused with 429 Too Many
	// Requests and a Retry-After. Records with no sender count only toward
	// TrackLimit.
	SenderLimit Limit
	TrackLimit  Limit

	// OnOpen is called when the handler opens a track for writing, before any
	// of its records reach OnRecord: on first use, and again on the next use
	// after the handler closed it for idleness.
	OnOpen func(ctx context.Context, t Track)

	// TrackIdleTimeout closes a track the handler has not written to for this
	// long, releasing its writer, idempotency keys and limits. Zero means
	// [DefaultTrackIdleTimeout]; a negative timeout is an error.
	TrackIdleTimeout time.Duration

	// OnRecord is called after record, the JSON encoding of a [Record], was
	// committed to t as group g, before the request is answered. Records of
	// one track are delivered in commit order.
	OnRecord func(ctx context.Context, t Track, g ledger.GroupInfo, record []byte)

	// MaxBodyBytes caps the request body. Zero means [DefaultMaxBodyBytes].
	MaxBodyBytes int64

	// Config is passed to the tracks the handler creates and opens.
	Config ledger.Config

	// Logger receives internal errors before they are answered with a generic
	// 500. Nil means no logging.
	Logger *slog.Logger
}

// Handler records into and reads back the tracks of one store, addressed by
// the URL. It implements [http.Handler]; mount it under a prefix with
// [http.StripPrefix]:
//
//	mux.Handle("/ingest/", http.StripPrefix("/ingest", handler))
//
//	PUT  /tracks/{broadcast path}/{track name}  create the track when it does not exist
//	POST /tracks/{broadcast path}/{track name}  append the body, one JSON value, as one record
//	GET  /tracks/{broadcast path}/{track name}  read the newest records, or those ?before= a group
//
// A record creates its track too; PUT is for a track that should exist before
// its first record. A record sent with an Idempotency-Key header is stored once
// per sender and track: a retry with the same key is answered as the first was.
type Handler struct {
	store store.Store
	opts  Options

	maxBody int64
	idle    time.Duration
	logger  *slog.Logger
	mux     *http.ServeMux

	mu     sync.Mutex
	tracks map[ledger.TrackPath]*track
	// nextSweep is when open next looks for idle tracks to close.
	nextSweep time.Time
}

// track is one track the handler writes to.
type track struct {
	Track

	// users counts the requests holding the track, and lastUsed is when the
	// last one let go. A track is closed only when no request holds it.
	// Guarded by Handler.mu.
	users    int
	lastUsed time.Time

	// mu serializes the track's records and guards the fields below.
	mu     sync.Mutex
	writer *ledger.Writer
	// replies holds the answers to keyed records, oldest key first in keys.
	replies map[string]recordResponse
	keys    []string
	// limit is the track's bucket, and senders each sender's.
	limit   bucket
	senders map[string]*bucket
}

// NewHandler builds a [Handler] over s.
func NewHandler(s store.Store, opts Options) (*Handler, error) {
	if s == nil {
		return nil, errors.New("ingest: nil store")
	}
	if opts.TrackIdleTimeout < 0 {
		return nil, fmt.Errorf("ingest: negative TrackIdleTimeout %v", opts.TrackIdleTimeout)
	}
	h := &Handler{
		store:   s,
		opts:    opts,
		maxBody: opts.MaxBodyBytes,
		idle:    opts.TrackIdleTimeout,
		logger:  opts.Logger,
		mux:     http.NewServeMux(),
		tracks:  make(map[ledger.TrackPath]*track),
	}
	if h.maxBody == 0 {
		h.maxBody = DefaultMaxBodyBytes
	}
	if h.idle == 0 {
		h.idle = DefaultTrackIdleTimeout
	}
	if h.logger == nil {
		h.logger = slog.New(slog.DiscardHandler)
	}
	h.mux.HandleFunc("PUT /tracks/{track...}", h.serveCreate)
	h.mux.HandleFunc("POST /tracks/{track...}", h.serveRecord)
	h.mux.HandleFunc("GET /tracks/{track...}", h.serveHistory)
	return h, nil
}

// ServeHTTP implements [http.Handler].
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mux.ServeHTTP(w, r)
}

// serveCreate creates the request's track when it does not exist: 201 when it
// did not, 204 when it did.
func (h *Handler) serveCreate(w http.ResponseWriter, r *http.Request) {
	t, _, ok := h.resolve(w, r, Write)
	if !ok {
		return
	}
	tr, created, err := h.open(r.Context(), t)
	if err != nil {
		h.internalError(w, r, "open track", err)
		return
	}
	h.release(tr)
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
	t, sender, ok := h.resolve(w, r, Write)
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
	data, err := json.Marshal(Record{Sender: sender, Payload: payload})
	if err != nil {
		h.internalError(w, r, "encode record", err)
		return
	}
	tr, _, err := h.open(r.Context(), t)
	if err != nil {
		h.internalError(w, r, "open track", err)
		return
	}
	defer h.release(tr)

	// A key is its sender's: two senders that happen to choose the same key
	// do not answer each other's records.
	if key != "" {
		key = sender + "\x00" + key
	}

	// Committing and notifying under one lock delivers a track's records to
	// OnRecord in commit order, and lets a retry find the reply to its key.
	tr.mu.Lock()
	if reply, seen := tr.replies[key]; seen && key != "" {
		tr.mu.Unlock()
		writeJSON(w, http.StatusCreated, reply)
		return
	}
	if wait := h.admit(tr, sender); wait > 0 {
		tr.mu.Unlock()
		w.Header().Set("Retry-After", strconv.Itoa(int(wait.Seconds())+1))
		http.Error(w, http.StatusText(http.StatusTooManyRequests), http.StatusTooManyRequests)
		return
	}
	// The append outlives a client that leaves mid-request: cut short, it
	// could not tell whether the store took the commit, and a record that was
	// committed is still delivered to OnRecord.
	group, err := tr.writer.Append(context.WithoutCancel(r.Context()), 0, data)
	// An append that committed but could not seal returns the group with its
	// error: the record is stored, so it is delivered and answered as such.
	committed := group.ObjectKey != ""
	reply := recordResponse{Group: group.ID.String(), Wallclock: group.Wallclock}
	if committed {
		if key != "" {
			tr.remember(key, reply)
		}
		if h.opts.OnRecord != nil {
			h.opts.OnRecord(r.Context(), tr.Track, group, data)
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

// historyEntry is one record of a history page.
type historyEntry struct {
	Group     string `json:"group"`
	Wallclock int64  `json:"wallclock"`
	Record
}

// historyResponse is the body of a history page, oldest record first. Before is
// the cursor for the page of older records, present while there may be one.
type historyResponse struct {
	Records []historyEntry `json:"records"`
	Before  string         `json:"before,omitempty"`
}

// serveHistory answers a page of the track's records: the newest, or those
// committed before the group ?before= names, at most ?limit=.
func (h *Handler) serveHistory(w http.ResponseWriter, r *http.Request) {
	t, _, ok := h.resolve(w, r, Read)
	if !ok {
		return
	}
	query := r.URL.Query()
	limit := DefaultPageSize
	if raw := query.Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > MaxPageSize {
			http.Error(w, fmt.Sprintf("limit must be between 1 and %d", MaxPageSize), http.StatusBadRequest)
			return
		}
		limit = n
	}
	var before ledger.GroupID
	if raw := query.Get("before"); raw != "" {
		id, err := ledger.ParseGroupID(raw)
		if err != nil {
			http.Error(w, "before must be a group ID", http.StatusBadRequest)
			return
		}
		before = id
	}

	lt, err := ledger.Open(r.Context(), h.store, t.Path(), h.opts.Config)
	if errors.Is(err, ledger.ErrTrackNotFound) {
		http.Error(w, "no such track", http.StatusNotFound)
		return
	}
	if err != nil {
		h.internalError(w, r, "open track", err)
		return
	}
	reader, err := lt.Reader(r.Context())
	if err != nil {
		h.internalError(w, r, "read track", err)
		return
	}
	groups, err := reader.Before(r.Context(), before, limit)
	if err != nil {
		h.internalError(w, r, "read history", err)
		return
	}
	page := historyResponse{Records: make([]historyEntry, 0, len(groups))}
	for _, g := range groups {
		data, err := reader.ReadGroup(r.Context(), g.ObjectKey)
		if err != nil {
			h.internalError(w, r, "read record", err)
			return
		}
		entry := historyEntry{Group: g.ID.String(), Wallclock: g.Wallclock}
		if err := json.Unmarshal(data, &entry.Record); err != nil {
			h.internalError(w, r, "decode record", err)
			return
		}
		page.Records = append(page.Records, entry)
	}
	if len(groups) == limit {
		page.Before = groups[0].ID.String()
	}
	writeJSON(w, http.StatusOK, page)
	// A handler that is only read from still lets go of the tracks it wrote.
	h.mu.Lock()
	h.closeIdle(time.Now())
	h.mu.Unlock()
}

// resolve reads the request's track and authorizes the access on it, answering
// the request when either fails.
func (h *Handler) resolve(w http.ResponseWriter, r *http.Request, access Access) (Track, string, bool) {
	t, err := trackFromPath(r.PathValue("track"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return Track{}, "", false
	}
	if h.opts.Authorize == nil {
		return t, "", true
	}
	sender, err := h.opts.Authorize(r, t, access)
	switch {
	case err == nil:
		return t, sender, true
	case errors.Is(err, ErrUnauthenticated):
		if h.opts.Challenge != "" {
			w.Header().Set("WWW-Authenticate", h.opts.Challenge)
		}
		http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
	default:
		http.Error(w, http.StatusText(http.StatusForbidden), http.StatusForbidden)
	}
	return Track{}, "", false
}

// open returns the track's writer, creating the track when it does not exist
// and reporting whether it did. The caller holds the track until it calls
// release. Opening reads the store, so it runs without h.mu; when two requests
// open the same track at once, the first to finish is kept.
func (h *Handler) open(ctx context.Context, t Track) (*track, bool, error) {
	key := t.Path()
	h.mu.Lock()
	h.closeIdle(time.Now())
	tr, ok := h.tracks[key]
	if ok {
		tr.users++
	}
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
		tr.users++
		h.mu.Unlock()
		return tr, false, nil
	}
	tr = &track{Track: t, writer: writer, users: 1}
	// Holding the new track until OnOpen returns keeps the requests that find
	// it meanwhile from recording, so OnOpen comes before its first OnRecord.
	tr.mu.Lock()
	defer tr.mu.Unlock()
	h.tracks[key] = tr
	h.mu.Unlock()
	if h.opts.OnOpen != nil {
		h.opts.OnOpen(ctx, t)
	}
	return tr, created, nil
}

// release lets go of a track open returned.
func (h *Handler) release(tr *track) {
	h.mu.Lock()
	defer h.mu.Unlock()
	tr.users--
	tr.lastUsed = time.Now()
}

// closeIdle forgets the tracks no request holds that have gone unused for the
// idle timeout, at most every half timeout. A closed track is opened afresh
// from the store on its next use. h.mu is held.
func (h *Handler) closeIdle(now time.Time) {
	if now.Before(h.nextSweep) {
		return
	}
	h.nextSweep = now.Add(h.idle / 2)
	for key, tr := range h.tracks {
		if tr.users == 0 && now.Sub(tr.lastUsed) >= h.idle {
			delete(h.tracks, key)
		}
	}
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
