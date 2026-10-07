package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"path"
	"strings"
	"sync"

	"github.com/okdaichi/qumo-ledger/ledger"
	"github.com/okdaichi/qumo-ledger/ledger/store"
)

// DefaultMaxBodyBytes is the largest request body a [Handler] reads when
// [Options.MaxBodyBytes] is zero.
const DefaultMaxBodyBytes = 64 << 10

// Encoding and MIME of the tracks a [Handler] creates: every group is one JSON
// [Record].
const (
	Encoding = "json"
	MIME     = "application/json"
)

// timescale is the media timescale of the tracks a Handler creates.
const timescale = 1000

// Request is the body of an announce or a record request.
type Request struct {
	// BroadcastPath and TrackName together name the track.
	BroadcastPath string `json:"broadcast_path"`
	TrackName     string `json:"track_name"`

	// Name identifies the contributor within the track.
	Name string `json:"name"`

	// Payload is the record's content, any JSON value. Announce ignores it.
	Payload json.RawMessage `json:"payload,omitempty"`
}

// Track returns the ledger track the request names: the broadcast path without
// its leading slash, followed by the track name.
func (r *Request) Track() ledger.TrackPath {
	return ledger.TrackPath(strings.Trim(r.BroadcastPath, "/") + "/" + r.TrackName)
}

// Record is what one group of an ingested track stores: the contributor and
// the payload it sent.
type Record struct {
	Name    string          `json:"name"`
	Payload json.RawMessage `json:"payload"`
}

// Announced describes a track an announce request established.
type Announced struct {
	// BroadcastPath and TrackName are the request's, and Track is the ledger
	// track they name.
	BroadcastPath string
	TrackName     string
	Track         ledger.TrackPath

	Name string

	// Created reports whether this request created the track.
	Created bool
}

// Recorded describes a committed record.
type Recorded struct {
	// BroadcastPath and TrackName are the request's, and Track is the ledger
	// track they name.
	BroadcastPath string
	TrackName     string
	Track         ledger.TrackPath

	Group ledger.GroupInfo
	Record
}

// Options configures a [Handler]. The zero value accepts every request.
type Options struct {
	// Authorize decides whether a request may proceed. A non-nil error refuses
	// it with 403 Forbidden. Nil allows every request.
	Authorize func(r *http.Request, req *Request) error

	// OnAnnounce is called after an announce request established its track.
	OnAnnounce func(ctx context.Context, a Announced)

	// OnRecord is called after a record was committed, before the request is
	// answered. Records of one track are delivered in commit order.
	OnRecord func(ctx context.Context, rec Recorded)

	// MaxBodyBytes caps the request body. Zero means [DefaultMaxBodyBytes].
	MaxBodyBytes int64

	// Config is passed to the tracks the handler creates and opens.
	Config ledger.Config

	// Logger receives internal errors before they are answered with a generic
	// 500. Nil means no logging.
	Logger *slog.Logger
}

// Handler accepts announce and record requests and appends to the tracks of one
// store. It implements [http.Handler] and routes by the request URL's base
// name, so it is mount-point-agnostic:
//
//	mux.Handle("/ingest/", handler)
//
//	POST .../announce  establish a track
//	POST .../record    append one record to an announced track
type Handler struct {
	store store.Store
	opts  Options

	maxBody int64
	logger  *slog.Logger

	mu     sync.Mutex
	tracks map[ledger.TrackPath]*trackWriter
}

// trackWriter serializes the records of one track.
type trackWriter struct {
	mu     sync.Mutex
	writer *ledger.Writer
}

// NewHandler builds a [Handler] over s.
func NewHandler(s store.Store, opts Options) (*Handler, error) {
	if s == nil {
		return nil, errors.New("ingest: nil store")
	}
	maxBody := opts.MaxBodyBytes
	if maxBody == 0 {
		maxBody = DefaultMaxBodyBytes
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &Handler{
		store:   s,
		opts:    opts,
		maxBody: maxBody,
		logger:  logger,
		tracks:  make(map[ledger.TrackPath]*trackWriter),
	}, nil
}

// ServeHTTP routes a request to the announce or record handler.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var serve func(http.ResponseWriter, *http.Request, *Request)
	switch path.Base(r.URL.Path) {
	case "announce":
		serve = h.serveAnnounce
	case "record":
		serve = h.serveRecord
	default:
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}

	req, ok := h.decode(w, r)
	if !ok {
		return
	}
	if h.opts.Authorize != nil {
		if err := h.opts.Authorize(r, req); err != nil {
			http.Error(w, http.StatusText(http.StatusForbidden), http.StatusForbidden)
			return
		}
	}
	serve(w, r, req)
}

// decode reads and validates the request body, answering the request itself
// when the body is unusable.
func (h *Handler) decode(w http.ResponseWriter, r *http.Request) (*Request, bool) {
	var req Request
	body := http.MaxBytesReader(w, r.Body, h.maxBody)
	if err := json.NewDecoder(body).Decode(&req); err != nil {
		if _, tooLarge := errors.AsType[*http.MaxBytesError](err); tooLarge {
			http.Error(w, http.StatusText(http.StatusRequestEntityTooLarge), http.StatusRequestEntityTooLarge)
			return nil, false
		}
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return nil, false
	}
	switch {
	case strings.Trim(req.BroadcastPath, "/") == "":
		http.Error(w, "broadcast_path is required", http.StatusBadRequest)
		return nil, false
	case req.TrackName == "" || strings.Contains(req.TrackName, "/"):
		http.Error(w, "track_name is required and must not contain a slash", http.StatusBadRequest)
		return nil, false
	case req.Name == "":
		http.Error(w, "name is required", http.StatusBadRequest)
		return nil, false
	}
	return &req, true
}

// announceResponse is the body of a successful announce.
type announceResponse struct {
	Track   ledger.TrackPath `json:"track"`
	Created bool             `json:"created"`
}

// serveAnnounce establishes the request's track, creating it when it does not
// exist. Announcing an existing track succeeds.
func (h *Handler) serveAnnounce(w http.ResponseWriter, r *http.Request, req *Request) {
	track := req.Track()
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

	if h.opts.OnAnnounce != nil {
		h.opts.OnAnnounce(r.Context(), Announced{
			BroadcastPath: req.BroadcastPath,
			TrackName:     req.TrackName,
			Track:         track,
			Name:          req.Name,
			Created:       created,
		})
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	writeJSON(w, status, announceResponse{Track: track, Created: created})
}

// recordResponse is the body of a successful record.
type recordResponse struct {
	Track ledger.TrackPath `json:"track"`
	// Group is the committed group's ID in its text form.
	Group string `json:"group"`
	// Wallclock is when the record was committed, in Unix nanoseconds.
	Wallclock int64 `json:"wallclock"`
}

// serveRecord appends the request's payload to its track as one group and
// answers once it is committed. A track nobody announced answers 404.
func (h *Handler) serveRecord(w http.ResponseWriter, r *http.Request, req *Request) {
	if len(req.Payload) == 0 {
		http.Error(w, "payload is required", http.StatusBadRequest)
		return
	}
	track := req.Track()
	tw, err := h.writerFor(r.Context(), track)
	switch {
	case errors.Is(err, ledger.ErrTrackNotFound):
		http.Error(w, "track is not announced", http.StatusNotFound)
		return
	case errors.Is(err, ledger.ErrInvalidTrackPath):
		http.Error(w, "invalid track path", http.StatusBadRequest)
		return
	case err != nil:
		h.internalError(w, r, "open track writer", err)
		return
	}

	record := Record{Name: req.Name, Payload: req.Payload}
	data, err := json.Marshal(record)
	if err != nil {
		http.Error(w, "invalid payload", http.StatusBadRequest)
		return
	}

	// Committing and notifying under one lock delivers a track's records to
	// OnRecord in commit order.
	tw.mu.Lock()
	group, err := tw.writer.Append(r.Context(), 0, data)
	if err == nil && h.opts.OnRecord != nil {
		h.opts.OnRecord(r.Context(), Recorded{
			BroadcastPath: req.BroadcastPath,
			TrackName:     req.TrackName,
			Track:         track,
			Group:         group,
			Record:        record,
		})
	}
	tw.mu.Unlock()
	if err != nil {
		h.internalError(w, r, "append record", err)
		return
	}

	writeJSON(w, http.StatusCreated, recordResponse{
		Track:     track,
		Group:     group.ID.String(),
		Wallclock: group.Wallclock,
	})
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
