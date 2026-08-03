package stream

import (
	"context"
	"errors"
	"io"
	"net/http"
	"path"
	"strconv"
	"strings"

	"github.com/okdaichi/qumo-ledger/ledger"
)

// Handler serves one track over HTTP as both HLS and DASH. It implements
// [http.Handler] but owns no listener and no lifecycle: drop it into an
// [http.Server], the way [http.FileServer] is used. It routes by the request
// URL's base name, so it is mount-point-agnostic — mount it at the track's path:
//
//	mux.Handle("/"+string(track)+"/", handler)
//
//	.../playlist.m3u8 (or any *.m3u8)  the HLS media playlist
//	.../manifest.mpd   (or any *.mpd)   the DASH MPD
//	.../<group-id>.<ext>                one segment — shared by both formats
//	.../init.<ext>                      the init segment, when supplied
//
// A fresh [*ledger.Reader] is opened per request. A Reader is single-consumer,
// and concurrent consumers each opening their own is the documented cheap
// pattern, so the handler does that rather than share one.
type Handler struct {
	track    *ledger.Track
	schema   ledger.TrackSchema
	resolver SegmentResolver
	init     InitSegment
	segExt   string
	segMIME  string
}

// Options configures a [Handler]. The zero value is usable alongside a track: the
// resolver proxies, segments take the schema's extension and MIME, and no init
// segment is emitted.
type Options struct {
	// Resolver decides redirect-versus-proxy per segment. Nil means
	// [ProxyResolver].
	Resolver SegmentResolver

	// InitSegment, when set, is emitted as HLS #EXT-X-MAP and DASH
	// @initialization. Leave zero for self-initializing containers (MPEG-TS).
	InitSegment InitSegment

	// SegmentExt overrides the schema-derived segment extension (".m4s" for fmp4).
	SegmentExt string

	// SegmentMIME overrides the schema-derived segment MIME type.
	SegmentMIME string
}

// InitSegment is the fMP4 initialization segment (ftyp + moov). Supply Bytes —
// served at /init.<ext> and referenced from both manifests — or URL, referenced
// as-is and not served. Leave zero for self-initializing containers such as
// MPEG-TS.
type InitSegment struct {
	// Bytes, when set, is served at the init URL and referenced from both
	// manifests. URL is ignored when Bytes is set.
	Bytes []byte

	// URL, when set and Bytes is empty, is referenced from both manifests and is
	// not served by this handler — for an init segment already reachable at a
	// signed object URL.
	URL string
}

// NewHandler builds a [Handler] over track. It reads the track root once to
// derive the segment extension and MIME from the schema; the matching Options
// fields override them.
func NewHandler(track *ledger.Track, opts Options) (*Handler, error) {
	if track == nil {
		return nil, errors.New("stream: nil track")
	}

	info := track.Root()

	segExt := opts.SegmentExt
	if segExt == "" {
		segExt = defaultSegmentExt(info.TrackSchema)
	}
	segMIME := opts.SegmentMIME
	if segMIME == "" {
		segMIME = info.MIME
	}

	resolver := opts.Resolver
	if resolver == nil {
		resolver = ProxyResolver{}
	}

	return &Handler{
		track:    track,
		schema:   info.TrackSchema,
		resolver: resolver,
		init:     opts.InitSegment,
		segExt:   segExt,
		segMIME:  segMIME,
	}, nil
}

// ServeHTTP routes a request to the playlist, manifest, segment, or init handler.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	base := path.Base(r.URL.Path)
	switch {
	case strings.HasSuffix(base, ".m3u8"):
		h.serveHLS(w, r)
	case strings.HasSuffix(base, ".mpd"):
		h.serveDASH(w, r)
	case base == "init"+h.segExt:
		h.serveInit(w, r)
	default:
		h.serveSegment(w, r)
	}
}

// serveHLS renders and writes the HLS media playlist.
func (h *Handler) serveHLS(w http.ResponseWriter, r *http.Request) {
	groups, err := h.gather(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	body, err := renderHLS(h.schema, groups, h.renderOpts())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
	_, _ = w.Write(body) // not actionable: once the body write fails the client is gone
}

// serveDASH renders and writes the DASH MPD.
func (h *Handler) serveDASH(w http.ResponseWriter, r *http.Request) {
	groups, err := h.gather(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	body, err := renderDASH(h.schema, groups, h.renderOpts())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/dash+xml")
	_, _ = w.Write(body) // not actionable: once the body write fails the client is gone
}

// serveInit writes the caller-supplied init segment, when there is one.
func (h *Handler) serveInit(w http.ResponseWriter, r *http.Request) {
	if len(h.init.Bytes) == 0 {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", h.segMIME)
	w.Header().Set("Content-Length", strconv.Itoa(len(h.init.Bytes)))
	_, _ = w.Write(h.init.Bytes) // not actionable: once the body write fails the client is gone
}

// serveSegment resolves a segment by its GroupID and either redirects to it or
// proxies its bytes. One handler serves both HLS and DASH, since both address
// segments by GroupID.
func (h *Handler) serveSegment(w http.ResponseWriter, r *http.Request) {
	id, ok := parseSegmentID(path.Base(r.URL.Path), h.segExt)
	if !ok {
		http.NotFound(w, r)
		return
	}

	reader, err := h.track.Reader(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	group, err := reader.Lookup(r.Context(), id)
	if err != nil {
		// No committed group for the id: the segment was never written, or the
		// client is asking ahead of the tip.
		http.NotFound(w, r)
		return
	}

	url, err := h.resolver.ResolveSegment(r.Context(), group)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if url != "" {
		http.Redirect(w, r, url, http.StatusFound)
		return
	}

	data, err := reader.ReadGroup(r.Context(), group)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	mime := group.MIME
	if mime == "" {
		mime = h.segMIME
	}
	w.Header().Set("Content-Type", mime)
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	_, _ = w.Write(data) // not actionable: once the body write fails the client is gone
}

// gather opens a Reader, rewinds to the start, and drains every committed group
// in commit order. It is the shared input to both renderers.
func (h *Handler) gather(ctx context.Context) ([]ledger.GroupInfo, error) {
	reader, err := h.track.Reader(ctx)
	if err != nil {
		return nil, err
	}
	reader.SeekStart()

	var groups []ledger.GroupInfo
	for {
		g, err := reader.Next(ctx)
		if errors.Is(err, io.EOF) {
			return groups, nil
		}
		if err != nil {
			return nil, err
		}
		groups = append(groups, g)
	}
}

// renderOpts is the renderers' input projected from the handler's resolved state.
func (h *Handler) renderOpts() renderOpts {
	return renderOpts{segExt: h.segExt, initURI: h.initURI()}
}

// initURI is the URI both manifests reference for the init segment: a local path
// when the handler serves the bytes, the caller's URL when it does not.
func (h *Handler) initURI() string {
	switch {
	case len(h.init.Bytes) > 0:
		return "init" + h.segExt
	case h.init.URL != "":
		return h.init.URL
	default:
		return ""
	}
}

// parseSegmentID reads a GroupID off a segment URL's base name by stripping the
// segment extension. The second result is false when the name is not a group id.
func parseSegmentID(base, ext string) (ledger.GroupID, bool) {
	id, err := ledger.ParseGroupID(strings.TrimSuffix(base, ext))
	if err != nil || id == 0 {
		return 0, false
	}
	return id, true
}

// defaultSegmentExt picks a segment extension for a schema the caller did not
// override. fmp4 segments are CMAF .m4s; MPEG-TS is .ts; anything else falls back
// to a generic extension rather than guessing.
func defaultSegmentExt(s ledger.TrackSchema) string {
	switch s.Encoding {
	case "fmp4":
		return ".m4s"
	case "ts", "mpegts":
		return ".ts"
	default:
		return ".bin"
	}
}
