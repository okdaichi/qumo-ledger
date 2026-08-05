package stream

import (
	"context"
	"errors"
	"fmt"
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
	window   int
}

// ErrInitRequired reports that the track's container cannot be played without an
// initialization segment and [Options.InitSegment] was left zero. Segments of a
// fragmented-MP4 track are moof+mdat: the codec configuration lives in the moov
// of a separate init segment, so a manifest without one is unplayable by every
// client. Failing here is deliberate — the manifests would otherwise render
// without #EXT-X-MAP (or DASH @initialization) and be served as a valid 200,
// turning a misconfiguration into a silent playback failure.
var ErrInitRequired = errors.New("stream: init segment required for this encoding")

// Options configures a [Handler]. The zero value is usable alongside a track
// whose container is self-initializing: the resolver proxies, segments take the
// schema's extension and MIME, and no init segment is emitted. A
// fragmented-MP4 track additionally requires InitSegment; see [ErrInitRequired].
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

	// Window caps how many of the most recent segments a manifest lists. Zero
	// lists every committed segment.
	//
	// A live track runs indefinitely, so an unwindowed manifest grows without
	// bound: an hour of one-second segments is 3,600 entries re-rendered on
	// every request, and a player re-fetches the whole thing each reload. A
	// window bounds the response and turns the HLS playlist from EVENT (append
	// only, whole history) into a sliding live playlist.
	//
	// It bounds the manifest, not the read: the handler still walks the track to
	// count what precedes the window, because a sliding playlist's
	// EXT-X-MEDIA-SEQUENCE has to be exact. Segments outside the window remain
	// addressable — the ledger deletes nothing, so a client that kept a URL can
	// still fetch it.
	Window int
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
//
// It returns [ErrInitRequired] when the schema's container needs an
// initialization segment and Options.InitSegment is zero.
func NewHandler(track *ledger.Track, opts Options) (*Handler, error) {
	if track == nil {
		return nil, errors.New("stream: nil track")
	}

	info := track.Root()

	if requiresInit(info.TrackSchema) && opts.InitSegment.Bytes == nil && opts.InitSegment.URL == "" {
		return nil, fmt.Errorf("%w: encoding %q", ErrInitRequired, info.TrackSchema.Encoding)
	}

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
		window:   opts.Window,
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
	window, err := h.gather(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	body, err := renderHLS(h.schema, window.groups, h.renderOpts(window))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
	_, _ = w.Write(body) // not actionable: once the body write fails the client is gone
}

// serveDASH renders and writes the DASH MPD.
func (h *Handler) serveDASH(w http.ResponseWriter, r *http.Request) {
	window, err := h.gather(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	body, err := renderDASH(h.schema, window.groups, h.renderOpts(window))
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

// manifestWindow is what the renderers need about a track: the groups to list,
// and what the groups before them imply for the numbering a sliding manifest has
// to carry.
type manifestWindow struct {
	// groups are the segments to list, in commit order.
	groups []ledger.GroupInfo

	// dropped is how many segments preceded the window — the first listed
	// segment's media sequence.
	dropped uint64

	// discontinuities is how many epoch changes preceded the window: the
	// discontinuity sequence the first listed segment continues from.
	discontinuities uint64

	// precedingEpoch is the epoch of the segment immediately before the window,
	// or zero when the window starts at the track's beginning. A window that
	// opens on a new epoch has a timeline reset at its very first segment, and
	// nothing in the listed groups themselves reveals it.
	precedingEpoch uint64
}

// gather opens a Reader, rewinds to the start, and drains every committed group
// in commit order. It is the shared input to both renderers.
//
// With a window configured it keeps only the newest Window groups, in a ring,
// and accumulates what the dropped groups imply for the manifest's numbering. A
// sliding playlist must state its media and discontinuity sequences exactly and
// consistently across reloads, which is what the full walk buys — the output is
// short but the counts are not derivable from it. The walk reads manifests, not
// segment payloads.
func (h *Handler) gather(ctx context.Context) (manifestWindow, error) {
	reader, err := h.track.Reader(ctx)
	if err != nil {
		return manifestWindow{}, err
	}
	reader.SeekStart()

	var out manifestWindow

	// A discontinuity belongs to the segment that opens a new epoch, so it
	// rolls off the manifest when that segment does — the ring carries the flag
	// alongside the group rather than recomputing it from a window that no
	// longer contains the boundary.
	type entry struct {
		info          ledger.GroupInfo
		discontinuity bool
	}

	// ring holds the last h.window entries when windowing; next is where the
	// following one lands, wrapping once the ring is full.
	var ring []entry
	var next int
	if h.window > 0 {
		ring = make([]entry, 0, h.window)
	}

	var prevEpoch uint64
	var seen uint64

	for {
		g, err := reader.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return manifestWindow{}, err
		}

		epoch := g.ID.Epoch()
		e := entry{info: g, discontinuity: seen > 0 && epoch != prevEpoch}
		prevEpoch = epoch
		seen++

		if h.window <= 0 {
			out.groups = append(out.groups, g)
			continue
		}
		if len(ring) < h.window {
			ring = append(ring, e)
			continue
		}

		// The evicted entry is now behind the window: it becomes what precedes
		// the first listed segment, and its own discontinuity has rolled off.
		evicted := ring[next]
		out.precedingEpoch = evicted.info.ID.Epoch()
		if evicted.discontinuity {
			out.discontinuities++
		}

		ring[next] = e
		next = (next + 1) % h.window
		out.dropped++
	}

	if h.window <= 0 {
		return out, nil
	}

	// Unroll the ring back into commit order: once full, the oldest entry sits
	// at the write cursor.
	ordered := ring
	if len(ring) == h.window {
		ordered = append(ring[next:len(ring):len(ring)], ring[:next]...)
	}
	out.groups = make([]ledger.GroupInfo, len(ordered))
	for i, e := range ordered {
		out.groups[i] = e.info
	}
	return out, nil
}

// renderOpts is the renderers' input projected from the handler's resolved state
// and the window it just gathered.
func (h *Handler) renderOpts(window manifestWindow) renderOpts {
	return renderOpts{
		segExt:                h.segExt,
		initURI:               h.initURI(),
		sliding:               h.window > 0,
		mediaSequence:         window.dropped,
		discontinuitySequence: window.discontinuities,
		precedingEpoch:        window.precedingEpoch,
	}
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

// requiresInit reports whether a schema's container carries its codec
// configuration in a separate initialization segment rather than in every
// segment. Fragmented MP4 does; MPEG-TS repeats its PAT/PMT per segment and so
// does not. An unrecognized encoding is left permissive: the caller knows its
// container, and refusing to serve one this package has no opinion about would
// be worse than serving it.
func requiresInit(s ledger.TrackSchema) bool {
	return s.Encoding == "fmp4"
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
