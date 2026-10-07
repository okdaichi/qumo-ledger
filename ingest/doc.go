// Package ingest accepts records over HTTP, appends them to [ledger] tracks,
// and reads them back.
//
// It is the inbound counterpart of the stream package: stream renders a track
// over HTTP, ingest fills one. A [Handler] is an [http.Handler] over a
// [store.Store].
//
// # Tracks and records
//
// A track is named by its broadcast path and track name, a [Track], and
// addressed by its ledger key under /tracks/: the track "chat" of the broadcast
// "/room/123" is /tracks/room/123/chat. A record is the body of a POST to its
// track, one JSON value, answered once it is committed as one group:
//
//	POST /tracks/room/123/chat
//	{"text": "hello"}
//
//	201 Created
//	{"group": "e000001-g00000000", "wallclock": 1791370000000000000}
//
// Each group stores a [Record]: the payload as it was sent, and the sender
// [Options.Authorize] named for the request, so who sent a record comes from
// its credential rather than from what it says. A record creates its track when
// the track does not exist; PUT to the track creates it ahead of its first
// record. Requests are routed by the URL, so a request is resolved and
// authorized before its body is read. Any number of senders record into one
// track, and the track's commit order is their order.
//
// # History
//
// A GET to the track answers a page of its records, oldest first: the newest,
// or those before the group ?before= names, at most ?limit=. The page's before
// is the cursor for the next older page:
//
//	GET /tracks/room/123/chat?limit=2
//
//	{"records": [{"group": "e000001-g00000003", "wallclock": …, "sender": "user-42", "payload": …}, …],
//	 "before": "e000001-g00000003"}
//
// # Retries and limits
//
// A record sent with an Idempotency-Key header is stored once per sender and
// track: a retry with the same key is answered with the first reply and stores
// nothing.
// A track remembers its most recent keys, so a retry belongs soon after the
// request it repeats.
//
// [Options.SenderLimit] and [Options.TrackLimit] bound how fast records arrive,
// per sender and per track; a record past either is answered 429 with a
// Retry-After.
//
// # Hooks
//
// The handler authorizes nothing and delivers nothing by itself.
// [Options.Authorize] decides every request, for a [Write] or a [Read], and
// names a write's sender. [Options.OnOpen] observes a track the handler starts
// writing to, and [Options.OnRecord] what was committed, which is where a
// caller forwards a record to live subscribers.
//
// # Time
//
// A record carries no media time of its own, so tracks are created with
// [ledger.TimeSourceIngest] and each group is anchored by the wall clock at
// commit. Read a window back with [ledger.Reader.RangeWallclock].
//
// # Writers
//
// A track has one writer at a time. A Handler serializes the records of each
// track within its process; two processes recording into the same track are not
// coordinated here.
package ingest
