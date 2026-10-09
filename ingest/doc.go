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
// # Redaction
//
// A DELETE to the track, ?group= naming one of its records, takes that record
// out of the track. The handler commits a redaction, a record naming the group
// in redacts with no payload, which reaches [Options.OnRecord] like any
// record, so live subscribers learn of it. Then the ledger redacts the group
// ([ledger.Track.Redact]): it marks the group redacted and deletes its
// payload. History answers the group from then on with its group and
// wallclock and "redacted": true, and no sender or payload:
//
//	DELETE /tracks/room/123/chat?group=e000001-g00000003
//
//	201 Created
//	{"group": "e000001-g00000007", "wallclock": 1791370000000000000}
//
// Redactions of one track run one at a time. A group already marked redacted
// is answered 204 and commits nothing; that includes a retry after a delete
// failed, which finishes the delete. A redaction can't itself be redacted. A
// payload missing without the mark is a lost object, answered 500, never
// passed off as a redaction.
//
// A consumer of [Options.OnRecord] tells a redaction by its redacts, not by
// a missing payload. [Options.Authorize] decides a redaction as a [Redact],
// and the sender it names is the redaction's. It sees the track, not the
// record, so redaction is for a party trusted with the whole track; an app
// that lets senders redact their own records checks the record first.
// Redactions are not counted against the track's limits.
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
// [Options.Authorize] decides every request, for a [Write], a [Read] or a [Redact], and
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
//
// A Handler closes a track it has not written to for [Options.TrackIdleTimeout],
// so a long-running one holds only the tracks in use. A closed track is opened
// again from the store on its next request; its idempotency keys and limits
// start afresh.
package ingest
