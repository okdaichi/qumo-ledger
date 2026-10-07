// Package ingest accepts records over HTTP and appends them to [ledger] tracks.
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
//	{"user": "alice", "text": "hello"}
//
//	201 Created
//	{"group": "e000001-g00000000", "wallclock": 1791370000000000000}
//
// The payload is stored and delivered as it was sent; whatever identifies its
// author belongs in it. A record creates its track when the track does not
// exist; PUT to the track creates it ahead of its first record. Requests are
// routed by the URL, so a request is resolved and authorized before its body is
// read. Any number of senders record into one track, and the track's commit
// order is their order.
//
// # Retries
//
// A record sent with an Idempotency-Key header is stored once per track: a
// retry with the same key is answered with the first reply and stores nothing.
// A track remembers its most recent keys, so a retry belongs soon after the
// request it repeats.
//
// # Hooks
//
// The handler authorizes nothing and delivers nothing by itself.
// [Options.Authorize] decides whether a request on a track may proceed, on
// every request. [Options.OnOpen] observes a track the handler starts writing
// to, and [Options.OnRecord] what was committed, which is where a caller
// forwards a record to live subscribers.
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
