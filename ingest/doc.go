// Package ingest accepts records over HTTP and appends them to [ledger] tracks.
//
// It is the inbound counterpart of the stream package: stream renders a track
// over HTTP, ingest fills one. A [Handler] is an [http.Handler] over a
// [store.Store].
//
// # Contributions
//
// A track is named by its broadcast path and track name, a [Track]. A
// contributor first announces the track:
//
//	POST /announce
//	{"broadcast_path": "/room/123", "track_name": "chat"}
//
//	201 Created
//	Location: contributions/{id}
//
// The announce creates the track when it does not exist and starts a
// contribution, which its ID addresses from then on. Each record is the body of
// a POST to the contribution, one JSON value, and is answered once it is
// committed as one group:
//
//	POST /contributions/{id}/records
//	{"user": "alice", "text": "hello"}
//
// The payload is stored and delivered as it was sent; whatever identifies its
// author belongs in it. Records are routed by the URL, so a request is resolved
// and authorized before its body is read.
//
// DELETE /contributions/{id} ends the contribution, as does recording nothing
// for [Options.IdleTimeout]. Requests to an ended contribution answer 410 Gone.
//
// # Retries
//
// A record sent with an Idempotency-Key header is stored once per contribution:
// a retry with the same key is answered with the first reply and stores
// nothing. A contribution remembers its most recent keys, so a retry belongs
// soon after the request it repeats.
//
// # Many contributions, one track
//
// Any number of contributions record into the same track, and the track's
// commit order is their order.
//
// # Hooks
//
// The handler authorizes nothing and delivers nothing by itself.
// [Options.Authorize] decides whether a request on a track may proceed: it is
// asked for the announce and again for every request to the contribution, so a
// credential that expires or is revoked stops a live contribution.
// [Options.OnAnnounce] and [Options.OnRecord] observe what was committed, which
// is where a caller forwards a record to live subscribers.
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
