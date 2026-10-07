// Package ingest accepts records over HTTP and appends them to [ledger] tracks.
//
// It is the inbound counterpart of the stream package: stream renders a track
// over HTTP, ingest fills one. A [Handler] is an [http.Handler] over a
// [store.Store].
//
// # Contributions
//
// A contributor first announces, naming a track and itself:
//
//	POST /announce
//	{"broadcast_path": "/room/123", "track_name": "chat", "name": "alice"}
//
//	201 Created
//	Location: contributions/{id}
//
// The announce creates the track when it does not exist and starts a
// contribution, which its ID addresses from then on. Each record is the body of
// a POST to the contribution, any JSON value, and is answered once it is
// committed as one group:
//
//	POST /contributions/{id}/records
//	{"text": "hello"}
//
// DELETE /contributions/{id} ends the contribution. So does announcing again
// under the same name in the same track, which starts a new one, and recording
// nothing for [Options.IdleTimeout]. Requests to an ended contribution answer
// 410 Gone.
//
// Records are routed by the URL, so a request is resolved and authorized before
// its body is read.
//
// # Many contributors, one track
//
// A track is named by its broadcast path and track name; name identifies who is
// contributing to it. Any number of contributors record into the same track,
// and the track's commit order is their order. Each group stores the
// contributor alongside its payload, as a [Record].
//
// # Hooks
//
// The handler authorizes nothing and delivers nothing by itself.
// [Options.Authorize] decides whether a request may proceed: it is asked with
// the announcement for the announce and again for every request to the
// contribution, so a credential that expires or is revoked stops a live
// contribution. [Options.OnAnnounce] and [Options.OnRecord] observe what was
// committed, which is where a caller forwards a record to live subscribers.
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
