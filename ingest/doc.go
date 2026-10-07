// Package ingest accepts records over HTTP and appends them to [ledger] tracks.
//
// It is the inbound counterpart of the stream package: stream renders a track
// over HTTP, ingest fills one. A [Handler] is an [http.Handler] over a
// [store.Store] that serves two POST requests, both with a JSON body naming a
// track and a contributor:
//
//	{"broadcast_path": "/room/123", "track_name": "chat", "name": "alice"}
//
// Announce establishes the track, creating it when it does not exist. Record
// appends one group to an announced track and answers once it is committed.
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
// [Options.Authorize] decides whether a request may proceed, and
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
