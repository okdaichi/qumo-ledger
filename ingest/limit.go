package ingest

import "time"

// senderBuckets is how many senders' buckets a track keeps before it forgets
// those that have refilled, which is the state a forgotten bucket starts in.
const senderBuckets = 1024

// active reports whether l limits anything.
func (l Limit) active() bool { return l.Rate > 0 && l.Burst > 0 }

// bucket is a token bucket: it holds up to Burst tokens and gains Rate a
// second, and each record takes one.
type bucket struct {
	tokens  float64
	last    time.Time
	started bool
}

// refill adds the tokens gained since the bucket was last refilled. A new
// bucket starts full.
func (b *bucket) refill(l Limit, now time.Time) {
	if !b.started {
		b.tokens, b.last, b.started = float64(l.Burst), now, true
		return
	}
	b.tokens = min(float64(l.Burst), b.tokens+now.Sub(b.last).Seconds()*l.Rate)
	b.last = now
}

// wait returns how long until the bucket holds a token, or zero when it does.
func (b *bucket) wait(l Limit) time.Duration {
	if b.tokens >= 1 {
		return 0
	}
	return time.Duration((1 - b.tokens) / l.Rate * float64(time.Second))
}

// full reports whether the bucket has refilled completely.
func (b *bucket) full(l Limit) bool { return b.tokens >= float64(l.Burst) }

// admit takes a token for one record from the track's bucket and the sender's,
// or, when either is empty, takes none and returns how long until both hold
// one. tr.mu is held.
func (h *Handler) admit(tr *track, sender string) time.Duration {
	trackLimit, senderLimit := h.opts.TrackLimit, h.opts.SenderLimit
	now := time.Now()
	var wait time.Duration
	if trackLimit.active() {
		tr.limit.refill(trackLimit, now)
		wait = tr.limit.wait(trackLimit)
	}
	var sb *bucket
	if sender != "" && senderLimit.active() {
		sb = tr.senderBucket(sender, senderLimit, now)
		wait = max(wait, sb.wait(senderLimit))
	}
	if wait > 0 {
		return wait
	}
	if trackLimit.active() {
		tr.limit.tokens--
	}
	if sb != nil {
		sb.tokens--
	}
	return 0
}

// senderBucket returns the sender's bucket, refilled to now. tr.mu is held.
func (tr *track) senderBucket(sender string, l Limit, now time.Time) *bucket {
	if tr.senders == nil {
		tr.senders = make(map[string]*bucket)
	}
	b, ok := tr.senders[sender]
	if !ok {
		if len(tr.senders) >= senderBuckets {
			for name, other := range tr.senders {
				if other.refill(l, now); other.full(l) {
					delete(tr.senders, name)
				}
			}
		}
		b = &bucket{}
		tr.senders[sender] = b
	}
	b.refill(l, now)
	return b
}
