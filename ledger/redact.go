package ledger

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/okdaichi/qumo-ledger/ledger/store"
)

// Redact takes the payload of the committed group g out of the track. It
// first records that g is redacted, in a marker object beside the epoch's
// groups, then deletes g's payload object. The manifest row stays, so ids,
// ordering and seeks hold, and [Reader.ReadGroup] answers [ErrGroupRedacted]
// for g from then on. Redacting a group again finishes any delete a failure cut
// short and is not an error.
//
// It is the one exception to group objects being immutable, and it never
// touches a manifest. Pass g as a row from [Reader.Lookup] or a range read.
func (t *Track) Redact(ctx context.Context, g GroupInfo) error {
	marker, err := t.redactedKey(g)
	if err != nil {
		return err
	}
	if _, err := t.store.Create(ctx, marker, nil); err != nil && !errors.Is(err, store.ErrExist) {
		return fmt.Errorf("ledger: mark %s redacted: %w", g.ID, err)
	}
	if err := t.store.Delete(ctx, g.ObjectKey); err != nil {
		return fmt.Errorf("ledger: delete redacted group %s: %w", g.ID, err)
	}
	return nil
}

// Redacted reports whether the committed group g has been redacted.
func (t *Track) Redacted(ctx context.Context, g GroupInfo) (bool, error) {
	marker, err := t.redactedKey(g)
	if err != nil {
		return false, err
	}
	return markerExists(ctx, t.store, marker)
}

// redactedKey is the marker key of g, a group of t.
func (t *Track) redactedKey(g GroupInfo) (string, error) {
	marker, ok := redactedKey(g.ObjectKey)
	if !ok || !strings.HasPrefix(g.ObjectKey, string(t.path)+"/") {
		return "", fmt.Errorf("%w: %q is not a group object of %s", ErrInvalidGroup, g.ObjectKey, t.path)
	}
	return marker, nil
}

// redactedKey maps a group's object key to its redaction marker:
// <track>/e000001/groups/g00000042 to <track>/e000001/redacted/g00000042.
func redactedKey(objectKey string) (string, bool) {
	dir, name, ok := strings.Cut(objectKey, "/"+groupPrefix)
	if !ok || name == "" || strings.Contains(name, "/") {
		return "", false
	}
	return dir + "/" + redactedPrefix + name, true
}

func markerExists(ctx context.Context, objects store.Store, key string) (bool, error) {
	_, _, err := objects.Get(ctx, key)
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, store.ErrNotExist):
		return false, nil
	default:
		return false, fmt.Errorf("ledger: read redaction marker %s: %w", key, err)
	}
}
