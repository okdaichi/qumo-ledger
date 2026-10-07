package memstore

import (
	"context"
	"net/url"

	"github.com/okdaichi/qumo-ledger/ledger/store"
)

// Scheme is the URI scheme [store.Open] opens with this backend. An empty URI
// opens it too.
const Scheme = store.MemoryScheme

func init() {
	store.Register(Scheme, func(context.Context, *url.URL) (store.Store, error) {
		return New(), nil
	})
}
