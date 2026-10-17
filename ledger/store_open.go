package ledger

import (
	"context"

	"github.com/okdaichi/qumo-ledger/ledger/store"
	_ "github.com/okdaichi/qumo-ledger/ledger/store/all"
)

// OpenStore opens the ledger store that uri names, registering every built-in
// backend (mem, file, s3, postgres). An empty uri opens memory.
//
// The returned store implements [fmt.Stringer] for startup logs.
func OpenStore(ctx context.Context, uri string) (store.Store, error) {
	return store.Open(ctx, uri)
}
