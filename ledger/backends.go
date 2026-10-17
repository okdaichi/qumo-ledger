package ledger

// Importing ledger registers every built-in store backend for [store.Open]:
// s3 (bucket), postgres / postgresql (db), file (fs), and mem (mem).
import (
	_ "github.com/okdaichi/qumo-ledger/ledger/store/bucket" // s3:
	_ "github.com/okdaichi/qumo-ledger/ledger/store/db"     // postgres:, postgresql:
	_ "github.com/okdaichi/qumo-ledger/ledger/store/fs"     // file:
	_ "github.com/okdaichi/qumo-ledger/ledger/store/mem"    // mem:, ""
)
