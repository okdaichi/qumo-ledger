// Package all registers every built-in store backend: s3 (bucket),
// postgres / postgresql (db), file (fs), and mem (mem).
//
// Importing it enables those URI schemes for [store.Open]:
//
//	import _ "github.com/okdaichi/qumo-ledger/ledger/store/all"
package all

import (
	_ "github.com/okdaichi/qumo-ledger/ledger/store/bucket" // s3:
	_ "github.com/okdaichi/qumo-ledger/ledger/store/db"     // postgres:, postgresql:
	_ "github.com/okdaichi/qumo-ledger/ledger/store/fs"     // file:
	_ "github.com/okdaichi/qumo-ledger/ledger/store/mem"    // mem:, ""
)
