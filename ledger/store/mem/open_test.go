package mem

import (
	"testing"

	"github.com/okdaichi/qumo-ledger/ledger/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpen_RegistersTheMemoryScheme(t *testing.T) {
	tests := map[string]string{"empty": "", "scheme": Scheme + ":"}
	for name, uri := range tests {
		t.Run(name, func(t *testing.T) {
			first, err := store.Open(t.Context(), uri)
			require.NoError(t, err)
			second, err := store.Open(t.Context(), uri)
			require.NoError(t, err)

			require.IsType(t, &Store{}, first)
			_, err = first.Create(t.Context(), "key", []byte("value"))
			require.NoError(t, err)
			_, _, err = second.Get(t.Context(), "key")
			assert.ErrorIs(t, err, store.ErrNotExist, "each open is a separate store")
		})
	}
}
