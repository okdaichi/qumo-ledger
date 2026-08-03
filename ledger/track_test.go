package ledger

import (
	"testing"

	"github.com/okdaichi/qumo-ledger/ledger/store/memstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTrackPath_validate(t *testing.T) {
	tests := map[string]struct {
		path    TrackPath
		wantErr bool
	}{
		"single segment":         {path: "video", wantErr: false},
		"nested":                 {path: "live/camera1/video", wantErr: false},
		"empty":                  {path: "", wantErr: true},
		"absolute":               {path: "/live/camera1", wantErr: true},
		"trailing slash":         {path: "live/camera1/", wantErr: true},
		"dot segment":            {path: "live/./camera1", wantErr: true},
		"parent segment":         {path: "live/../camera1", wantErr: true},
		"escapes root":           {path: "../secrets", wantErr: true},
		"double slash":           {path: "live//camera1", wantErr: true},
		"backslash is separator": {path: `live\camera1`, wantErr: true},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			err := tt.path.validate()
			if tt.wantErr {
				assert.ErrorIs(t, err, ErrInvalidTrackPath)
				return
			}
			assert.NoError(t, err)
		})
	}
}

func TestTimeSource_valid(t *testing.T) {
	tests := map[string]struct {
		source   TimeSource
		expected bool
	}{
		"frame":   {source: TimeSourceFrame, expected: true},
		"ingest":  {source: TimeSourceIngest, expected: true},
		"empty":   {source: "", expected: false},
		"unknown": {source: "wallclock", expected: false},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.source.valid())
		})
	}
}

func TestTrackSchema_validate(t *testing.T) {
	tests := map[string]struct {
		config  TrackSchema
		wantErr bool
	}{
		"video": {
			config:  TrackSchema{Timescale: 90000, TimeSource: TimeSourceFrame},
			wantErr: false,
		},
		"zero timescale leaves timestamps dimensionless": {
			config:  TrackSchema{Timescale: 0, TimeSource: TimeSourceFrame},
			wantErr: true,
		},
		"unknown time source": {
			config:  TrackSchema{Timescale: 1000, TimeSource: "guess"},
			wantErr: true,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			err := tt.config.validate()
			if tt.wantErr {
				assert.ErrorIs(t, err, ErrInvalidGroup)
				return
			}
			assert.NoError(t, err)
		})
	}
}

// TrackInfo embeds the schema, so its fields read directly and the whole
// schema can be handed back to Create to make another track like this one.
func TestTrackInfo_TrackSchema(t *testing.T) {
	objects := memstore.New()

	_, err := Create(t.Context(), objects, "live/cam1/video", testSchema(t), Config{})
	require.NoError(t, err)

	source := openReader(t, objects).Root()

	// Embedding promotes the schema's fields, so a reader still reads them
	// straight off the meta.
	assert.Equal(t, uint32(90000), source.Timescale)
	assert.Equal(t, TimeSourceFrame, source.TimeSource)
	assert.Equal(t, TrackPath("live/cam1/video"), source.Track)
	assert.Equal(t, uint64(1), source.Epoch)

	// The schema is a value, so a second track can be created with the first
	// one's schema rather than restating it.
	clone, err := Create(t.Context(), objects, "live/cam2/video", source.TrackSchema, Config{})
	require.NoError(t, err)

	cloned, err := clone.Reader(t.Context())
	require.NoError(t, err)

	assert.Equal(t, source.TrackSchema, cloned.Root().TrackSchema,
		"a track created from another's schema must match it exactly")
	assert.Equal(t, TrackPath("live/cam2/video"), cloned.Root().Track,
		"only the path differs — it comes from the argument, not the schema")
}
