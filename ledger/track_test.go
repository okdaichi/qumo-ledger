package ledger

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTrackPath_Validate(t *testing.T) {
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
			err := tt.path.Validate()
			if tt.wantErr {
				assert.ErrorIs(t, err, ErrInvalidTrackPath)
				return
			}
			assert.NoError(t, err)
		})
	}
}

func TestTrackPath_Prefix(t *testing.T) {
	tests := map[string]struct {
		path     TrackPath
		dir      string
		expected bool
	}{
		"direct child":                    {path: "live/cam1/video", dir: "live/cam1", expected: true},
		"trailing slash on dir":           {path: "live/cam1/video", dir: "live/cam1/", expected: true},
		"grandparent":                     {path: "live/cam1/video", dir: "live", expected: true},
		"sibling":                         {path: "live/cam2/video", dir: "live/cam1", expected: false},
		"the track itself":                {path: "live/cam1", dir: "live/cam1", expected: false},
		"partial segment is not a prefix": {path: "live/cam10/video", dir: "live/cam1", expected: false},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.path.Prefix(tt.dir))
		})
	}
}

func TestTimeSource_Valid(t *testing.T) {
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
			assert.Equal(t, tt.expected, tt.source.Valid())
		})
	}
}

func TestTrackConfig_validate(t *testing.T) {
	tests := map[string]struct {
		config  TrackConfig
		wantErr bool
	}{
		"video": {
			config:  TrackConfig{Timescale: 90000, TimeSource: TimeSourceFrame},
			wantErr: false,
		},
		"zero timescale leaves timestamps dimensionless": {
			config:  TrackConfig{Timescale: 0, TimeSource: TimeSourceFrame},
			wantErr: true,
		},
		"unknown time source": {
			config:  TrackConfig{Timescale: 1000, TimeSource: "guess"},
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
