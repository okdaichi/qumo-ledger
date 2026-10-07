package bucket

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The credentials and the expected signatures are the worked examples of the
// Amazon S3 Signature Version 4 documentation.
func TestSigner_MatchesAWSExamples(t *testing.T) {
	s := signer{
		region:          "us-east-1",
		accessKeyID:     "AKIAIOSFODNN7EXAMPLE",
		secretAccessKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
	}
	now := time.Date(2013, time.May, 24, 0, 0, 0, 0, time.UTC)
	tests := map[string]struct {
		url       string
		signature string
	}{
		"bucket lifecycle": {
			url:       "https://examplebucket.s3.amazonaws.com/?lifecycle",
			signature: "fea454ca298b7da1c68078a5d1bdbfbbe0d65c699e0f91ac7a200a0136783543",
		},
		"list objects": {
			url:       "https://examplebucket.s3.amazonaws.com/?max-keys=2&prefix=J",
			signature: "34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7",
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodGet, tt.url, nil)
			require.NoError(t, err)

			s.sign(req, nil, now)

			assert.Equal(t, "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, "+
				"SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature="+tt.signature,
				req.Header.Get("Authorization"))
		})
	}
}

func TestEscape(t *testing.T) {
	assert.Equal(t, "/room/123/chat/a%20b~-_.%2B", escapePath("/room/123/chat/a b~-_.+"))
	assert.Equal(t, "a%2Fb", escape("a/b", true))
	assert.Equal(t, "/", escapePath(""))
}
