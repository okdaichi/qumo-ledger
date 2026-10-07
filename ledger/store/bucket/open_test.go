package bucket

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigFromURI(t *testing.T) {
	env := map[string]string{
		"AWS_DEFAULT_REGION":    "ap-northeast-1",
		"AWS_ACCESS_KEY_ID":     "id",
		"AWS_SECRET_ACCESS_KEY": "secret",
	}
	u, err := url.Parse("s3://ledger/tenant/a/?endpoint=http://localhost:9000")
	require.NoError(t, err)

	cfg, err := configFromURI(u, func(name string) string { return env[name] })

	require.NoError(t, err)
	assert.Equal(t, Config{
		Bucket:          "ledger",
		Prefix:          "tenant/a",
		Region:          "ap-northeast-1",
		Endpoint:        "http://localhost:9000",
		AccessKeyID:     "id",
		SecretAccessKey: "secret",
	}, cfg)
}

func TestConfigFromURI_RegionInTheURIWins(t *testing.T) {
	u, err := url.Parse("s3://ledger?region=us-west-2")
	require.NoError(t, err)

	cfg, err := configFromURI(u, func(string) string { return "ap-northeast-1" })

	require.NoError(t, err)
	assert.Equal(t, "us-west-2", cfg.Region)
}

func TestConfigFromURI_Rejected(t *testing.T) {
	for name, uri := range map[string]string{
		"no bucket":   "s3:///prefix",
		"credentials": "s3://id:secret@ledger/prefix",
	} {
		t.Run(name, func(t *testing.T) {
			u, err := url.Parse(uri)
			require.NoError(t, err)

			_, err = configFromURI(u, func(string) string { return "" })

			assert.Error(t, err)
		})
	}
}
