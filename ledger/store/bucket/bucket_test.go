package bucket

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/okdaichi/qumo-ledger/ledger/store"
	"github.com/okdaichi/qumo-ledger/ledger/store/storetest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newStore returns a Store over a server running fake, with keys under
// prefix, and the config it was built from.
func newStore(tb testing.TB, fake *fakeS3, prefix string) *Store {
	tb.Helper()
	server := httptest.NewServer(fake)
	tb.Cleanup(server.Close)
	s, err := New(Config{
		Bucket:          fakeBucket,
		Prefix:          prefix,
		Region:          "us-east-1",
		Endpoint:        server.URL,
		AccessKeyID:     fakeAccessKeyID,
		SecretAccessKey: "test-secret",
		HTTPClient:      server.Client(),
	})
	require.NoError(tb, err)
	return s
}

func TestStore_Conformance(t *testing.T) {
	storetest.Run(t, func(t *testing.T) *Store { return newStore(t, &fakeS3{}, "") })
}

func TestStore_ListerConformance(t *testing.T) {
	storetest.RunLister(t, func(t *testing.T) *Store { return newStore(t, &fakeS3{}, "") })
}

func TestStore_KeepsKeysUnderThePrefix(t *testing.T) {
	fake := &fakeS3{}
	s := newStore(t, fake, "/tenant/a/")

	_, err := s.Create(t.Context(), "room/1/chat/manifest", []byte("m"))
	require.NoError(t, err)

	assert.Equal(t, []string{"tenant/a/room/1/chat/manifest"}, fake.keys())
	var listed []string
	for key, err := range s.List(t.Context(), "room/") {
		require.NoError(t, err)
		listed = append(listed, key)
	}
	assert.Equal(t, []string{"room/1/chat/manifest"}, listed)
}

func TestStore_ConcurrentConditionalWrite(t *testing.T) {
	tests := map[string]struct {
		conflicts int
		wantErr   bool
	}{
		"one conflict is retried":           {conflicts: 1},
		"conflicts up to the limit succeed": {conflicts: conflictAttempts - 1},
		"conflicts past the limit fail":     {conflicts: conflictAttempts, wantErr: true},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			fake := &fakeS3{conflicts: tt.conflicts}
			s := newStore(t, fake, "")

			_, err := s.Create(t.Context(), "key", []byte("value"))

			if tt.wantErr {
				assert.ErrorContains(t, err, "409")
				assert.Empty(t, fake.keys())
				return
			}
			require.NoError(t, err)
			assert.Equal(t, []string{"key"}, fake.keys())
		})
	}
}

func TestStore_ServiceErrors(t *testing.T) {
	const s3Error = `<?xml version="1.0"?><Error><Code>SlowDown</Code><Message>Please reduce your request rate.</Message></Error>`
	tests := map[string]struct {
		status  int
		body    string
		call    func(ctx context.Context, s *Store) error
		wantMsg string
	}{
		"get with an S3 error": {
			status: http.StatusServiceUnavailable, body: s3Error,
			call:    func(ctx context.Context, s *Store) error { _, _, err := s.Get(ctx, "key"); return err },
			wantMsg: "SlowDown: Please reduce your request rate.",
		},
		"get with a bare status": {
			status:  http.StatusInternalServerError,
			call:    func(ctx context.Context, s *Store) error { _, _, err := s.Get(ctx, "key"); return err },
			wantMsg: "500 Internal Server Error",
		},
		"create": {
			status:  http.StatusForbidden,
			call:    func(ctx context.Context, s *Store) error { _, err := s.Create(ctx, "key", nil); return err },
			wantMsg: "403 Forbidden",
		},
		"swap": {
			status: http.StatusInternalServerError,
			call: func(ctx context.Context, s *Store) error {
				_, err := s.Swap(ctx, "key", nil, store.Version(`"1"`))
				return err
			},
			wantMsg: "500 Internal Server Error",
		},
		"delete": {
			status: http.StatusServiceUnavailable, body: s3Error,
			call:    func(ctx context.Context, s *Store) error { return s.Delete(ctx, "key") },
			wantMsg: "SlowDown",
		},
		"list": {
			status: http.StatusForbidden, body: `<Error><Code>AccessDenied</Code><Message>no</Message></Error>`,
			call: func(ctx context.Context, s *Store) error {
				for _, err := range s.List(ctx, "") {
					return err
				}
				return nil
			},
			wantMsg: "AccessDenied",
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			s := newStore(t, &fakeS3{status: tt.status, body: tt.body}, "")

			err := tt.call(t.Context(), s)

			require.Error(t, err)
			assert.ErrorContains(t, err, tt.wantMsg)
			assert.NotErrorIs(t, err, store.ErrNotExist)
		})
	}
}

func TestStore_List_MalformedResponse(t *testing.T) {
	s := newStore(t, &fakeS3{listBody: "<ListBucketResult><Contents>"}, "")

	var errs []error
	for _, err := range s.List(t.Context(), "") {
		errs = append(errs, err)
	}

	require.Len(t, errs, 1)
	assert.ErrorContains(t, errs[0], "decode list response")
}

func TestStore_List_StopsWhenTheCallerDoes(t *testing.T) {
	fake := &fakeS3{}
	s := newStore(t, fake, "")
	for i := range 5 {
		_, err := s.Create(t.Context(), fmt.Sprintf("k%d", i), nil)
		require.NoError(t, err)
	}

	var seen []string
	for key, err := range s.List(t.Context(), "") {
		require.NoError(t, err)
		seen = append(seen, key)
		if len(seen) == 3 {
			break
		}
	}

	assert.Equal(t, []string{"k0", "k1", "k2"}, seen, "breaking mid-page and across a page boundary is safe")
}

func TestStore_EmptyKeyIsRefused(t *testing.T) {
	s := newStore(t, &fakeS3{}, "")

	_, _, err := s.Get(t.Context(), "")

	assert.ErrorContains(t, err, "empty key")
}

func TestStore_SendsTheSessionToken(t *testing.T) {
	fake := &fakeS3{}
	server := httptest.NewServer(fake)
	t.Cleanup(server.Close)
	s, err := New(Config{
		Bucket: fakeBucket, Region: "us-east-1", Endpoint: server.URL,
		AccessKeyID: fakeAccessKeyID, SecretAccessKey: "secret", SessionToken: "session-token",
	})
	require.NoError(t, err)

	_, err = s.Create(t.Context(), "key", []byte("value"))

	require.NoError(t, err)
	assert.Equal(t, []string{"session-token"}, fake.sessionTokens())
}

func TestNew(t *testing.T) {
	valid := Config{Bucket: fakeBucket, Region: "us-east-1", AccessKeyID: "id", SecretAccessKey: "secret"}
	tests := map[string]struct {
		change     func(*Config)
		wantBucket string
		wantPrefix string
		wantErr    bool
	}{
		"amazon s3":                {change: func(*Config) {}, wantBucket: "https://ledger.s3.us-east-1.amazonaws.com/"},
		"an endpoint":              {change: func(c *Config) { c.Endpoint = "http://localhost:9000" }, wantBucket: "http://localhost:9000/ledger/"},
		"an endpoint with a path":  {change: func(c *Config) { c.Endpoint = "https://s3.example/storage/" }, wantBucket: "https://s3.example/storage/ledger/"},
		"a prefix is one segment":  {change: func(c *Config) { c.Prefix = "/a/b/" }, wantBucket: "https://ledger.s3.us-east-1.amazonaws.com/", wantPrefix: "a/b/"},
		"no bucket":                {change: func(c *Config) { c.Bucket = "" }, wantErr: true},
		"no region":                {change: func(c *Config) { c.Region = "" }, wantErr: true},
		"no access key":            {change: func(c *Config) { c.AccessKeyID = "" }, wantErr: true},
		"no secret":                {change: func(c *Config) { c.SecretAccessKey = "" }, wantErr: true},
		"an endpoint with no host": {change: func(c *Config) { c.Endpoint = "localhost:9000" }, wantErr: true},
		"an endpoint not http":     {change: func(c *Config) { c.Endpoint = "ftp://localhost" }, wantErr: true},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			cfg := valid
			tt.change(&cfg)

			s, err := New(cfg)

			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, s)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantBucket, s.bucket.String())
			assert.Equal(t, tt.wantPrefix, s.prefix)
			assert.Same(t, http.DefaultClient, s.client)
		})
	}
}

func TestOpen_RegistersTheS3Scheme(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "id")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "secret")
	t.Setenv("AWS_SESSION_TOKEN", "")
	t.Setenv("AWS_REGION", "")
	t.Setenv("AWS_DEFAULT_REGION", "")

	s, err := store.Open(t.Context(), "s3://ledger/tenant?region=ap-northeast-1&endpoint=http://localhost:9000")
	require.NoError(t, err)
	require.IsType(t, &Store{}, s)
	assert.Equal(t, "http://localhost:9000/ledger/", s.(*Store).bucket.String())
	assert.Equal(t, "tenant/", s.(*Store).prefix)

	_, err = store.Open(t.Context(), "s3://ledger/tenant")
	assert.ErrorContains(t, err, "no region")
}

// liveURIEnv names an s3 URI of a real S3-compatible bucket, with credentials
// in the AWS_* variables. The conformance suite also runs against it when set.
const liveURIEnv = "S3STORE_TEST_URI"

var liveCount atomic.Int64

func TestStore_LiveConformance(t *testing.T) {
	uri := os.Getenv(liveURIEnv)
	if uri == "" {
		t.Skipf("%s is not set", liveURIEnv)
	}
	run := fmt.Sprintf("bucket-test-%d", time.Now().UnixNano())
	newLive := func(t *testing.T) storetest.ListerStore {
		u := strings.Replace(uri, "?", fmt.Sprintf("/%s/%d?", run, liveCount.Add(1)), 1)
		s, err := store.Open(t.Context(), u)
		require.NoError(t, err)
		return s.(storetest.ListerStore)
	}
	storetest.Run(t, newLive)
	storetest.RunLister(t, newLive)
}
