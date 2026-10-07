package s3store_test

import (
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/okdaichi/qumo-ledger/ledger/store"
	"github.com/okdaichi/qumo-ledger/ledger/store/s3store"
	"github.com/okdaichi/qumo-ledger/ledger/store/storetest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	bucket      = "ledger"
	accessKeyID = "test-access-key"
)

func TestStore_Conformance(t *testing.T) {
	storetest.Run(t, newFakeStore)
}

func TestStore_ListerConformance(t *testing.T) {
	storetest.RunLister(t, newFakeStore)
}

func TestStore_KeepsKeysUnderThePrefix(t *testing.T) {
	fake := &fakeS3{}
	s := newStore(t, fake, "tenant/a")

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

func TestStore_RetriesAConcurrentConditionalWrite(t *testing.T) {
	fake := &fakeS3{conflicts: 1}
	s := newStore(t, fake, "")

	_, err := s.Create(t.Context(), "key", []byte("value"))

	require.NoError(t, err)
	assert.Equal(t, []string{"key"}, fake.keys())
}

func TestNew_Rejected(t *testing.T) {
	valid := s3store.Config{Bucket: bucket, Region: "us-east-1", AccessKeyID: "id", SecretAccessKey: "secret"}
	tests := map[string]func(*s3store.Config){
		"no bucket":      func(c *s3store.Config) { c.Bucket = "" },
		"no region":      func(c *s3store.Config) { c.Region = "" },
		"no credentials": func(c *s3store.Config) { c.SecretAccessKey = "" },
		"a bad endpoint": func(c *s3store.Config) { c.Endpoint = "localhost:9000" },
	}
	for name, change := range tests {
		t.Run(name, func(t *testing.T) {
			cfg := valid
			change(&cfg)

			_, err := s3store.New(cfg)

			assert.Error(t, err)
		})
	}
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
	run := fmt.Sprintf("s3store-test-%d", time.Now().UnixNano())
	newLive := func(t *testing.T) store.Store {
		u := strings.Replace(uri, "?", fmt.Sprintf("/%s/%d?", run, liveCount.Add(1)), 1)
		s, err := store.Open(t.Context(), u)
		require.NoError(t, err)
		return s
	}
	storetest.Run(t, newLive)
	storetest.RunLister(t, func(t *testing.T) storetest.ListerStore {
		return newLive(t).(storetest.ListerStore)
	})
}

func newFakeStore(t *testing.T) *s3store.Store {
	return newStore(t, &fakeS3{}, "")
}

func newStore(t *testing.T, fake *fakeS3, prefix string) *s3store.Store {
	t.Helper()
	server := httptest.NewServer(fake)
	t.Cleanup(server.Close)
	s, err := s3store.New(s3store.Config{
		Bucket:          bucket,
		Prefix:          prefix,
		Region:          "us-east-1",
		Endpoint:        server.URL,
		AccessKeyID:     accessKeyID,
		SecretAccessKey: "test-secret",
		HTTPClient:      server.Client(),
	})
	require.NoError(t, err)
	return s
}

// fakeS3 serves one bucket with the parts of the S3 API the store uses. It
// checks that requests are signed but not the signatures themselves.
type fakeS3 struct {
	mu        sync.Mutex
	objects   map[string]fakeObject
	etags     int
	conflicts int // answer the next conditional PUTs with 409
}

type fakeObject struct {
	data []byte
	etag string
}

func (f *fakeS3) keys() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	keys := make([]string, 0, len(f.objects))
	for key := range f.objects {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !strings.HasPrefix(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256 Credential="+accessKeyID+"/") {
		http.Error(w, "unsigned", http.StatusForbidden)
		return
	}
	key, ok := strings.CutPrefix(r.URL.Path, "/"+bucket+"/")
	if !ok {
		http.NotFound(w, r)
		return
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if f.objects == nil {
		f.objects = map[string]fakeObject{}
	}
	object, exists := f.objects[key]

	switch {
	case r.Method == http.MethodGet && key == "":
		f.list(w, r)
	case r.Method == http.MethodGet:
		if !exists {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("ETag", object.etag)
		_, _ = w.Write(object.data)
	case r.Method == http.MethodPut:
		if f.conflicts > 0 {
			f.conflicts--
			w.WriteHeader(http.StatusConflict)
			return
		}
		if r.Header.Get("If-None-Match") == "*" && exists {
			w.WriteHeader(http.StatusPreconditionFailed)
			return
		}
		if match := r.Header.Get("If-Match"); match != "" {
			if !exists {
				http.NotFound(w, r)
				return
			}
			if match != object.etag {
				w.WriteHeader(http.StatusPreconditionFailed)
				return
			}
		}
		data, _ := io.ReadAll(r.Body)
		f.etags++
		etag := fmt.Sprintf(`"%d"`, f.etags)
		f.objects[key] = fakeObject{data: data, etag: etag}
		w.Header().Set("ETag", etag)
	case r.Method == http.MethodDelete:
		delete(f.objects, key)
		w.WriteHeader(http.StatusNoContent)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// list answers ListObjectsV2 two keys per page, so paging is exercised.
func (f *fakeS3) list(w http.ResponseWriter, r *http.Request) {
	const pageSize = 2
	prefix := r.URL.Query().Get("prefix")
	var keys []string
	for key := range f.objects {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	slices.Sort(keys)
	start := 0
	if token := r.URL.Query().Get("continuation-token"); token != "" {
		start, _ = slices.BinarySearch(keys, token)
	}
	type content struct{ Key string }
	result := struct {
		XMLName               xml.Name `xml:"ListBucketResult"`
		Contents              []content
		IsTruncated           bool
		NextContinuationToken string `xml:",omitempty"`
	}{}
	end := min(start+pageSize, len(keys))
	for _, key := range keys[start:end] {
		result.Contents = append(result.Contents, content{Key: key})
	}
	if end < len(keys) {
		result.IsTruncated = true
		result.NextContinuationToken = keys[end]
	}
	_ = xml.NewEncoder(w).Encode(result)
}
