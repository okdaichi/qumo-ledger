package s3store

import (
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"
)

// fakeBucket is the bucket fakeS3 serves, and fakeAccessKeyID the access key it
// expects requests to be signed with.
const (
	fakeBucket      = "ledger"
	fakeAccessKeyID = "test-access-key"
)

// fakeS3 serves one bucket with the parts of the S3 API the store uses. It
// checks that requests are signed but not the signatures themselves. The zero
// value is an empty, working bucket.
type fakeS3 struct {
	// conflicts answers the next conditional PUTs with 409.
	conflicts int
	// status, when set, answers every request with it and body.
	status int
	body   string
	// listBody, when set, answers every listing with it.
	listBody string

	mu      sync.Mutex
	objects map[string]fakeObject
	etags   int
	// tokens records the X-Amz-Security-Token of each request.
	tokens []string
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

func (f *fakeS3) sessionTokens() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.tokens)
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !strings.HasPrefix(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256 Credential="+fakeAccessKeyID+"/") {
		http.Error(w, "unsigned", http.StatusForbidden)
		return
	}
	key, ok := strings.CutPrefix(r.URL.Path, "/"+fakeBucket+"/")
	if !ok {
		http.NotFound(w, r)
		return
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	f.tokens = append(f.tokens, r.Header.Get("X-Amz-Security-Token"))
	if f.status != 0 {
		w.WriteHeader(f.status)
		_, _ = io.WriteString(w, f.body) // not actionable: the client reads what it gets
		return
	}
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
		_, _ = w.Write(object.data) // not actionable: the client reads what it gets
	case r.Method == http.MethodPut:
		f.put(w, r, key, object, exists)
	case r.Method == http.MethodDelete:
		delete(f.objects, key)
		w.WriteHeader(http.StatusNoContent)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (f *fakeS3) put(w http.ResponseWriter, r *http.Request, key string, object fakeObject, exists bool) {
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
	data, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	f.etags++
	etag := fmt.Sprintf(`"%d"`, f.etags)
	f.objects[key] = fakeObject{data: data, etag: etag}
	w.Header().Set("ETag", etag)
}

// list answers ListObjectsV2 two keys per page, so paging is exercised.
func (f *fakeS3) list(w http.ResponseWriter, r *http.Request) {
	if f.listBody != "" {
		_, _ = io.WriteString(w, f.listBody) // not actionable: the client reads what it gets
		return
	}
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
	_ = xml.NewEncoder(w).Encode(result) // not actionable: the client reads what it gets
}
