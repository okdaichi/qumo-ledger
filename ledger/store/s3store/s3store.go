// Package s3store provides a [store.Store] over a bucket of Amazon S3 or an
// S3-compatible object store.
//
// Objects are stored under a key prefix in one bucket. Create and Swap are
// conditional PUTs (If-None-Match: * and If-Match), so the service enforces
// them across processes, and an object's version is its ETag.
//
// It is a simple default: it speaks the S3 REST API directly with Signature
// Version 4 and static credentials, reads and writes objects whole, and
// addresses the bucket path-style when an endpoint is given.
package s3store

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"iter"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/okdaichi/qumo-ledger/ledger/store"
)

// Config locates a bucket and holds the credentials to sign requests with.
type Config struct {
	// Bucket is the bucket name. Required.
	Bucket string

	// Prefix is prepended to every key, so several ledgers can share a
	// bucket. Leading and trailing slashes are ignored.
	Prefix string

	// Region is the region requests are signed for. Required.
	Region string

	// Endpoint is the base URL of an S3-compatible service, such as
	// http://localhost:9000. Empty means Amazon S3 in Region.
	Endpoint string

	// AccessKeyID, SecretAccessKey and SessionToken are the credentials.
	// SessionToken is only set for temporary credentials.
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string

	// HTTPClient sends the requests. Nil means [http.DefaultClient].
	HTTPClient *http.Client
}

// conflictAttempts bounds how often a conditional PUT is retried after the
// service reports a concurrent conditional write to the same key.
const conflictAttempts = 3

// maxErrorBody bounds how much of an error response is kept for the error.
const maxErrorBody = 1 << 10

// Store keeps objects in a bucket.
type Store struct {
	client *http.Client
	bucket *url.URL
	prefix string
	signer signer
}

var (
	_ store.Store  = (*Store)(nil)
	_ store.Lister = (*Store)(nil)
)

// New returns a Store over the bucket cfg names. It sends no request.
func New(cfg Config) (*Store, error) {
	if cfg.Bucket == "" {
		return nil, errors.New("s3store: no bucket")
	}
	if cfg.Region == "" {
		return nil, errors.New("s3store: no region")
	}
	if cfg.AccessKeyID == "" || cfg.SecretAccessKey == "" {
		return nil, errors.New("s3store: no credentials")
	}

	var bucket *url.URL
	if cfg.Endpoint == "" {
		bucket = &url.URL{Scheme: "https", Host: cfg.Bucket + ".s3." + cfg.Region + ".amazonaws.com", Path: "/"}
	} else {
		endpoint, err := url.Parse(cfg.Endpoint)
		if err != nil || endpoint.Host == "" || (endpoint.Scheme != "http" && endpoint.Scheme != "https") {
			return nil, fmt.Errorf("s3store: invalid endpoint %q", cfg.Endpoint)
		}
		bucket = endpoint.JoinPath(cfg.Bucket)
		bucket.Path = "/" + strings.TrimPrefix(bucket.Path, "/") + "/"
		bucket.RawPath = ""
	}

	prefix := strings.Trim(cfg.Prefix, "/")
	if prefix != "" {
		prefix += "/"
	}
	client := cfg.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	return &Store{
		client: client,
		bucket: bucket,
		prefix: prefix,
		signer: signer{
			region:          cfg.Region,
			accessKeyID:     cfg.AccessKeyID,
			secretAccessKey: cfg.SecretAccessKey,
			sessionToken:    cfg.SessionToken,
		},
	}, nil
}

// Get implements [store.Store].
func (s *Store) Get(ctx context.Context, key string) ([]byte, store.Version, error) {
	resp, err := s.do(ctx, http.MethodGet, s.prefix+key, nil, nil, nil)
	if err != nil {
		return nil, store.NoVersion, fmt.Errorf("s3store: get %s: %w", key, err)
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusNotFound:
		return nil, store.NoVersion, fmt.Errorf("%w: %s", store.ErrNotExist, key)
	default:
		return nil, store.NoVersion, fmt.Errorf("s3store: get %s: %w", key, responseError(resp))
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, store.NoVersion, fmt.Errorf("s3store: get %s: %w", key, err)
	}
	return data, store.Version(resp.Header.Get("ETag")), nil
}

// Create implements [store.Store].
func (s *Store) Create(ctx context.Context, key string, data []byte) (store.Version, error) {
	version, status, err := s.put(ctx, key, data, http.Header{"If-None-Match": {"*"}})
	if err != nil {
		return store.NoVersion, fmt.Errorf("s3store: create %s: %w", key, err)
	}
	if status == http.StatusPreconditionFailed {
		return store.NoVersion, fmt.Errorf("%w: %s", store.ErrExist, key)
	}
	return version, nil
}

// Swap implements [store.Store].
func (s *Store) Swap(ctx context.Context, key string, data []byte, expect store.Version) (store.Version, error) {
	header := http.Header{"If-Match": {string(expect)}}
	if expect == store.NoVersion {
		header = http.Header{"If-None-Match": {"*"}}
	}
	version, status, err := s.put(ctx, key, data, header)
	if err != nil {
		return store.NoVersion, fmt.Errorf("s3store: swap %s: %w", key, err)
	}
	switch status {
	case http.StatusPreconditionFailed:
		return store.NoVersion, fmt.Errorf("%w: %s", store.ErrVersionMismatch, key)
	case http.StatusNotFound:
		return store.NoVersion, fmt.Errorf("%w: %s", store.ErrNotExist, key)
	}
	return version, nil
}

// Delete implements [store.Store].
func (s *Store) Delete(ctx context.Context, key string) error {
	resp, err := s.do(ctx, http.MethodDelete, s.prefix+key, nil, nil, nil)
	if err != nil {
		return fmt.Errorf("s3store: delete %s: %w", key, err)
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK, http.StatusNoContent, http.StatusNotFound:
		return nil
	}
	return fmt.Errorf("s3store: delete %s: %w", key, responseError(resp))
}

// List implements [store.Lister].
func (s *Store) List(ctx context.Context, prefix string) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		query := url.Values{"list-type": {"2"}, "prefix": {s.prefix + prefix}}
		for {
			page, err := s.listPage(ctx, query)
			if err != nil {
				yield("", fmt.Errorf("s3store: list %s: %w", prefix, err))
				return
			}
			for _, object := range page.Contents {
				if !yield(strings.TrimPrefix(object.Key, s.prefix), nil) {
					return
				}
			}
			if !page.IsTruncated || page.NextContinuationToken == "" {
				return
			}
			query.Set("continuation-token", page.NextContinuationToken)
		}
	}
}

// listResult is a ListObjectsV2 response.
type listResult struct {
	Contents []struct {
		Key string
	}
	IsTruncated           bool
	NextContinuationToken string
}

func (s *Store) listPage(ctx context.Context, query url.Values) (*listResult, error) {
	resp, err := s.do(ctx, http.MethodGet, "", query, nil, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, responseError(resp)
	}
	var page listResult
	if err := xml.NewDecoder(resp.Body).Decode(&page); err != nil {
		return nil, fmt.Errorf("decode list response: %w", err)
	}
	return &page, nil
}

// put sends a conditional PUT and returns the new version, or the status that
// refused it: 412 when the condition failed, 404 when If-Match found no object.
func (s *Store) put(ctx context.Context, key string, data []byte, header http.Header) (store.Version, int, error) {
	for attempt := 1; ; attempt++ {
		resp, err := s.do(ctx, http.MethodPut, s.prefix+key, nil, data, header)
		if err != nil {
			return store.NoVersion, 0, err
		}
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxErrorBody))
		resp.Body.Close()
		switch resp.StatusCode {
		case http.StatusOK:
			return store.Version(resp.Header.Get("ETag")), resp.StatusCode, nil
		case http.StatusPreconditionFailed, http.StatusNotFound:
			return store.NoVersion, resp.StatusCode, nil
		case http.StatusConflict:
			if attempt < conflictAttempts {
				continue
			}
		}
		return store.NoVersion, 0, fmt.Errorf("unexpected status %s", resp.Status)
	}
}

// do sends a signed request for the object at key, or for the bucket when key
// is empty and a query is given.
func (s *Store) do(ctx context.Context, method, key string, query url.Values, body []byte, header http.Header) (*http.Response, error) {
	if key == "" && query == nil {
		return nil, errors.New("empty key")
	}
	u := *s.bucket
	u.Path += key
	u.RawQuery = query.Encode()

	req, err := http.NewRequestWithContext(ctx, method, u.String(), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	// Go's URL parser unescapes the path; restore it exactly as written.
	req.URL.Path = u.Path
	req.ContentLength = int64(len(body))
	for name, values := range header {
		req.Header[name] = values
	}
	s.signer.sign(req, body, time.Now())
	return s.client.Do(req)
}

// responseError describes a response the store did not expect.
func responseError(resp *http.Response) error {
	var body struct {
		Code    string
		Message string
	}
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody))
	if xml.Unmarshal(raw, &body) == nil && body.Code != "" {
		return fmt.Errorf("%s: %s: %s", resp.Status, body.Code, body.Message)
	}
	return fmt.Errorf("unexpected status %s", resp.Status)
}
