package s3store

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"
)

// signer signs requests with AWS Signature Version 4 for the S3 service.
type signer struct {
	region          string
	accessKeyID     string
	secretAccessKey string
	sessionToken    string
}

const (
	signAlgorithm = "AWS4-HMAC-SHA256"
	signService   = "s3"
	amzDate       = "20060102T150405Z"
	amzDay        = "20060102"
)

// sign adds the headers that authenticate req with payload as its body. It
// rewrites the URL's escaping to the canonical form it signs.
func (s signer) sign(req *http.Request, payload []byte, now time.Time) {
	now = now.UTC()
	payloadHash := hashHex(payload)
	req.Header.Set("X-Amz-Date", now.Format(amzDate))
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	if s.sessionToken != "" {
		req.Header.Set("X-Amz-Security-Token", s.sessionToken)
	}

	path := escapePath(req.URL.Path)
	req.URL.RawPath = path
	query := canonicalQuery(req.URL.Query())
	req.URL.RawQuery = query
	host := req.Host
	if host == "" {
		host = req.URL.Host
	}

	headers := map[string]string{"host": host}
	for name, values := range req.Header {
		name = strings.ToLower(name)
		if strings.HasPrefix(name, "x-amz-") {
			headers[name] = strings.TrimSpace(strings.Join(values, ","))
		}
	}
	names := make([]string, 0, len(headers))
	for name := range headers {
		names = append(names, name)
	}
	slices.Sort(names)
	var canonicalHeaders strings.Builder
	for _, name := range names {
		canonicalHeaders.WriteString(name + ":" + headers[name] + "\n")
	}
	signedHeaders := strings.Join(names, ";")

	canonicalRequest := strings.Join([]string{
		req.Method,
		path,
		query,
		canonicalHeaders.String(),
		signedHeaders,
		payloadHash,
	}, "\n")
	scope := now.Format(amzDay) + "/" + s.region + "/" + signService + "/aws4_request"
	stringToSign := strings.Join([]string{
		signAlgorithm,
		now.Format(amzDate),
		scope,
		hashHex([]byte(canonicalRequest)),
	}, "\n")

	key := hmacSHA256([]byte("AWS4"+s.secretAccessKey), now.Format(amzDay))
	key = hmacSHA256(key, s.region)
	key = hmacSHA256(key, signService)
	key = hmacSHA256(key, "aws4_request")
	signature := hex.EncodeToString(hmacSHA256(key, stringToSign))

	req.Header.Set("Authorization", signAlgorithm+
		" Credential="+s.accessKeyID+"/"+scope+
		", SignedHeaders="+signedHeaders+
		", Signature="+signature)
}

// canonicalQuery encodes a query sorted by name and then value.
func canonicalQuery(query url.Values) string {
	pairs := make([]string, 0, len(query))
	for name, values := range query {
		for _, value := range values {
			pairs = append(pairs, escape(name, true)+"="+escape(value, true))
		}
	}
	slices.Sort(pairs)
	return strings.Join(pairs, "&")
}

// escapePath encodes a path as S3 signs it, keeping its slashes.
func escapePath(path string) string {
	if path == "" {
		return "/"
	}
	return escape(path, false)
}

// escape percent-encodes every byte except the unreserved characters, and
// except slashes unless encodeSlash is set.
func escape(s string, encodeSlash bool) string {
	const hexDigits = "0123456789ABCDEF"
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case 'A' <= c && c <= 'Z', 'a' <= c && c <= 'z', '0' <= c && c <= '9',
			c == '-', c == '_', c == '.', c == '~', c == '/' && !encodeSlash:
			b.WriteByte(c)
		default:
			b.WriteByte('%')
			b.WriteByte(hexDigits[c>>4])
			b.WriteByte(hexDigits[c&0xf])
		}
	}
	return b.String()
}

func hashHex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func hmacSHA256(key []byte, data string) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(data))
	return mac.Sum(nil)
}
