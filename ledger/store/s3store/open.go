package s3store

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/okdaichi/qumo-ledger/ledger/store"
)

// Scheme is the URI scheme [store.Open] opens with this backend. The host is
// the bucket and the path the key prefix:
//
//	s3://bucket/prefix?region=ap-northeast-1
//	s3://bucket/prefix?region=us-east-1&endpoint=http://localhost:9000
//
// The region falls back to AWS_REGION and then AWS_DEFAULT_REGION. The
// credentials are read from AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY and
// AWS_SESSION_TOKEN.
const Scheme = "s3"

func init() {
	store.Register(Scheme, func(_ context.Context, u *url.URL) (store.Store, error) {
		cfg, err := configFromURI(u, os.Getenv)
		if err != nil {
			return nil, fmt.Errorf("s3store: %q: %w", u.Redacted(), err)
		}
		return New(cfg)
	})
}

// configFromURI builds a Config from an s3 URI and the environment.
func configFromURI(u *url.URL, getenv func(string) string) (Config, error) {
	if u.Host == "" {
		return Config{}, errors.New("an s3 URI names a bucket as its host")
	}
	if u.User != nil {
		return Config{}, errors.New("an s3 URI carries no credentials; set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY")
	}
	query := u.Query()
	region := query.Get("region")
	for _, name := range []string{"AWS_REGION", "AWS_DEFAULT_REGION"} {
		if region == "" {
			region = getenv(name)
		}
	}
	return Config{
		Bucket:          u.Host,
		Prefix:          strings.Trim(u.Path, "/"),
		Region:          region,
		Endpoint:        query.Get("endpoint"),
		AccessKeyID:     getenv("AWS_ACCESS_KEY_ID"),
		SecretAccessKey: getenv("AWS_SECRET_ACCESS_KEY"),
		SessionToken:    getenv("AWS_SESSION_TOKEN"),
	}, nil
}
