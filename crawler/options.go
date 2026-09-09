package crawler

import (
	"errors"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Default values for Options, used both by NewOptions and by the CLI flag
// definitions in cmd/hexlet-go-crawler, so there is exactly one place that
// defines what "default" means for this crawler.
const (
	DefaultDepth       = 2
	DefaultRetries     = 3
	DefaultDelay       = 1 * time.Second
	DefaultTimeout     = 5 * time.Second
	DefaultUserAgent   = "go-crawler/1.0"
	DefaultConcurrency = 5
	DefaultIndentJSON  = true
)

// retryBackoff is the fixed pause observed between a failed attempt and the
// next retry of the same request. It is independent of, and in addition to,
// any global Delay pacing: it exists so that retries of one request
// never fire back-to-back in a burst, even when no global rate limit is
// configured.
const retryBackoff = 100 * time.Millisecond

type Options struct {
	URL         string
	Depth       int
	Retries     int
	Delay       time.Duration
	Timeout     time.Duration
	UserAgent   string
	Concurrency int
	IndentJSON  bool
	HTTPClient  *http.Client
}

func NewOptions(rawURL string) Options {
	return Options{
		URL:         normalizeURL(rawURL),
		Depth:       DefaultDepth,
		Retries:     DefaultRetries,
		Delay:       DefaultDelay,
		Timeout:     DefaultTimeout,
		UserAgent:   DefaultUserAgent,
		Concurrency: DefaultConcurrency,
		IndentJSON:  DefaultIndentJSON,
	}
}

func normalizeURL(rawURL string) string {
	u, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return rawURL
	}
	u.Scheme = strings.ToLower(u.Scheme)
	u.Host = strings.ToLower(u.Host)
	u.Fragment = ""
	if u.Path == "" {
		u.Path = "/"
	}
	return u.String()
}

func (o Options) Client() *http.Client {
	if o.HTTPClient != nil {
		return o.HTTPClient
	}
	return &http.Client{}
}

func (o Options) Validate() error {
	if o.URL == "" {
		return errors.New("URL is required")
	}
	if o.Concurrency < 1 {
		return errors.New("concurrency must be at least 1")
	}
	if o.Depth < 0 {
		return errors.New("depth cannot be negative")
	}
	return nil
}

// interval returns the minimum spacing that must be observed between
// consecutive HTTP requests across the whole crawl. Delay is used as-is,
// including when it is zero, which means no throttling at all. Converting
// a target requests-per-second rate into Delay (1s/RPS) is the caller's
// responsibility, e.g. the CLI does this before constructing Options.
func (o Options) interval() time.Duration {
	return o.Delay
}
