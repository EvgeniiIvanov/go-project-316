package crawler

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNormalizeURL(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"lowercases scheme and host", "HTTP://Example.COM/Path", "http://example.com/Path"},
		{"strips fragment", "http://example.com/path#section", "http://example.com/path"},
		{"empty path becomes slash", "http://example.com", "http://example.com/"},
		{"keeps query untouched", "http://example.com/path?a=1", "http://example.com/path?a=1"},
		{"invalid url returned unchanged", "http://[::1", "http://[::1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, normalizeURL(tc.in))
		})
	}
}

func TestOptionsValidate(t *testing.T) {
	cases := []struct {
		name    string
		opts    Options
		wantErr string
	}{
		{"valid options", Options{URL: "http://x", Concurrency: 1, Depth: 0}, ""},
		{"missing url", Options{URL: "", Concurrency: 1}, "URL is required"},
		{"zero concurrency", Options{URL: "http://x", Concurrency: 0}, "concurrency must be at least 1"},
		{"negative depth", Options{URL: "http://x", Concurrency: 1, Depth: -1}, "depth cannot be negative"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.opts.Validate()
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.EqualError(t, err, tc.wantErr)
		})
	}
}

func TestNewOptionsDefaults(t *testing.T) {
	opts := NewOptions("http://example.com")
	require.Equal(t, DefaultDepth, opts.Depth)
	require.Equal(t, DefaultRetries, opts.Retries)
	require.Equal(t, DefaultDelay, opts.Delay)
	require.Equal(t, DefaultTimeout, opts.Timeout)
	require.Equal(t, DefaultUserAgent, opts.UserAgent)
	require.Equal(t, DefaultConcurrency, opts.Concurrency)
	require.Equal(t, DefaultIndentJSON, opts.IndentJSON)
}

// newTestSite starts a local server whose root page links to /about (twice,
// once with a query string so both are distinct per normalizeURL), /missing
// (a 404), and to externalURL (a different host, which must not be followed).
func newTestSite(t *testing.T, externalURL string) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprintf(w, `<html><body>
			<a href="/about">About</a>
			<a href="/about?ref=1">About with query</a>
			<a href="/missing">Missing</a>
			<a href="%s">External</a>
		</body></html>`, externalURL)
	})
	mux.HandleFunc("/about", func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, `<html><body>about page</body></html>`)
	})
	mux.HandleFunc("/missing", func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func testOptions(url string) Options {
	opts := NewOptions(url)
	opts.Delay = 0
	opts.Retries = 0
	opts.Timeout = 2 * time.Second
	opts.Concurrency = 3
	return opts
}

func TestAnalyze_CrawlsSameHostOnly(t *testing.T) {
	external := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("external host must never be requested")
	}))
	defer external.Close()

	site := newTestSite(t, external.URL)
	opts := testOptions(site.URL)
	opts.Depth = 1

	data, err := Analyze(context.Background(), opts)
	require.NoError(t, err)

	var report Report
	require.NoError(t, json.Unmarshal(data, &report))

	require.Equal(t, site.URL+"/", report.RootURL)
	require.Len(t, report.Pages, 4) // root, /about, /about?ref=1, /missing

	byURL := make(map[string]Page)
	for _, p := range report.Pages {
		byURL[p.URL] = p
	}
	require.Equal(t, "ok", byURL[site.URL+"/"].Status)
	require.Equal(t, "ok", byURL[site.URL+"/about"].Status)
	require.Equal(t, "ok", byURL[site.URL+"/about?ref=1"].Status)
	require.Equal(t, "error", byURL[site.URL+"/missing"].Status)
	require.Equal(t, http.StatusNotFound, byURL[site.URL+"/missing"].HTTPStatus)
}

func TestAnalyze_DepthZeroFetchesOnlyRoot(t *testing.T) {
	site := newTestSite(t, "http://example-external.invalid/")
	opts := testOptions(site.URL)
	opts.Depth = 0

	data, err := Analyze(context.Background(), opts)
	require.NoError(t, err)

	var report Report
	require.NoError(t, json.Unmarshal(data, &report))
	require.Len(t, report.Pages, 1)
	require.Equal(t, site.URL+"/", report.Pages[0].URL)
}

func TestRun_DedupesRepeatedLinks(t *testing.T) {
	var aboutHits int32
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, `<html><body>
			<a href="/about">1</a>
			<a href="/about">2</a>
			<a href="/about">3</a>
		</body></html>`)
	})
	mux.HandleFunc("/about", func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&aboutHits, 1)
		_, _ = fmt.Fprint(w, `about`)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	opts := testOptions(srv.URL)
	opts.Depth = 1

	report, err := NewCrawler(opts).Run(context.Background())
	require.NoError(t, err)
	require.Len(t, report.Pages, 2) // root + /about, deduped
	require.EqualValues(t, 1, atomic.LoadInt32(&aboutHits))
}

func TestFetch_RetriesUntilSuccess(t *testing.T) {
	// Only transport-level failures (no HTTP response at all) are retried;
	// a received non-2xx status is treated as final. So to exercise the
	// retry path we hijack and abruptly close the connection for the first
	// two attempts, which surfaces as a client-side error with status 0.
	var attempts int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&attempts, 1) <= 2 {
			hj, ok := w.(http.Hijacker)
			require.True(t, ok)
			conn, _, err := hj.Hijack()
			require.NoError(t, err)
			_ = conn.Close()
			return
		}
		_, _ = fmt.Fprint(w, "ok")
	}))
	defer srv.Close()

	opts := testOptions(srv.URL)
	opts.Retries = 2
	c := NewCrawler(opts)

	body, status, err := c.fetch(context.Background(), srv.URL)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, status)
	require.Equal(t, "ok", string(body))
	require.EqualValues(t, 3, atomic.LoadInt32(&attempts))
}

func TestFetch_NonRetryableStatusStopsImmediately(t *testing.T) {
	var attempts int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&attempts, 1)
		http.NotFound(w, r)
	}))
	defer srv.Close()

	opts := testOptions(srv.URL)
	opts.Retries = 3
	c := NewCrawler(opts)

	_, status, err := c.fetch(context.Background(), srv.URL)
	require.Error(t, err)
	require.Equal(t, http.StatusNotFound, status)
	require.EqualValues(t, 1, atomic.LoadInt32(&attempts))
}
