package crawler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
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

func newResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Body:       io.NopCloser(strings.NewReader(body)),
		Header:     make(http.Header),
	}
}

// fakeSite is an in-process, network-free stand-in for a website: it answers
// http.Client requests directly via RoundTrip, routing by URL path, and
// records how many times each URL was requested.
type fakeSite struct {
	mu       sync.Mutex
	calls    map[string]int
	handlers map[string]func(*http.Request) (*http.Response, error)
}

func newFakeSite() *fakeSite {
	return &fakeSite{
		calls:    make(map[string]int),
		handlers: make(map[string]func(*http.Request) (*http.Response, error)),
	}
}

func (s *fakeSite) handle(path string, fn func(*http.Request) (*http.Response, error)) {
	s.handlers[path] = fn
}

func (s *fakeSite) RoundTrip(r *http.Request) (*http.Response, error) {
	s.mu.Lock()
	s.calls[r.URL.String()]++
	s.mu.Unlock()

	fn, ok := s.handlers[r.URL.Path]
	if !ok {
		return newResponse(http.StatusNotFound, ""), nil
	}
	return fn(r)
}

func (s *fakeSite) callCount(rawURL string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls[rawURL]
}

func (s *fakeSite) client() *http.Client {
	return &http.Client{Transport: s}
}

func testOptions(rawURL string, client *http.Client) Options {
	opts := NewOptions(rawURL)
	opts.Delay = 0
	opts.Retries = 0
	opts.Timeout = 2 * time.Second
	opts.Concurrency = 3
	opts.HTTPClient = client
	return opts
}

const htmlTemplate = `<html><body>%s</body></html>`

func TestAnalyze_CrawlsSameHostOnly(t *testing.T) {
	site := newFakeSite()
	site.handle("/", func(r *http.Request) (*http.Response, error) {
		return newResponse(http.StatusOK, fmt.Sprintf(htmlTemplate, `
			<a href="/about">About</a>
			<a href="/about?ref=1">About with query</a>
			<a href="/missing">Missing</a>
			<a href="http://external.test/">External</a>
		`)), nil
	})
	site.handle("/about", func(r *http.Request) (*http.Response, error) {
		return newResponse(http.StatusOK, fmt.Sprintf(htmlTemplate, "about page")), nil
	})

	opts := testOptions("http://fake.test/", site.client())
	opts.Depth = 1

	data, err := Analyze(context.Background(), opts)
	require.NoError(t, err)

	var report Report
	require.NoError(t, json.Unmarshal(data, &report))

	require.Equal(t, "http://fake.test/", report.RootURL)
	require.Len(t, report.Pages, 4) // root, /about, /about?ref=1, /missing

	byURL := make(map[string]Page)
	for _, p := range report.Pages {
		byURL[p.URL] = p
	}
	require.Equal(t, "ok", byURL["http://fake.test/"].Status)
	require.Equal(t, "ok", byURL["http://fake.test/about"].Status)
	require.Equal(t, "ok", byURL["http://fake.test/about?ref=1"].Status)
	require.Equal(t, "error", byURL["http://fake.test/missing"].Status)
	require.Equal(t, http.StatusNotFound, byURL["http://fake.test/missing"].HTTPStatus)

	// External links are still probed to check whether they are broken, but
	// they must never turn into a Page of their own (i.e. never crawled).
	require.Equal(t, 1, site.callCount("http://external.test/"))
	require.NotContains(t, byURL, "http://external.test/")
}

func TestAnalyze_DepthZeroFetchesOnlyRoot(t *testing.T) {
	site := newFakeSite()
	site.handle("/", func(r *http.Request) (*http.Response, error) {
		return newResponse(http.StatusOK, fmt.Sprintf(htmlTemplate, `<a href="/about">About</a>`)), nil
	})

	opts := testOptions("http://fake.test/", site.client())
	opts.Depth = 0

	data, err := Analyze(context.Background(), opts)
	require.NoError(t, err)

	var report Report
	require.NoError(t, json.Unmarshal(data, &report))
	require.Len(t, report.Pages, 1)
	require.Equal(t, "http://fake.test/", report.Pages[0].URL)
	// /about is never crawled as its own page (depth exceeded), but it is
	// still probed once to check whether it is a broken link.
	require.Equal(t, 1, site.callCount("http://fake.test/about"))
}

func TestRun_DedupesRepeatedLinks(t *testing.T) {
	site := newFakeSite()
	site.handle("/", func(r *http.Request) (*http.Response, error) {
		return newResponse(http.StatusOK, fmt.Sprintf(htmlTemplate, `
			<a href="/about">1</a>
			<a href="/about">2</a>
			<a href="/about">3</a>
		`)), nil
	})
	site.handle("/about", func(r *http.Request) (*http.Response, error) {
		return newResponse(http.StatusOK, "about"), nil
	})

	opts := testOptions("http://fake.test/", site.client())
	opts.Depth = 1

	report, err := NewCrawler(opts).Run(context.Background())
	require.NoError(t, err)
	require.Len(t, report.Pages, 2) // root + /about, deduped
	// One call from crawling /about as a page, one from probing it as a
	// link on the root page; the 3 duplicate hrefs must not multiply either.
	require.Equal(t, 2, site.callCount("http://fake.test/about"))
}

func TestCrawl_RetriesOnNetworkFailure(t *testing.T) {
	site := newFakeSite()
	site.handle("/", func(r *http.Request) (*http.Response, error) {
		if site.callCount(r.URL.String()) <= 2 {
			return nil, errors.New("connection refused")
		}
		return newResponse(http.StatusOK, "root"), nil
	})

	opts := testOptions("http://fake.test/", site.client())
	opts.Retries = 2

	report, err := NewCrawler(opts).Run(context.Background())
	require.NoError(t, err)
	require.Len(t, report.Pages, 1)
	require.Equal(t, "ok", report.Pages[0].Status)
	require.Equal(t, 3, site.callCount("http://fake.test/"))
}

func TestCrawl_NotFoundStopsImmediately(t *testing.T) {
	site := newFakeSite()
	site.handle("/", func(r *http.Request) (*http.Response, error) {
		return newResponse(http.StatusNotFound, ""), nil
	})

	opts := testOptions("http://fake.test/", site.client())
	opts.Retries = 3

	report, err := NewCrawler(opts).Run(context.Background())
	require.NoError(t, err)
	require.Len(t, report.Pages, 1)
	require.Equal(t, "error", report.Pages[0].Status)
	require.Equal(t, http.StatusNotFound, report.Pages[0].HTTPStatus)
	require.Equal(t, 1, site.callCount("http://fake.test/"))
}

func TestCrawl_ServerErrorStopsImmediately(t *testing.T) {
	site := newFakeSite()
	site.handle("/", func(r *http.Request) (*http.Response, error) {
		return newResponse(http.StatusInternalServerError, ""), nil
	})

	opts := testOptions("http://fake.test/", site.client())
	opts.Retries = 3

	report, err := NewCrawler(opts).Run(context.Background())
	require.NoError(t, err)
	require.Len(t, report.Pages, 1)
	require.Equal(t, "error", report.Pages[0].Status)
	require.Equal(t, http.StatusInternalServerError, report.Pages[0].HTTPStatus)
	require.Equal(t, 1, site.callCount("http://fake.test/"))
}

func TestCrawl_BrokenLinks_OnlyBrokenOnesAreReported(t *testing.T) {
	site := newFakeSite()
	site.handle("/", func(r *http.Request) (*http.Response, error) {
		return newResponse(http.StatusOK, fmt.Sprintf(htmlTemplate, `
			<a href="/ok">Working link</a>
			<a href="/ghost">Broken link</a>
		`)), nil
	})
	site.handle("/ok", func(r *http.Request) (*http.Response, error) {
		return newResponse(http.StatusOK, "fine"), nil
	})
	site.handle("/ghost", func(r *http.Request) (*http.Response, error) {
		return newResponse(http.StatusNotFound, ""), nil
	})

	opts := testOptions("http://fake.test/", site.client())
	opts.Depth = 0 // only check links on the root page, do not crawl them

	report, err := NewCrawler(opts).Run(context.Background())
	require.NoError(t, err)
	require.Len(t, report.Pages, 1)

	root := report.Pages[0]
	require.Equal(t, "ok", root.Status)
	require.Len(t, root.BrokenLinks, 1)
	require.Equal(t, "http://fake.test/ghost", root.BrokenLinks[0].URL)
	require.Equal(t, http.StatusNotFound, root.BrokenLinks[0].StatusCode)
	require.Empty(t, root.BrokenLinks[0].Error)
}

func TestCrawl_BrokenLinks_NetworkFailureIsReported(t *testing.T) {
	site := newFakeSite()
	site.handle("/", func(r *http.Request) (*http.Response, error) {
		return newResponse(http.StatusOK, fmt.Sprintf(htmlTemplate, `<a href="http://unreachable.test/app.js">External</a>`)), nil
	})
	site.handle("/app.js", func(r *http.Request) (*http.Response, error) {
		return nil, errors.New("dial tcp: lookup unreachable.test: no such host")
	})

	opts := testOptions("http://fake.test/", site.client())
	opts.Depth = 1

	report, err := NewCrawler(opts).Run(context.Background())
	require.NoError(t, err)
	require.Len(t, report.Pages, 1) // external host is checked, but never crawled as a page

	root := report.Pages[0]
	require.Len(t, root.BrokenLinks, 1)
	require.Equal(t, "http://unreachable.test/app.js", root.BrokenLinks[0].URL)
	require.Zero(t, root.BrokenLinks[0].StatusCode)
	require.Contains(t, root.BrokenLinks[0].Error, "no such host")
}

func TestCrawl_BrokenLinks_IgnoresUnsupportedSchemesAndEmptyHrefs(t *testing.T) {
	site := newFakeSite()
	site.handle("/", func(r *http.Request) (*http.Response, error) {
		return newResponse(http.StatusOK, fmt.Sprintf(htmlTemplate, `
			<a href="mailto:someone@example.com">Mail</a>
			<a href="javascript:void(0)">JS</a>
			<a href="">Empty</a>
			<a href="/ok">Working link</a>
		`)), nil
	})
	site.handle("/ok", func(r *http.Request) (*http.Response, error) {
		return newResponse(http.StatusOK, "fine"), nil
	})

	opts := testOptions("http://fake.test/", site.client())
	opts.Depth = 0

	report, err := NewCrawler(opts).Run(context.Background())
	require.NoError(t, err)
	require.Len(t, report.Pages, 1)
	require.Empty(t, report.Pages[0].BrokenLinks)
}

func TestCrawl_BrokenLinks_UsesHeadAndFallsBackToGet(t *testing.T) {
	site := newFakeSite()
	site.handle("/", func(r *http.Request) (*http.Response, error) {
		return newResponse(http.StatusOK, fmt.Sprintf(htmlTemplate, `<a href="/no-head">No HEAD support</a>`)), nil
	})
	site.handle("/no-head", func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodHead {
			return newResponse(http.StatusMethodNotAllowed, ""), nil
		}
		return newResponse(http.StatusOK, "fine via GET"), nil
	})

	opts := testOptions("http://fake.test/", site.client())
	opts.Depth = 0

	report, err := NewCrawler(opts).Run(context.Background())
	require.NoError(t, err)
	require.Empty(t, report.Pages[0].BrokenLinks)
}

func TestCrawl_Timeout(t *testing.T) {
	site := newFakeSite()
	site.handle("/", func(r *http.Request) (*http.Response, error) {
		<-r.Context().Done()
		return nil, r.Context().Err()
	})

	opts := testOptions("http://fake.test/", site.client())
	opts.Timeout = 20 * time.Millisecond
	opts.Retries = 0

	report, err := NewCrawler(opts).Run(context.Background())
	require.NoError(t, err)
	require.Len(t, report.Pages, 1)
	require.Equal(t, "error", report.Pages[0].Status)
	require.Equal(t, 0, report.Pages[0].HTTPStatus)
	require.Contains(t, report.Pages[0].Error, "deadline exceeded")
}
