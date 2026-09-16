package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
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
		{"empty path left untouched", "http://example.com", "http://example.com"},
		{"keeps query untouched", "http://example.com/path?a=1", "http://example.com/path?a=1"},
		{"invalid url returned unchanged", "http://[::1", "http://[::1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, normalizeURL(tc.in))
		})
	}
}

func TestOptionsInterval(t *testing.T) {
	cases := []struct {
		name  string
		delay time.Duration
		want  time.Duration
	}{
		{"zero means no throttling", 0, 0},
		{"positive delay is used as-is", 200 * time.Millisecond, 200 * time.Millisecond},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts := Options{Delay: tc.delay}
			require.Equal(t, tc.want, opts.interval())
		})
	}
}

func newResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Body:       io.NopCloser(strings.NewReader(body)),
		Header:     make(http.Header),
	}
}

// newResponseWithContentLength builds a response the same way newResponse
// does, but also sets ContentLength: since fakeSite hands *http.Response
// values back directly (bypassing the real transport's header parsing),
// tests that care about Content-Length handling must set this field
// themselves. Pass -1 to simulate a response with no Content-Length header.
func newResponseWithContentLength(status int, body string, contentLength int64) *http.Response {
	resp := newResponse(status, body)
	resp.ContentLength = contentLength
	return resp
}

// fakeSite is an in-process, network-free stand-in for a website: it answers
// http.Client requests directly via RoundTrip, routing by URL path, and
// records how many times each URL was requested.
type fakeSite struct {
	mu       sync.Mutex
	calls    map[string]int
	times    []time.Time
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
	s.times = append(s.times, time.Now())
	s.mu.Unlock()

	fn, ok := s.handlers[r.URL.Path]
	if !ok {
		return newResponse(http.StatusNotFound, ""), nil
	}
	return fn(r)
}

// requestTimes returns the wall-clock time at which each request was
// received, in the order RoundTrip observed them. Used to verify global
// pacing (Delay) across all workers.
func (s *fakeSite) requestTimes() []time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]time.Time, len(s.times))
	copy(out, s.times)
	return out
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
	opts.Depth = 2 // root, plus its direct links

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

func TestCrawl_ServerErrorRetriesThenFails(t *testing.T) {
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
	// Retries + 1 total attempts: the initial try plus 3 retries, never more.
	require.Equal(t, opts.Retries+1, site.callCount("http://fake.test/"))
}

// TestCrawl_RetriesExhausted_TwoFailuresWithRetries2IsAnError covers the
// spec's explicit example: with --retries=2 and two failures, the final
// outcome must be an error (attempt 1 fails, retry 1 fails, retry 2
// exhausts the budget).
func TestCrawl_RetriesExhausted_TwoFailuresWithRetries2IsAnError(t *testing.T) {
	site := newFakeSite()
	site.handle("/", func(r *http.Request) (*http.Response, error) {
		return newResponse(http.StatusServiceUnavailable, ""), nil
	})

	opts := testOptions("http://fake.test/", site.client())
	opts.Retries = 2

	report, err := NewCrawler(opts).Run(context.Background())
	require.NoError(t, err)
	require.Len(t, report.Pages, 1)
	require.Equal(t, "error", report.Pages[0].Status)
	require.Equal(t, http.StatusServiceUnavailable, report.Pages[0].HTTPStatus)
	require.Equal(t, 3, site.callCount("http://fake.test/"))
}

// TestCrawl_OneFailureThenSuccessWithRetries2IsOK covers the spec's other
// explicit example: one failure followed by a successful second attempt
// counts as success overall.
func TestCrawl_OneFailureThenSuccessWithRetries2IsOK(t *testing.T) {
	site := newFakeSite()
	site.handle("/", func(r *http.Request) (*http.Response, error) {
		if site.callCount(r.URL.String()) == 1 {
			return newResponse(http.StatusServiceUnavailable, ""), nil
		}
		return newResponse(http.StatusOK, "root"), nil
	})

	opts := testOptions("http://fake.test/", site.client())
	opts.Retries = 2

	report, err := NewCrawler(opts).Run(context.Background())
	require.NoError(t, err)
	require.Len(t, report.Pages, 1)
	require.Equal(t, "ok", report.Pages[0].Status)
	require.Equal(t, 2, site.callCount("http://fake.test/"))
}

// TestCrawl_NonRetryableStatusStopsImmediately ensures a definitive 4xx
// (other than 429) is never retried, even when retries are available.
func TestCrawl_NonRetryableStatusStopsImmediately(t *testing.T) {
	site := newFakeSite()
	site.handle("/", func(r *http.Request) (*http.Response, error) {
		return newResponse(http.StatusForbidden, ""), nil
	})

	opts := testOptions("http://fake.test/", site.client())
	opts.Retries = 3

	report, err := NewCrawler(opts).Run(context.Background())
	require.NoError(t, err)
	require.Len(t, report.Pages, 1)
	require.Equal(t, "error", report.Pages[0].Status)
	require.Equal(t, http.StatusForbidden, report.Pages[0].HTTPStatus)
	require.Equal(t, 1, site.callCount("http://fake.test/"))
}

// TestCrawl_BrokenLink_ReflectsLastAttemptResult ensures the broken-links
// report reflects the outcome of the final retry, not the first failure.
func TestCrawl_BrokenLink_ReflectsLastAttemptResult(t *testing.T) {
	site := newFakeSite()
	site.handle("/", func(r *http.Request) (*http.Response, error) {
		return newResponse(http.StatusOK, fmt.Sprintf(htmlTemplate, `<a href="/flaky">Flaky</a>`)), nil
	})
	site.handle("/flaky", func(r *http.Request) (*http.Response, error) {
		if site.callCount(r.URL.String()) == 1 {
			return newResponse(http.StatusServiceUnavailable, ""), nil
		}
		return newResponse(http.StatusOK, "ok"), nil
	})

	opts := testOptions("http://fake.test/", site.client())
	opts.Retries = 2
	opts.Depth = 1 // keep /flaky as a checked link only, not a crawled page

	report, err := NewCrawler(opts).Run(context.Background())
	require.NoError(t, err)
	require.Len(t, report.Pages, 1)
	require.Empty(t, report.Pages[0].BrokenLinks)
}

// TestCrawl_RetryWaitIsNonZeroAndContextCancellationStopsIt ensures a
// canceled context aborts a pending retry wait immediately instead of
// blocking for retryBackoff.
func TestCrawl_RetryWaitIsNonZeroAndContextCancellationStopsIt(t *testing.T) {
	c := NewCrawler(testOptions("http://fake.test/", http.DefaultClient))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	err := c.retryWait(ctx)
	elapsed := time.Since(start)

	require.ErrorIs(t, err, context.Canceled)
	require.Less(t, elapsed, retryBackoff)
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
	opts.Depth = 1 // only check links on the root page, do not crawl them

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
	opts.Depth = 1

	report, err := NewCrawler(opts).Run(context.Background())
	require.NoError(t, err)
	require.Len(t, report.Pages, 1)
	require.Empty(t, report.Pages[0].BrokenLinks)
}

// TestCrawl_DepthZeroMeansNoLimit ensures the special-cased Depth=0 crawls
// every discovered same-host page regardless of how deep it is, unlike any
// positive Depth value which caps how many levels are followed.
func TestCrawl_DepthZeroMeansNoLimit(t *testing.T) {
	site := newFakeSite()
	site.handle("/", func(r *http.Request) (*http.Response, error) {
		return newResponse(http.StatusOK, fmt.Sprintf(htmlTemplate, `<a href="/level1">1</a>`)), nil
	})
	site.handle("/level1", func(r *http.Request) (*http.Response, error) {
		return newResponse(http.StatusOK, fmt.Sprintf(htmlTemplate, `<a href="/level2">2</a>`)), nil
	})
	site.handle("/level2", func(r *http.Request) (*http.Response, error) {
		return newResponse(http.StatusOK, fmt.Sprintf(htmlTemplate, `<a href="/level3">3</a>`)), nil
	})
	site.handle("/level3", func(r *http.Request) (*http.Response, error) {
		return newResponse(http.StatusOK, "leaf"), nil
	})

	opts := testOptions("http://fake.test/", site.client())
	opts.Depth = 0

	report, err := NewCrawler(opts).Run(context.Background())
	require.NoError(t, err)
	require.Len(t, report.Pages, 4)

	byURL := make(map[string]Page)
	for _, p := range report.Pages {
		byURL[p.URL] = p
	}
	require.Contains(t, byURL, "http://fake.test/level3")
	require.Equal(t, 3, byURL["http://fake.test/level3"].Depth)
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
	opts.Depth = 1

	report, err := NewCrawler(opts).Run(context.Background())
	require.NoError(t, err)
	require.Empty(t, report.Pages[0].BrokenLinks)
}

func TestCrawl_BrokenLinks_SharedLinkIsCheckedOnlyOnce(t *testing.T) {
	site := newFakeSite()
	site.handle("/", func(r *http.Request) (*http.Response, error) {
		return newResponse(http.StatusOK, fmt.Sprintf(htmlTemplate, `
			<a href="/page-a">A</a>
			<a href="http://cdn.test/asset.js">Shared external asset</a>
		`)), nil
	})
	site.handle("/page-a", func(r *http.Request) (*http.Response, error) {
		// Same external link also appears on this second, same-host page.
		return newResponse(http.StatusOK, fmt.Sprintf(htmlTemplate, `<a href="http://cdn.test/asset.js">Shared external asset</a>`)), nil
	})
	site.handle("/asset.js", func(r *http.Request) (*http.Response, error) {
		return newResponse(http.StatusNotFound, ""), nil
	})

	opts := testOptions("http://fake.test/", site.client())
	opts.Depth = 2 // root, plus its direct links (so /page-a is crawled too)

	report, err := NewCrawler(opts).Run(context.Background())
	require.NoError(t, err)

	byURL := make(map[string]Page)
	for _, p := range report.Pages {
		byURL[p.URL] = p
	}
	require.Len(t, byURL["http://fake.test/"].BrokenLinks, 1)
	require.Len(t, byURL["http://fake.test/page-a"].BrokenLinks, 1)

	// The external asset is never crawled as its own page (different host),
	// and despite being referenced from two pages, it must only be
	// requested once for the whole run: the second reference is served
	// from the per-run cache.
	require.Equal(t, 1, site.callCount("http://cdn.test/asset.js"))
}

func TestCrawl_SEO_AllTagsPresent(t *testing.T) {
	site := newFakeSite()
	site.handle("/", func(r *http.Request) (*http.Response, error) {
		return newResponse(http.StatusOK, `<html><head>
			<title>Example Test</title>
			<meta name="description" content="A short description.">
		</head><body>
			<h1>Welcome</h1>
		</body></html>`), nil
	})

	opts := testOptions("http://fake.test/", site.client())
	opts.Depth = 1

	report, err := NewCrawler(opts).Run(context.Background())
	require.NoError(t, err)
	require.Len(t, report.Pages, 1)

	seo := report.Pages[0].SEO
	require.True(t, seo.HasTitle)
	require.Equal(t, "Example Test", seo.Title)
	require.True(t, seo.HasDescription)
	require.Equal(t, "A short description.", seo.Description)
	require.True(t, seo.HasH1)
}

func TestCrawl_SEO_TagsMissing(t *testing.T) {
	site := newFakeSite()
	site.handle("/", func(r *http.Request) (*http.Response, error) {
		return newResponse(http.StatusOK, `<html><head></head><body><p>No SEO tags here.</p></body></html>`), nil
	})

	opts := testOptions("http://fake.test/", site.client())
	opts.Depth = 1

	report, err := NewCrawler(opts).Run(context.Background())
	require.NoError(t, err)
	require.Len(t, report.Pages, 1)

	seo := report.Pages[0].SEO
	require.False(t, seo.HasTitle)
	require.Empty(t, seo.Title)
	require.False(t, seo.HasDescription)
	require.Empty(t, seo.Description)
	require.False(t, seo.HasH1)
}

func TestCrawl_SEO_DecodesHTMLEntities(t *testing.T) {
	site := newFakeSite()
	site.handle("/", func(r *http.Request) (*http.Response, error) {
		return newResponse(http.StatusOK, `<html><head>
			<title>Fish &amp; Chips</title>
			<meta name="description" content="Salt &amp; vinegar included">
		</head><body></body></html>`), nil
	})

	opts := testOptions("http://fake.test/", site.client())
	opts.Depth = 1

	report, err := NewCrawler(opts).Run(context.Background())
	require.NoError(t, err)
	require.Len(t, report.Pages, 1)

	seo := report.Pages[0].SEO
	require.Equal(t, "Fish & Chips", seo.Title)
	require.Equal(t, "Salt & vinegar included", seo.Description)
}

func TestCrawl_Assets_ImageScriptAndStyleAreReported(t *testing.T) {
	site := newFakeSite()
	site.handle("/", func(r *http.Request) (*http.Response, error) {
		return newResponse(http.StatusOK, `<html><head>
			<link rel="stylesheet" href="/style.css">
		</head><body>
			<img src="/logo.png">
			<script src="/app.js"></script>
		</body></html>`), nil
	})
	site.handle("/logo.png", func(r *http.Request) (*http.Response, error) {
		return newResponseWithContentLength(http.StatusOK, "PNGDATA", 7), nil
	})
	site.handle("/app.js", func(r *http.Request) (*http.Response, error) {
		return newResponseWithContentLength(http.StatusOK, "console.log(1)", 15), nil
	})
	site.handle("/style.css", func(r *http.Request) (*http.Response, error) {
		return newResponseWithContentLength(http.StatusOK, "body{}", 6), nil
	})

	opts := testOptions("http://fake.test/", site.client())
	opts.Depth = 1

	report, err := NewCrawler(opts).Run(context.Background())
	require.NoError(t, err)
	require.Len(t, report.Pages, 1)

	assets := report.Pages[0].Assets
	require.Len(t, assets, 3)

	byType := make(map[string]Asset)
	for _, a := range assets {
		byType[a.Type] = a
	}

	img := byType[AssetTypeImage]
	require.Equal(t, "http://fake.test/logo.png", img.URL)
	require.Equal(t, http.StatusOK, img.StatusCode)
	require.Equal(t, int64(7), img.SizeBytes)
	require.Empty(t, img.Error)

	script := byType[AssetTypeScript]
	require.Equal(t, "http://fake.test/app.js", script.URL)
	require.Equal(t, int64(15), script.SizeBytes)

	style := byType[AssetTypeStyle]
	require.Equal(t, "http://fake.test/style.css", style.URL)
	require.Equal(t, int64(6), style.SizeBytes)
}

func TestCrawl_Assets_MissingContentLengthFallsBackToBodySize(t *testing.T) {
	site := newFakeSite()
	site.handle("/", func(r *http.Request) (*http.Response, error) {
		return newResponse(http.StatusOK, `<img src="/logo.png">`), nil
	})
	site.handle("/logo.png", func(r *http.Request) (*http.Response, error) {
		// No Content-Length header: ContentLength defaults to -1, as it
		// would for a chunked or otherwise length-less real response.
		return newResponseWithContentLength(http.StatusOK, "PNGDATA", -1), nil
	})

	opts := testOptions("http://fake.test/", site.client())
	opts.Depth = 1

	report, err := NewCrawler(opts).Run(context.Background())
	require.NoError(t, err)
	require.Len(t, report.Pages, 1)
	require.Len(t, report.Pages[0].Assets, 1)

	asset := report.Pages[0].Assets[0]
	require.Equal(t, int64(len("PNGDATA")), asset.SizeBytes)
	require.Empty(t, asset.Error)
}

func TestCrawl_Assets_ErrorStatusIsReportedWithMessage(t *testing.T) {
	site := newFakeSite()
	site.handle("/", func(r *http.Request) (*http.Response, error) {
		return newResponse(http.StatusOK, `<img src="/missing.png">`), nil
	})
	site.handle("/missing.png", func(r *http.Request) (*http.Response, error) {
		return newResponse(http.StatusNotFound, ""), nil
	})

	opts := testOptions("http://fake.test/", site.client())
	opts.Depth = 1

	report, err := NewCrawler(opts).Run(context.Background())
	require.NoError(t, err)
	require.Len(t, report.Pages, 1)
	require.Len(t, report.Pages[0].Assets, 1)

	asset := report.Pages[0].Assets[0]
	require.Equal(t, http.StatusNotFound, asset.StatusCode)
	require.Equal(t, int64(0), asset.SizeBytes)
	require.NotEmpty(t, asset.Error)
}

func TestCrawl_Assets_NetworkFailureIsReported(t *testing.T) {
	site := newFakeSite()
	site.handle("/", func(r *http.Request) (*http.Response, error) {
		return newResponse(http.StatusOK, `<img src="/logo.png">`), nil
	})
	site.handle("/logo.png", func(r *http.Request) (*http.Response, error) {
		return nil, errors.New("connection refused")
	})

	opts := testOptions("http://fake.test/", site.client())
	opts.Depth = 1

	report, err := NewCrawler(opts).Run(context.Background())
	require.NoError(t, err)
	require.Len(t, report.Pages, 1)
	require.Len(t, report.Pages[0].Assets, 1)

	asset := report.Pages[0].Assets[0]
	require.Equal(t, 0, asset.StatusCode)
	require.Equal(t, int64(0), asset.SizeBytes)
	require.NotEmpty(t, asset.Error)
}

// TestCrawl_Assets_SharedAssetIsFetchedOnlyOnce ensures an asset referenced
// from multiple pages is fetched once and yields identical data everywhere
// it appears in the report.
func TestCrawl_Assets_SharedAssetIsFetchedOnlyOnce(t *testing.T) {
	site := newFakeSite()
	site.handle("/", func(r *http.Request) (*http.Response, error) {
		return newResponse(http.StatusOK, fmt.Sprintf(htmlTemplate, `
			<a href="/other">Other</a>
			<img src="/shared.png">
			<img src="/shared.png">
		`)), nil
	})
	site.handle("/other", func(r *http.Request) (*http.Response, error) {
		return newResponse(http.StatusOK, `<img src="/shared.png">`), nil
	})
	site.handle("/shared.png", func(r *http.Request) (*http.Response, error) {
		return newResponseWithContentLength(http.StatusOK, "PNGDATA", 7), nil
	})

	opts := testOptions("http://fake.test/", site.client())
	opts.Depth = 2 // root, plus its direct links

	report, err := NewCrawler(opts).Run(context.Background())
	require.NoError(t, err)
	require.Len(t, report.Pages, 2)

	// The duplicate <img> on the root page must not multiply into two
	// entries; each page also references the asset exactly once.
	for _, page := range report.Pages {
		require.Len(t, page.Assets, 1)
		require.Equal(t, "http://fake.test/shared.png", page.Assets[0].URL)
		require.Equal(t, int64(7), page.Assets[0].SizeBytes)
	}

	require.Equal(t, 1, site.callCount("http://fake.test/shared.png"))
}

// linksToPages builds an HTML body linking to n same-host pages, each of
// which serves a trivial 200 response with no further links, so the crawl
// makes exactly n+1 requests (root + n children).
func linksToPages(n int) (string, []string) {
	var sb strings.Builder
	paths := make([]string, n)
	for i := range n {
		p := fmt.Sprintf("/page%d", i)
		paths[i] = p
		fmt.Fprintf(&sb, `<a href="%s">p</a>`, p)
	}
	return sb.String(), paths
}

func TestCrawl_DelayThrottlesRequestsGlobally(t *testing.T) {
	const n = 3
	links, paths := linksToPages(n)
	site := newFakeSite()
	site.handle("/", func(r *http.Request) (*http.Response, error) {
		return newResponse(http.StatusOK, fmt.Sprintf(htmlTemplate, links)), nil
	})
	for _, p := range paths {
		site.handle(p, func(r *http.Request) (*http.Response, error) {
			return newResponse(http.StatusOK, fmt.Sprintf(htmlTemplate, "")), nil
		})
	}

	opts := testOptions("http://fake.test/", site.client())
	opts.Depth = 2 // root, plus its direct links
	opts.Concurrency = n
	opts.Delay = 40 * time.Millisecond

	report, err := NewCrawler(opts).Run(context.Background())
	require.NoError(t, err)
	require.Len(t, report.Pages, n+1)

	times := site.requestTimes()
	// Each child link is both crawled as its own page and probed by the
	// broken-link checker, so total requests are root(1) + 2*n children.
	require.Len(t, times, 1+2*n)
	const tolerance = 5 * time.Millisecond
	for i := 1; i < len(times); i++ {
		gap := times[i].Sub(times[i-1])
		require.GreaterOrEqualf(t, gap, opts.Delay-tolerance, "gap between request %d and %d was %v, want >= %v", i-1, i, gap, opts.Delay)
	}
}

func TestCrawl_NoLimitIsNotArtificiallySlowed(t *testing.T) {
	const n = 20
	links, paths := linksToPages(n)
	site := newFakeSite()
	site.handle("/", func(r *http.Request) (*http.Response, error) {
		return newResponse(http.StatusOK, fmt.Sprintf(htmlTemplate, links)), nil
	})
	for _, p := range paths {
		site.handle(p, func(r *http.Request) (*http.Response, error) {
			return newResponse(http.StatusOK, fmt.Sprintf(htmlTemplate, "")), nil
		})
	}

	opts := testOptions("http://fake.test/", site.client())
	opts.Depth = 2 // root, plus its direct links
	opts.Concurrency = n
	opts.Delay = 0

	start := time.Now()
	report, err := NewCrawler(opts).Run(context.Background())
	elapsed := time.Since(start)
	require.NoError(t, err)
	require.Len(t, report.Pages, n+1)
	// With no rate limit, n+1 in-process requests across n workers should
	// complete almost instantly. A generous bound well below what any
	// configured Delay in the other tests would allow catches the case
	// where throttling is accidentally applied when both limits are zero.
	require.Less(t, elapsed, 200*time.Millisecond)
}

func TestCrawl_ContextCancellationStopsThrottleWaitImmediately(t *testing.T) {
	site := newFakeSite()
	site.handle("/", func(r *http.Request) (*http.Response, error) {
		return newResponse(http.StatusOK, fmt.Sprintf(htmlTemplate, "")), nil
	})

	opts := testOptions("http://fake.test/", site.client())
	opts.Delay = time.Hour // effectively infinite; only cancellation should unblock throttle

	c := NewCrawler(opts)
	// The ticker's first tick doesn't fire until opts.Delay has elapsed, so
	// this throttle call is guaranteed to block on it (or on ctx.Done())
	// rather than returning immediately.
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- c.throttle(ctx)
	}()

	cancel()

	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("throttle did not return promptly after context cancellation")
	}
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

// captureStderr temporarily redirects os.Stderr to a pipe for the duration
// of fn, and returns everything written to it. It is not safe for tests
// that run in parallel with each other, since os.Stderr is process-global.
func captureStderr(t *testing.T, fn func()) string {
	t.Helper()

	r, w, err := os.Pipe()
	require.NoError(t, err)

	original := os.Stderr
	os.Stderr = w
	defer func() { os.Stderr = original }()

	fn()

	require.NoError(t, w.Close())
	out, err := io.ReadAll(r)
	require.NoError(t, err)
	return string(out)
}

func TestCrawl_Debug_LogsRequestsToStderr(t *testing.T) {
	site := newFakeSite()
	site.handle("/", func(r *http.Request) (*http.Response, error) {
		return newResponse(http.StatusOK, fmt.Sprintf(htmlTemplate, `<a href="/broken">broken</a>`)), nil
	})
	site.handle("/broken", func(r *http.Request) (*http.Response, error) {
		return newResponse(http.StatusNotFound, ""), nil
	})

	opts := testOptions("http://fake.test/", site.client())
	opts.Debug = true

	output := captureStderr(t, func() {
		_, err := NewCrawler(opts).Run(context.Background())
		require.NoError(t, err)
	})

	require.Contains(t, output, "[DEBUG]")
	require.Contains(t, output, "GET http://fake.test/ -> 200")
	require.Contains(t, output, "http://fake.test/broken -> 404")
}

func TestCrawl_NoDebug_LogsNothingToStderr(t *testing.T) {
	site := newFakeSite()
	site.handle("/", func(r *http.Request) (*http.Response, error) {
		return newResponse(http.StatusOK, fmt.Sprintf(htmlTemplate, "hi")), nil
	})

	opts := testOptions("http://fake.test/", site.client())
	opts.Debug = false

	output := captureStderr(t, func() {
		_, err := NewCrawler(opts).Run(context.Background())
		require.NoError(t, err)
	})

	require.Empty(t, output)
}
