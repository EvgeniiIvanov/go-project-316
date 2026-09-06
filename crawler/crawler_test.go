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
// pacing (Delay/RPS) across all workers.
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
	opts.Depth = 0 // keep /flaky as a checked link only, not a crawled page

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
	opts.Depth = 1

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
	opts.Depth = 0

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
	opts.Depth = 0

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
	opts.Depth = 0

	report, err := NewCrawler(opts).Run(context.Background())
	require.NoError(t, err)
	require.Len(t, report.Pages, 1)

	seo := report.Pages[0].SEO
	require.Equal(t, "Fish & Chips", seo.Title)
	require.Equal(t, "Salt & vinegar included", seo.Description)
}

func TestAnalyze_ReturnsPartialReportOnContextCancellation(t *testing.T) {
	started := make(chan struct{})
	site := newFakeSite()
	site.handle("/", func(r *http.Request) (*http.Response, error) {
		return newResponse(http.StatusOK, fmt.Sprintf(htmlTemplate, `<a href="/slow">Slow</a>`)), nil
	})
	site.handle("/slow", func(r *http.Request) (*http.Response, error) {
		close(started)
		<-r.Context().Done()
		return nil, r.Context().Err()
	})

	opts := testOptions("http://fake.test/", site.client())
	opts.Depth = 1
	opts.Concurrency = 1

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		<-started
		cancel()
	}()

	data, err := Analyze(ctx, opts)
	require.Error(t, err)
	require.NotNil(t, data)

	var report Report
	require.NoError(t, json.Unmarshal(data, &report))
	require.Equal(t, "http://fake.test/", report.RootURL)

	byURL := make(map[string]Page)
	for _, p := range report.Pages {
		byURL[p.URL] = p
	}
	// The root page's own fetch completed before cancellation; only its
	// /slow link check was still in flight when ctx was canceled, so /slow
	// never becomes its own crawled page (the worker sees ctx already done
	// before it would enqueue it), but the root page it must still show up,
	// with the interrupted link check recorded as broken, in a valid report
	// rather than the whole thing being discarded because the crawl as a
	// whole ended in an error.
	root := byURL["http://fake.test/"]
	require.Equal(t, "ok", root.Status)
	require.Len(t, root.BrokenLinks, 1)
	require.Equal(t, "http://fake.test/slow", root.BrokenLinks[0].URL)
	require.NotEmpty(t, root.BrokenLinks[0].Error)
}

func TestOptionsInterval(t *testing.T) {
	cases := []struct {
		name  string
		delay time.Duration
		rps   int
		want  time.Duration
	}{
		{"neither set means no throttling", 0, 0, 0},
		{"delay only", 200 * time.Millisecond, 0, 200 * time.Millisecond},
		{"rps only", 0, 5, 200 * time.Millisecond},
		{"rps takes priority over delay when both set", time.Second, 5, 200 * time.Millisecond},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts := Options{Delay: tc.delay, RPS: tc.rps}
			require.Equal(t, tc.want, opts.interval())
		})
	}
}

func TestOptionsValidate_NegativeRPS(t *testing.T) {
	opts := Options{URL: "http://x", Concurrency: 1, RPS: -1}
	require.EqualError(t, opts.Validate(), "rps cannot be negative")
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

func TestCrawl_RPSThrottlesRequestsGlobally(t *testing.T) {
	const n = 4
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
	opts.Depth = 1
	opts.Concurrency = n     // enough workers that, unthrottled, all children fire near-simultaneously
	opts.RPS = 20            // 50ms minimum spacing, global across all workers
	opts.Delay = time.Second // must be ignored: RPS takes priority

	report, err := NewCrawler(opts).Run(context.Background())
	require.NoError(t, err)
	require.Len(t, report.Pages, n+1)

	times := site.requestTimes()
	// Each child link is both crawled as its own page and probed by the
	// broken-link checker, so total requests are root(1) + 2*n children.
	require.Len(t, times, 1+2*n)
	minInterval := time.Second / time.Duration(opts.RPS)
	// Allow a small tolerance for scheduler/timer jitter; the point of this
	// test is to catch requests firing without any pacing at all (which
	// would show gaps close to 0), not to enforce sub-millisecond ticker
	// precision.
	const tolerance = 5 * time.Millisecond
	for i := 1; i < len(times); i++ {
		gap := times[i].Sub(times[i-1])
		require.GreaterOrEqualf(t, gap, minInterval-tolerance, "gap between request %d and %d was %v, want >= %v", i-1, i, gap, minInterval)
	}
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
	opts.Depth = 1
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
	opts.Depth = 1
	opts.Concurrency = n
	opts.Delay = 0
	opts.RPS = 0

	start := time.Now()
	report, err := NewCrawler(opts).Run(context.Background())
	elapsed := time.Since(start)
	require.NoError(t, err)
	require.Len(t, report.Pages, n+1)
	// With no rate limit, n+1 in-process requests across n workers should
	// complete almost instantly. A generous bound well below what any
	// configured Delay/RPS in the other tests would allow catches the case
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
