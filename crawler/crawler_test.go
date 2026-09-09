package crawler

import (
	"bytes"
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

// referenceReportJSON is Hexlet's canonical example report: every field
// that must always be present in the JSON output, populated with a
// non-default value (or the empty value that still must appear).
const referenceReportJSON = `{
  "root_url": "https://example.com",
  "depth": 1,
  "generated_at": "2024-06-01T12:34:56Z",
  "pages": [
    {
      "url": "https://example.com",
      "depth": 0,
      "http_status": 200,
      "status": "ok",
      "seo": {
        "has_title": true,
        "title": "Example title",
        "has_description": true,
        "description": "Example description",
        "has_h1": true
      },
      "broken_links": [
        {
          "url": "https://example.com/missing",
          "status_code": 404,
          "error": "Not Found"
        }
      ],
      "assets": [
        {
          "url": "https://example.com/static/logo.png",
          "type": "image",
          "status_code": 200,
          "size_bytes": 12345,
          "error": ""
        }
      ],
      "discovered_at": "2024-06-01T12:34:56Z"
    }
  ]
}`

// referenceReport builds the Report value that referenceReportJSON encodes.
func referenceReport() Report {
	ts := time.Date(2024, 6, 1, 12, 34, 56, 0, time.UTC)
	return Report{
		RootURL:     "https://example.com",
		Depth:       1,
		GeneratedAt: ts,
		Pages: []Page{
			{
				URL:        "https://example.com",
				Depth:      0,
				HTTPStatus: http.StatusOK,
				Status:     "ok",
				Error:      "",
				SEO: SEO{
					HasTitle:       true,
					Title:          "Example title",
					HasDescription: true,
					Description:    "Example description",
					HasH1:          true,
				},
				BrokenLinks: []BrokenLink{
					{URL: "https://example.com/missing", StatusCode: http.StatusNotFound, Error: "Not Found"},
				},
				Assets: []Asset{
					{URL: "https://example.com/static/logo.png", Type: AssetTypeImage, StatusCode: http.StatusOK, SizeBytes: 12345, Error: ""},
				},
				DiscoveredAt: ts,
			},
		},
	}
}

// compactJSON removes insignificant whitespace, so two JSON documents that
// differ only in formatting compare equal byte-for-byte, including key
// order (json.Compact does not reorder keys, it only strips whitespace).
func compactJSON(t *testing.T, data []byte) string {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, json.Compact(&buf, data))
	return buf.String()
}

// TestReport_JSONMatchesReferenceSchema compares the library's JSON output,
// byte-for-byte after whitespace normalization, against Hexlet's reference
// report: every key, its position, and its value (including empty strings
// like Asset/BrokenLink's "error": "") must match exactly. Page.Error is
// omitted entirely when empty (see Page's omitempty tag).
func TestReport_JSONMatchesReferenceSchema(t *testing.T) {
	report := referenceReport()
	want := compactJSON(t, []byte(referenceReportJSON))

	compact, err := json.Marshal(report)
	require.NoError(t, err)
	require.Equal(t, want, string(compact))
}

// TestReport_IndentJSONOnlyAffectsFormatting ensures IndentJSON changes
// nothing but whitespace: once both outputs are compacted, they must be
// identical, including key order and every value.
func TestReport_IndentJSONOnlyAffectsFormatting(t *testing.T) {
	report := referenceReport()

	compact, err := json.Marshal(report)
	require.NoError(t, err)

	indented, err := json.MarshalIndent(report, "", "  ")
	require.NoError(t, err)
	// Sanity check that indenting actually changed the formatting, so this
	// test cannot pass vacuously if MarshalIndent silently behaved like
	// Marshal.
	require.NotEqual(t, string(compact), string(indented))

	require.Equal(t, compactJSON(t, compact), compactJSON(t, indented))
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
	opts.Depth = 0

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
	opts.Depth = 0

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
	opts.Depth = 0

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
	opts.Depth = 0

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
	opts.Depth = 1

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
