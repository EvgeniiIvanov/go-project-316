package crawler

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestOptionsValidate(t *testing.T) {
	cases := []struct {
		name    string
		opts    Options
		wantErr string
	}{
		{"valid options, depth zero means unlimited", Options{URL: "http://x", Concurrency: 1, Depth: 0}, ""},
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
          "size_bytes": 12345
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
// report: every key, its position, and its value must match exactly.
// Page.Error and Asset.Error are both omitted entirely when empty (see
// their omitempty tags); BrokenLink.Error stays mandatory since exactly one
// of BrokenLink.StatusCode/Error is always meaningful.
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
	opts.Depth = 2 // root, plus its direct links

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

func TestAnalyze_DepthOneFetchesOnlyRoot(t *testing.T) {
	site := newFakeSite()
	site.handle("/", func(r *http.Request) (*http.Response, error) {
		return newResponse(http.StatusOK, fmt.Sprintf(htmlTemplate, `<a href="/about">About</a>`)), nil
	})

	opts := testOptions("http://fake.test/", site.client())
	opts.Depth = 1

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

// TestAnalyze_DepthZeroIsUnlimited ensures the public Analyze entry point
// exposes the same Depth=0 "no limit" special case as the underlying
// engine, following links regardless of how deep the site goes.
func TestAnalyze_DepthZeroIsUnlimited(t *testing.T) {
	site := newFakeSite()
	site.handle("/", func(r *http.Request) (*http.Response, error) {
		return newResponse(http.StatusOK, fmt.Sprintf(htmlTemplate, `<a href="/about">About</a>`)), nil
	})
	site.handle("/about", func(r *http.Request) (*http.Response, error) {
		return newResponse(http.StatusOK, fmt.Sprintf(htmlTemplate, `<a href="/team">Team</a>`)), nil
	})
	site.handle("/team", func(r *http.Request) (*http.Response, error) {
		return newResponse(http.StatusOK, "team page"), nil
	})

	opts := testOptions("http://fake.test/", site.client())
	opts.Depth = 0

	data, err := Analyze(context.Background(), opts)
	require.NoError(t, err)

	var report Report
	require.NoError(t, json.Unmarshal(data, &report))
	require.Len(t, report.Pages, 3)

	byURL := make(map[string]Page)
	for _, p := range report.Pages {
		byURL[p.URL] = p
	}
	require.Contains(t, byURL, "http://fake.test/team")
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
	opts.Depth = 2 // root, plus its direct links (so /slow is reachable)
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
