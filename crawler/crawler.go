package crawler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/html"
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

// Report is the top-level JSON result of a crawl.
type Report struct {
	RootURL     string    `json:"root_url"`
	Depth       int       `json:"depth"`
	GeneratedAt time.Time `json:"generated_at"`
	Pages       []Page    `json:"pages"`
}

// Page describes the outcome of fetching a single URL during the crawl.
type Page struct {
	URL         string       `json:"url"`
	Depth       int          `json:"depth"`
	HTTPStatus  int          `json:"http_status"`
	Status      string       `json:"status"`
	Error       string       `json:"error"`
	BrokenLinks []BrokenLink `json:"broken_links,omitempty"`
}

// BrokenLink describes a link found on a page whose target could not be
// reached: either the server answered with a 4xx/5xx status, or the request
// failed outright (network error, timeout, etc). Exactly one of StatusCode
// or Error is set.
type BrokenLink struct {
	URL        string `json:"url"`
	StatusCode int    `json:"status_code,omitempty"`
	Error      string `json:"error,omitempty"`
}

type crawlJob struct {
	url   *url.URL
	depth int
}

type Crawler struct {
	client *http.Client
	opts   Options
	root   *url.URL

	mu      sync.Mutex
	visited map[string]struct{}

	// limiter paces every outgoing HTTP request (first attempts and
	// retries alike) so that, no matter how many workers are running,
	// requests never go out faster than one per opts.Delay. nil means no
	// pacing (opts.Delay <= 0).
	limiter *time.Ticker
}

func NewCrawler(opts Options) *Crawler {
	c := &Crawler{
		client:  opts.Client(),
		opts:    opts,
		visited: make(map[string]struct{}),
	}
	if opts.Delay > 0 {
		c.limiter = time.NewTicker(opts.Delay)
	}
	return c
}

// throttle blocks until it is this caller's turn to send a request,
// according to the shared rate limiter. It returns early if ctx is done.
func (c *Crawler) throttle(ctx context.Context) error {
	if c.limiter == nil {
		return nil
	}
	select {
	case <-c.limiter.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Crawler) visitedOrMark(u *url.URL) bool {
	key := normalizeURL(u.String())
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.visited[key]; ok {
		return false
	}
	c.visited[key] = struct{}{}
	return true
}

func (c *Crawler) Run(ctx context.Context) (*Report, error) {
	root, err := url.Parse(c.opts.URL)
	if err != nil {
		return nil, fmt.Errorf("invalid root url: %w", err)
	}
	c.root = root

	report := &Report{
		RootURL:     c.opts.URL,
		Depth:       c.opts.Depth,
		GeneratedAt: time.Now().UTC().Truncate(time.Second),
	}

	if !c.visitedOrMark(root) {
		return report, nil
	}

	run := &crawlRun{
		report: report,
		// jobs is shared by all workers: they read jobs from it and, when
		// a page has new links, write more jobs back into it. The buffer
		// is generous so that writing back rarely blocks; workers are
		// also consumers of the same channel, so if it ever did fill up
		// while every worker was stuck trying to send, that would
		// deadlock. The ctx.Done() case in enqueue is the escape hatch.
		jobs: make(chan crawlJob, 256),
	}

	for i := 0; i < c.opts.Concurrency; i++ {
		go c.worker(ctx, run)
	}

	run.pending.Add(1)
	run.jobs <- crawlJob{url: root, depth: 0}

	run.pending.Wait()
	close(run.jobs)

	return report, ctx.Err()
}

// crawlRun holds the state shared by all workers of a single Run call.
type crawlRun struct {
	jobs chan crawlJob

	// pending tracks jobs that are queued or currently being processed.
	// A job's Add(1) happens before it is sent to jobs; its Done() happens
	// only after its own children have already been added. That ordering
	// guarantees pending can only reach zero when nothing is left to do.
	pending sync.WaitGroup

	pagesMu sync.Mutex
	report  *Report
}

// worker consumes jobs until run.jobs is closed, fetching each page and
// enqueueing any newly discovered links as further jobs.
func (c *Crawler) worker(ctx context.Context, run *crawlRun) {
	for job := range run.jobs {
		page, links := c.processPage(ctx, job)

		run.pagesMu.Lock()
		run.report.Pages = append(run.report.Pages, page)
		run.pagesMu.Unlock()

		if ctx.Err() == nil && job.depth+1 <= c.opts.Depth {
			for _, link := range links {
				if c.visitedOrMark(link) {
					run.enqueue(ctx, crawlJob{url: link, depth: job.depth + 1})
				}
			}
		}
		run.pending.Done()
	}
}

// enqueue registers a pending job and sends it to the jobs channel, or
// backs out if ctx is done before the send can happen.
func (run *crawlRun) enqueue(ctx context.Context, job crawlJob) {
	run.pending.Add(1)
	select {
	case run.jobs <- job:
	case <-ctx.Done():
		run.pending.Done()
	}
}

func (c *Crawler) processPage(ctx context.Context, job crawlJob) (Page, []*url.URL) {
	page := Page{
		URL:   job.url.String(),
		Depth: job.depth,
	}

	body, status, err := c.fetch(ctx, job.url.String())
	page.HTTPStatus = status
	if err != nil {
		page.Status = "error"
		page.Error = err.Error()
		return page, nil
	}
	page.Status = "ok"

	var links []*url.URL
	for _, link := range dedupeLinks(extractLinks(job.url, body)) {
		if !isCheckableScheme(link) {
			continue
		}
		if strings.EqualFold(link.Host, c.root.Host) {
			links = append(links, link)
		}
		if ctx.Err() != nil {
			continue
		}
		if broken := c.checkLink(ctx, link); broken != nil {
			page.BrokenLinks = append(page.BrokenLinks, *broken)
		}
	}
	return page, links
}

// dedupeLinks removes repeated hrefs (e.g. the same link appearing several
// times on one page), preserving first-seen order, so neither the crawl
// queue nor the broken-links report gets duplicate entries for one page.
func dedupeLinks(links []*url.URL) []*url.URL {
	seen := make(map[string]struct{}, len(links))
	unique := links[:0]
	for _, link := range links {
		key := normalizeURL(link.String())
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		unique = append(unique, link)
	}
	return unique
}

// isCheckableScheme reports whether link is something an HTTP client can
// actually request. Schemes like mailto:, tel:, or javascript: (and links
// left without a scheme after resolution) are not broken-link candidates,
// they were never going to be fetched in the first place.
func isCheckableScheme(link *url.URL) bool {
	switch strings.ToLower(link.Scheme) {
	case "http", "https":
		return true
	default:
		return false
	}
}

// checkLink probes a single link's target to see whether it is reachable.
// It returns nil when the link is fine (2xx/3xx), and a *BrokenLink when the
// server answered with a 4xx/5xx status or the request failed outright.
func (c *Crawler) checkLink(ctx context.Context, link *url.URL) *BrokenLink {
	if err := c.throttle(ctx); err != nil {
		return nil
	}

	status, err := c.probe(ctx, link.String())
	if err != nil {
		return &BrokenLink{URL: link.String(), Error: err.Error()}
	}
	if status >= 400 {
		return &BrokenLink{URL: link.String(), StatusCode: status}
	}
	return nil
}

// probe checks whether rawURL is reachable, without caring about the body.
// It prefers HEAD, since it exists purely to check reachability, but falls
// back to GET when the server doesn't support HEAD (405/501): the result
// reported to callers only depends on the final status, not the method used
// to obtain it.
func (c *Crawler) probe(ctx context.Context, rawURL string) (int, error) {
	reqCtx, cancel := context.WithTimeout(ctx, c.opts.Timeout)
	defer cancel()

	status, err := c.doProbeRequest(reqCtx, http.MethodHead, rawURL)
	if err != nil {
		return 0, err
	}
	if status == http.StatusMethodNotAllowed || status == http.StatusNotImplemented {
		return c.doProbeRequest(reqCtx, http.MethodGet, rawURL)
	}
	return status, nil
}

func (c *Crawler) doProbeRequest(ctx context.Context, method, rawURL string) (int, error) {
	req, err := http.NewRequestWithContext(ctx, method, rawURL, nil)
	if err != nil {
		return 0, err
	}
	if c.opts.UserAgent != "" {
		req.Header.Set("User-Agent", c.opts.UserAgent)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)

	return resp.StatusCode, nil
}

func (c *Crawler) fetch(ctx context.Context, rawURL string) ([]byte, int, error) {
	var lastErr error
	for attempt := 0; attempt <= c.opts.Retries; attempt++ {
		if err := c.throttle(ctx); err != nil {
			return nil, 0, err
		}

		body, status, err := c.doRequest(ctx, rawURL)
		if err == nil {
			return body, status, nil
		}
		lastErr = err
		if status != 0 {
			// A response was received but had a non-2xx status; that is not
			// a transient failure worth retrying.
			return body, status, lastErr
		}
	}
	return nil, 0, lastErr
}

// doRequest performs a single GET attempt for rawURL.
func (c *Crawler) doRequest(ctx context.Context, rawURL string) ([]byte, int, error) {
	reqCtx, cancel := context.WithTimeout(ctx, c.opts.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, 0, err
	}
	if c.opts.UserAgent != "" {
		req.Header.Set("User-Agent", c.opts.UserAgent)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return body, resp.StatusCode, fmt.Errorf("unexpected status code %d", resp.StatusCode)
	}
	return body, resp.StatusCode, nil
}

// extractLinks parses an HTML document and returns the hrefs of all anchor
// tags, resolved against base. Fragments are stripped since they never
// affect what the server returns.
func extractLinks(base *url.URL, body []byte) []*url.URL {
	var links []*url.URL
	tokenizer := html.NewTokenizer(bytes.NewReader(body))
	for {
		tt := tokenizer.Next()
		if tt == html.ErrorToken {
			return links
		}
		if tt != html.StartTagToken && tt != html.SelfClosingTagToken {
			continue
		}
		token := tokenizer.Token()
		if token.Data != "a" {
			continue
		}
		for _, attr := range token.Attr {
			if attr.Key != "href" {
				continue
			}
			href := strings.TrimSpace(attr.Val)
			if href == "" {
				continue
			}
			u, err := url.Parse(href)
			if err != nil {
				continue
			}
			resolved := base.ResolveReference(u)
			resolved.Fragment = ""
			links = append(links, resolved)
		}
	}
}

// Analyze runs a full crawl for opts and returns the resulting Report
// marshaled as JSON.
func Analyze(ctx context.Context, opts Options) ([]byte, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	report, err := NewCrawler(opts).Run(ctx)
	if err != nil {
		return nil, err
	}

	if opts.IndentJSON {
		return json.MarshalIndent(report, "", "  ")
	}
	return json.Marshal(report)
}
