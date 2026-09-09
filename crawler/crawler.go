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

// Report is the top-level JSON result of a crawl.
type Report struct {
	RootURL     string    `json:"root_url"`
	Depth       int       `json:"depth"`
	GeneratedAt time.Time `json:"generated_at"`
	Pages       []Page    `json:"pages"`
}

// Page describes the outcome of fetching a single URL during the crawl.
// Every field is always present in the JSON report, even when it holds a
// zero value (empty string, empty array, etc): consumers should never need
// to handle a missing key, only an empty one.
type Page struct {
	URL          string       `json:"url"`
	Depth        int          `json:"depth"`
	HTTPStatus   int          `json:"http_status"`
	Status       string       `json:"status"`
	Error        string       `json:"error"`
	SEO          SEO          `json:"seo"`
	BrokenLinks  []BrokenLink `json:"broken_links"`
	Assets       []Asset      `json:"assets"`
	DiscoveredAt time.Time    `json:"discovered_at"`
}

// Asset describes a single static resource (image, script, or stylesheet)
// referenced by a page. Exactly one of a successful (StatusCode < 400,
// Error == "") or failed (Error != "") outcome applies; all fields are
// always present in the JSON report, even on failure, so consumers never
// need to special-case a missing field.
type Asset struct {
	URL        string `json:"url"`
	Type       string `json:"type"`
	StatusCode int    `json:"status_code"`
	SizeBytes  int64  `json:"size_bytes"`
	Error      string `json:"error"`
}

// Asset type values recognized in the report.
const (
	AssetTypeImage  = "image"
	AssetTypeScript = "script"
	AssetTypeStyle  = "style"
)

// SEO holds the on-page SEO signals extracted from a page's HTML. The has_*
// flags reflect whether the corresponding tag was found in the document at
// all, regardless of whether its text is empty; the text fields hold the
// decoded (entity-unescaped), trimmed content of that tag, or "" when the
// tag was not found.
type SEO struct {
	HasTitle       bool   `json:"has_title"`
	Title          string `json:"title"`
	HasDescription bool   `json:"has_description"`
	Description    string `json:"description"`
	HasH1          bool   `json:"has_h1"`
}

// BrokenLink describes a link found on a page whose target could not be
// reached: either the server answered with a 4xx/5xx status, or the request
// failed outright (network error, timeout, etc). Exactly one of StatusCode
// or Error is set.
type BrokenLink struct {
	URL        string `json:"url"`
	StatusCode int    `json:"status_code"`
	Error      string `json:"error"`
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

	// linkChecks caches the outcome of probing a link's reachability, keyed
	// by its normalized URL, so that a link repeated across many pages (or
	// one that is both linked-to and separately crawled as its own page) is
	// only ever requested once per run, no matter how many pages reference
	// it or how many workers ask concurrently.
	linkChecks map[string]*linkCheckEntry

	// assetChecks caches the outcome of fetching an asset (image, script, or
	// stylesheet), keyed by its normalized URL, so that the same asset
	// referenced from multiple pages is only ever requested once per run.
	assetChecks map[string]*assetCheckEntry

	// limiter paces every outgoing HTTP request (first attempts and
	// retries alike) so that, no matter how many workers are running,
	// requests never go out faster than one per opts.interval(). nil means
	// no pacing (opts.interval() <= 0).
	limiter *time.Ticker
}

// linkCheckEntry memoizes the result of probing one URL. once ensures the
// probe runs exactly once even if several workers race to check the same
// link at the same time; every caller either runs the probe or waits for
// the one that is already running, then reads its result.
type linkCheckEntry struct {
	once   sync.Once
	result *BrokenLink
}

// assetCheckEntry memoizes the result of fetching one asset URL. once
// ensures the fetch runs exactly once even if several workers race to
// request the same asset at the same time.
type assetCheckEntry struct {
	once   sync.Once
	result Asset
}

func NewCrawler(opts Options) *Crawler {
	c := &Crawler{
		client:      opts.Client(),
		opts:        opts,
		visited:     make(map[string]struct{}),
		linkChecks:  make(map[string]*linkCheckEntry),
		assetChecks: make(map[string]*assetCheckEntry),
	}
	if interval := opts.interval(); interval > 0 {
		c.limiter = time.NewTicker(interval)
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

// retryWait pauses for retryBackoff before the next retry attempt, returning
// ctx.Err() immediately if ctx is canceled or times out before the wait
// completes, so a canceled crawl never sits through a retry backoff.
func (c *Crawler) retryWait(ctx context.Context) error {
	timer := time.NewTimer(retryBackoff)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// isRetryableStatus reports whether a request attempt represents a transient
// failure worth retrying: a network-level error (no response at all), a 429
// Too Many Requests, or any 5xx server error. Any other outcome, including a
// definitive 4xx status such as 404, is treated as permanent and is not
// retried, regardless of how many retries remain.
func isRetryableStatus(status int, err error) bool {
	if status == http.StatusTooManyRequests || status >= http.StatusInternalServerError {
		return true
	}
	if status != 0 {
		return false
	}
	return err != nil
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
		URL:          job.url.String(),
		Depth:        job.depth,
		DiscoveredAt: time.Now().UTC().Truncate(time.Second),
		BrokenLinks:  []BrokenLink{},
		Assets:       []Asset{},
	}

	body, status, err := c.fetch(ctx, job.url.String())
	page.HTTPStatus = status
	if err != nil {
		page.Status = "error"
		page.Error = err.Error()
		return page, nil
	}
	page.Status = "ok"
	page.SEO = extractSEO(body)

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

	for _, ref := range dedupeAssetRefs(extractAssets(job.url, body)) {
		if !isCheckableScheme(ref.url) || ctx.Err() != nil {
			continue
		}
		page.Assets = append(page.Assets, c.checkAsset(ctx, ref.url, ref.typ))
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
// Results are cached per run: a link referenced from multiple pages, or one
// that is both linked-to and crawled as its own page, is only probed once.
func (c *Crawler) checkLink(ctx context.Context, link *url.URL) *BrokenLink {
	entry := c.linkCheckEntry(link)

	entry.once.Do(func() {
		status, err := c.probeWithRetries(ctx, link.String())
		switch {
		case err != nil:
			entry.result = &BrokenLink{URL: link.String(), Error: err.Error()}
		case status >= 400:
			entry.result = &BrokenLink{URL: link.String(), StatusCode: status}
		}
	})

	return entry.result
}

// probeWithRetries probes rawURL, retrying up to opts.Retries additional
// times on transient failures (network errors, 429, 5xx) with a fixed,
// non-zero pause between attempts. The broken-link report reflects the
// outcome of the last attempt only.
func (c *Crawler) probeWithRetries(ctx context.Context, rawURL string) (int, error) {
	var status int
	var err error
	for attempt := 0; attempt <= c.opts.Retries; attempt++ {
		if werr := c.throttle(ctx); werr != nil {
			return 0, werr
		}
		status, err = c.probe(ctx, rawURL)
		if !isRetryableStatus(status, err) || attempt == c.opts.Retries {
			return status, err
		}
		if werr := c.retryWait(ctx); werr != nil {
			return status, werr
		}
	}
	return status, err
}

// linkCheckEntry returns the cache entry for link's normalized URL,
// creating it on first use.
func (c *Crawler) linkCheckEntry(link *url.URL) *linkCheckEntry {
	key := normalizeURL(link.String())

	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.linkChecks[key]
	if !ok {
		entry = &linkCheckEntry{}
		c.linkChecks[key] = entry
	}
	return entry
}

// assetCacheEntry returns the cache entry for assetURL's normalized URL,
// creating it on first use.
func (c *Crawler) assetCacheEntry(assetURL *url.URL) *assetCheckEntry {
	key := normalizeURL(assetURL.String())

	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.assetChecks[key]
	if !ok {
		entry = &assetCheckEntry{}
		c.assetChecks[key] = entry
	}
	return entry
}

// checkAsset fetches a single asset (image, script, or stylesheet) and
// reports its status code, size, and any error. Results are cached per run
// by normalized URL: an asset referenced from multiple pages is only ever
// requested once, no matter how many pages reference it or how many workers
// ask concurrently.
func (c *Crawler) checkAsset(ctx context.Context, assetURL *url.URL, assetType string) Asset {
	entry := c.assetCacheEntry(assetURL)
	entry.once.Do(func() {
		entry.result = c.fetchAsset(ctx, assetURL.String(), assetType)
	})
	return entry.result
}

// fetchAsset performs a single GET for rawURL and builds the Asset report
// entry. Size is taken from the Content-Length header when the server sent
// one; otherwise it falls back to the length of the actual body read. If
// the size cannot be determined at all (the body could not be read), size
// is left at 0 and error explains why.
func (c *Crawler) fetchAsset(ctx context.Context, rawURL, assetType string) Asset {
	asset := Asset{URL: rawURL, Type: assetType}

	if err := c.throttle(ctx); err != nil {
		asset.Error = err.Error()
		return asset
	}

	reqCtx, cancel := context.WithTimeout(ctx, c.opts.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, rawURL, nil)
	if err != nil {
		asset.Error = err.Error()
		return asset
	}
	if c.opts.UserAgent != "" {
		req.Header.Set("User-Agent", c.opts.UserAgent)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		asset.Error = err.Error()
		return asset
	}
	defer func() { _ = resp.Body.Close() }()
	asset.StatusCode = resp.StatusCode

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		asset.Error = fmt.Sprintf("failed to read response body: %v", err)
		return asset
	}
	if resp.StatusCode >= http.StatusBadRequest {
		asset.Error = fmt.Sprintf("unexpected status code %d", resp.StatusCode)
		return asset
	}
	if resp.ContentLength >= 0 {
		asset.SizeBytes = resp.ContentLength
	} else {
		asset.SizeBytes = int64(len(body))
	}
	return asset
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

// fetch performs a GET for rawURL, retrying up to opts.Retries additional
// times on transient failures (network errors, 429, 5xx) with a fixed,
// non-zero pause between attempts. The result of the last attempt (success
// or failure) is what gets returned and, ultimately, reported.
func (c *Crawler) fetch(ctx context.Context, rawURL string) ([]byte, int, error) {
	var body []byte
	var status int
	var err error
	for attempt := 0; attempt <= c.opts.Retries; attempt++ {
		if werr := c.throttle(ctx); werr != nil {
			return nil, 0, werr
		}

		body, status, err = c.doRequest(ctx, rawURL)
		if !isRetryableStatus(status, err) || attempt == c.opts.Retries {
			return body, status, err
		}
		if werr := c.retryWait(ctx); werr != nil {
			return body, status, werr
		}
	}
	return body, status, err
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

// assetRef pairs a resolved asset URL with its detected type.
type assetRef struct {
	url *url.URL
	typ string
}

// extractAssets parses an HTML document and returns every image (<img
// src>), script (<script src>), and stylesheet (<link rel="stylesheet"
// href>) it references, with URLs resolved against base.
func extractAssets(base *url.URL, body []byte) []assetRef {
	var assets []assetRef
	tokenizer := html.NewTokenizer(bytes.NewReader(body))
	for {
		tt := tokenizer.Next()
		if tt == html.ErrorToken {
			return assets
		}
		if tt != html.StartTagToken && tt != html.SelfClosingTagToken {
			continue
		}

		token := tokenizer.Token()
		var ref *assetRef
		switch token.Data {
		case "img":
			ref = resolveAssetAttr(base, token, "src", AssetTypeImage)
		case "script":
			ref = resolveAssetAttr(base, token, "src", AssetTypeScript)
		case "link":
			if isStylesheetLink(token) {
				ref = resolveAssetAttr(base, token, "href", AssetTypeStyle)
			}
		}
		if ref != nil {
			assets = append(assets, *ref)
		}
	}
}

// isStylesheetLink reports whether a <link> tag's rel attribute identifies
// it as a stylesheet.
func isStylesheetLink(token html.Token) bool {
	for _, attr := range token.Attr {
		if strings.EqualFold(attr.Key, "rel") && strings.EqualFold(strings.TrimSpace(attr.Val), "stylesheet") {
			return true
		}
	}
	return false
}

// resolveAssetAttr extracts attrKey from token, resolves it against base,
// and returns an assetRef of the given type, or nil if the attribute is
// missing, empty, or unparsable.
func resolveAssetAttr(base *url.URL, token html.Token, attrKey, assetType string) *assetRef {
	for _, attr := range token.Attr {
		if attr.Key != attrKey {
			continue
		}
		val := strings.TrimSpace(attr.Val)
		if val == "" {
			return nil
		}
		u, err := url.Parse(val)
		if err != nil {
			return nil
		}
		resolved := base.ResolveReference(u)
		resolved.Fragment = ""
		return &assetRef{url: resolved, typ: assetType}
	}
	return nil
}

// dedupeAssetRefs removes repeated asset references (e.g. the same script
// included twice on one page), preserving first-seen order, so a page's
// asset list never contains the same URL more than once.
func dedupeAssetRefs(refs []assetRef) []assetRef {
	seen := make(map[string]struct{}, len(refs))
	unique := refs[:0]
	for _, ref := range refs {
		key := normalizeURL(ref.url.String())
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		unique = append(unique, ref)
	}
	return unique
}

// extractSEO parses an HTML document and reports its on-page SEO signals:
// the <title> text, the content of <meta name="description">, and whether
// an <h1> is present. Only the first occurrence of each tag counts; later
// duplicates are ignored. html.Tokenizer decodes entities for both text
// content (Text()) and attribute values (as part of Token()), so values
// like "Fish &amp; Chips" come out already as "Fish & Chips".
func extractSEO(body []byte) SEO {
	var seo SEO
	tokenizer := html.NewTokenizer(bytes.NewReader(body))

	for {
		tt := tokenizer.Next()
		if tt == html.ErrorToken {
			return seo
		}
		if tt != html.StartTagToken && tt != html.SelfClosingTagToken {
			continue
		}

		token := tokenizer.Token()
		switch token.Data {
		case "title":
			captureTitle(tokenizer, tt, &seo)
		case "meta":
			captureMetaDescription(token, &seo)
		case "h1":
			seo.HasH1 = true
		}
	}
}

// captureTitle records the first <title> tag's text, if any is present as
// the immediately following text token. It is a no-op once a title has
// already been captured, so later duplicates are ignored.
func captureTitle(tokenizer *html.Tokenizer, tt html.TokenType, seo *SEO) {
	if seo.HasTitle {
		return
	}
	seo.HasTitle = true
	if tt == html.StartTagToken && tokenizer.Next() == html.TextToken {
		seo.Title = strings.TrimSpace(string(tokenizer.Text()))
	}
}

// captureMetaDescription records the content of the first
// <meta name="description" content="..."> tag. It is a no-op once a
// description has already been captured, so later duplicates are ignored.
func captureMetaDescription(token html.Token, seo *SEO) {
	if seo.HasDescription {
		return
	}
	var name, content string
	for _, attr := range token.Attr {
		switch strings.ToLower(attr.Key) {
		case "name":
			name = strings.ToLower(strings.TrimSpace(attr.Val))
		case "content":
			content = attr.Val
		}
	}
	if name == "description" {
		seo.HasDescription = true
		seo.Description = strings.TrimSpace(content)
	}
}

// Analyze runs a full crawl for opts and returns the resulting Report
// marshaled as JSON. If the crawl is cut short (ctx canceled or its deadline
// exceeded), Run still returns a non-nil report describing whatever pages
// were collected before the cancellation; Analyze marshals that report and
// returns it alongside the error, rather than discarding it, so a caller
// that aborts a long crawl still gets valid, usable JSON for the pages that
// were fetched so far. Only errors that prevent a report from existing at
// all (e.g. an unparsable root URL) result in a nil []byte.
func Analyze(ctx context.Context, opts Options) ([]byte, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	report, runErr := NewCrawler(opts).Run(ctx)
	if report == nil {
		return nil, runErr
	}

	var data []byte
	var err error
	if opts.IndentJSON {
		data, err = json.MarshalIndent(report, "", "  ")
	} else {
		data, err = json.Marshal(report)
	}
	if err != nil {
		return nil, err
	}
	return data, runErr
}
