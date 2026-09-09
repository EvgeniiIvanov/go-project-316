package crawler

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

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
