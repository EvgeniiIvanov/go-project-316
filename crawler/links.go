package crawler

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"golang.org/x/net/html"
)

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

func (c *Crawler) doProbeRequest(ctx context.Context, method, rawURL string) (status int, err error) {
	start := time.Now()
	defer func() { c.logRequest(method, rawURL, start, status, err) }()

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
