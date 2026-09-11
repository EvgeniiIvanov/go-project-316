package crawler

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
)

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
func (c *Crawler) doRequest(ctx context.Context, rawURL string) (body []byte, status int, err error) {
	start := time.Now()
	defer func() { c.logRequest(http.MethodGet, rawURL, start, status, err) }()

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

	body, err = io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return body, resp.StatusCode, fmt.Errorf("unexpected status code %d", resp.StatusCode)
	}
	return body, resp.StatusCode, nil
}
