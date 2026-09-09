package crawler

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"golang.org/x/net/html"
)

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
