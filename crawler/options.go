// Package crawler is the public entry point for the website crawler. Its
// implementation (HTTP fetching, HTML parsing, the worker pool that drives
// the crawl) lives in the internal/engine package and is not part of this
// package's API surface; only Analyze, Options, and the report types below
// are meant to be used by callers outside this module.
package crawler

import "code/internal/engine"

// Default values for Options, used both by NewOptions and by the CLI flag
// definitions in cmd/hexlet-go-crawler, so there is exactly one place that
// defines what "default" means for this crawler.
const (
	DefaultDepth       = engine.DefaultDepth
	DefaultRetries     = engine.DefaultRetries
	DefaultDelay       = engine.DefaultDelay
	DefaultTimeout     = engine.DefaultTimeout
	DefaultUserAgent   = engine.DefaultUserAgent
	DefaultConcurrency = engine.DefaultConcurrency
	DefaultIndentJSON  = engine.DefaultIndentJSON
)

// Options configures a single crawl run. See internal/engine.Options for
// field documentation; it is aliased here so the type is identical for
// callers regardless of which package name they refer to it by.
type Options = engine.Options

// NewOptions returns Options for rawURL populated with the package's
// default values.
func NewOptions(rawURL string) Options {
	return engine.NewOptions(rawURL)
}
