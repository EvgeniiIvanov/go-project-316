package crawler

import "time"

// Report is the top-level JSON result of a crawl.
type Report struct {
	RootURL     string    `json:"root_url"`
	Depth       int       `json:"depth"`
	GeneratedAt time.Time `json:"generated_at"`
	Pages       []Page    `json:"pages"`
}

// Page describes the outcome of fetching a single URL during the crawl.
// Every field is always present in the JSON report, even when it holds a
// zero value (empty string, empty array, etc), except Error, which is
// omitted entirely when the page was fetched successfully (Status == "ok").
type Page struct {
	URL          string       `json:"url"`
	Depth        int          `json:"depth"`
	HTTPStatus   int          `json:"http_status"`
	Status       string       `json:"status"`
	Error        string       `json:"error,omitempty"`
	SEO          SEO          `json:"seo"`
	BrokenLinks  []BrokenLink `json:"broken_links"`
	Assets       []Asset      `json:"assets"`
	DiscoveredAt time.Time    `json:"discovered_at"`
}

// Asset describes a single static resource (image, script, or stylesheet)
// referenced by a page. Exactly one of a successful (StatusCode < 400,
// Error == "") or failed (Error != "") outcome applies. Error is omitted
// entirely from the JSON when the asset was fetched successfully.
type Asset struct {
	URL        string `json:"url"`
	Type       string `json:"type"`
	StatusCode int    `json:"status_code"`
	SizeBytes  int64  `json:"size_bytes"`
	Error      string `json:"error,omitempty"`
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
