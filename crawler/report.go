package crawler

import "code/internal/engine"

// Report is the top-level JSON result of a crawl, and Page/Asset/BrokenLink/
// SEO are its nested elements. See internal/engine for field documentation;
// they are aliased here so the types are identical for callers regardless of
// which package name they refer to them by.
type (
	Report     = engine.Report
	Page       = engine.Page
	Asset      = engine.Asset
	BrokenLink = engine.BrokenLink
	SEO        = engine.SEO
)

// Asset type values recognized in the report.
const (
	AssetTypeImage  = engine.AssetTypeImage
	AssetTypeScript = engine.AssetTypeScript
	AssetTypeStyle  = engine.AssetTypeStyle
)
