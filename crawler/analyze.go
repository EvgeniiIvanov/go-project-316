package crawler

import (
	"context"
	"encoding/json"
)

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
