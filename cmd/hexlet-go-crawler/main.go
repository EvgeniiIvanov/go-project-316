package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"code/crawler"

	"github.com/urfave/cli/v3"
)

// appFlags defines the CLI flags accepted by the crawler command. It is a
// package-level var so tests can build an equivalent *cli.Command without
// duplicating the flag definitions.
var appFlags = []cli.Flag{
	&cli.IntFlag{
		Name:  "depth",
		Usage: "maximum crawling depth, 0 means only the root page",
		Value: crawler.DefaultDepth,
	},
	&cli.IntFlag{
		Name:  "retries",
		Usage: "number of retries for a failed request",
		Value: crawler.DefaultRetries,
	},
	&cli.DurationFlag{
		Name:  "delay",
		Usage: "fixed delay between requests, e.g. 200ms, 1s (global across all workers; ignored if --rps is set)",
		Value: crawler.DefaultDelay,
	},
	&cli.FloatFlag{
		Name:  "rps",
		Usage: "target requests per second, global across all workers; takes priority over --delay when set (0 disables)",
		Value: 0,
	},
	&cli.DurationFlag{
		Name:  "timeout",
		Usage: "per-request timeout, e.g. 5s, 15s",
		Value: crawler.DefaultTimeout,
	},
	&cli.StringFlag{
		Name:  "user-agent",
		Usage: "custom User-Agent header for requests",
		Value: crawler.DefaultUserAgent,
	},
	&cli.IntFlag{
		Name:  "workers",
		Usage: "number of concurrent workers",
		Value: crawler.DefaultConcurrency,
	},
	&cli.BoolFlag{
		Name:  "indent",
		Usage: "pretty-print JSON output",
		Value: crawler.DefaultIndentJSON,
	},
	&cli.BoolFlag{
		Name:  "debug",
		Usage: "log every HTTP request to stderr (method, URL, start time, outcome, duration)",
		Value: false,
	},
}

func main() {
	cmd := &cli.Command{
		Name:  "hexlet-go-crawler",
		Usage: "analyze a website structure",
		UsageText: `hexlet-go-crawler [global options] <url>

		Examples:
  hexlet-go-crawler https://example.com
  hexlet-go-crawler --depth 2 --indent https://example.com`,
		Flags: appFlags,
		Action: func(ctx context.Context, cmd *cli.Command) error {
			url := cmd.Args().First()
			if url == "" {
				return cli.Exit("error: missing url argument", 1)
			}

			opts := buildOptions(url, cmd)

			data, err := crawler.Analyze(ctx, opts)
			if data == nil {
				return cli.Exit(fmt.Sprintf("error: %v", err), 1)
			}
			// Print the report JSON verbatim, with nothing before or after
			// it besides the single trailing newline that terminals and
			// line-oriented tools (e.g. diff, cat) expect.
			fmt.Println(string(data))
			if err != nil {
				return cli.Exit(fmt.Sprintf("error: %v", err), 1)
			}
			return nil
		},
	}

	if err := cmd.Run(context.Background(), os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// resolveDelay converts the --delay/--rps flag pair into the single delay
// value crawler.Options understands. A positive rps takes priority over
// delay and is converted to its equivalent spacing (1s/rps); rps <= 0, or
// an rps so large that 1s/rps rounds down to zero, leaves delay unchanged.
func resolveDelay(delay time.Duration, rps float64) time.Duration {
	if rps > 0 {
		if perReq := time.Duration(float64(time.Second) / rps); perReq > 0 {
			return perReq
		}
	}
	return delay
}

// buildOptions maps the CLI flags of cmd into crawler.Options for the given
// target url.
func buildOptions(url string, cmd *cli.Command) crawler.Options {
	opts := crawler.NewOptions(url)
	opts.Depth = cmd.Int("depth")
	opts.Retries = cmd.Int("retries")
	opts.Delay = resolveDelay(cmd.Duration("delay"), cmd.Float64("rps"))
	opts.Timeout = cmd.Duration("timeout")
	opts.UserAgent = cmd.String("user-agent")
	opts.Concurrency = cmd.Int("workers")
	opts.IndentJSON = cmd.Bool("indent")
	opts.Debug = cmd.Bool("debug")
	return opts
}
