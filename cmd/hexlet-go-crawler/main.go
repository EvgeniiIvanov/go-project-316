package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"code/crawler"

	"github.com/urfave/cli/v3"
)

func main() {
	cmd := &cli.Command{
		Name:  "hexlet-go-crawler",
		Usage: "analyze a website structure",
		UsageText: `hexlet-go-crawler [global options] <url>

		Examples:
  hexlet-go-crawler https://example.com
  hexlet-go-crawler --depth 2 --indent https://example.com`,
		Flags: []cli.Flag{
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
			&cli.IntFlag{
				Name:  "rps",
				Usage: "target requests per second, global across all workers; takes priority over --delay when set (0 disables)",
				Value: crawler.DefaultRPS,
			},
			&cli.IntFlag{
				Name:  "timeout",
				Usage: "request timeout in seconds",
				Value: int(crawler.DefaultTimeout.Seconds()),
			},
			&cli.StringFlag{
				Name:  "user-agent",
				Usage: "User-Agent header for requests",
				Value: crawler.DefaultUserAgent,
			},
			&cli.IntFlag{
				Name:  "concurrency",
				Usage: "number of concurrent workers",
				Value: crawler.DefaultConcurrency,
			},
			&cli.BoolFlag{
				Name:  "indent",
				Usage: "pretty-print JSON output",
				Value: crawler.DefaultIndentJSON,
			},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			url := cmd.Args().First()
			if url == "" {
				return cli.Exit("error: missing url argument", 1)
			}

			opts := crawler.NewOptions(url)
			opts.Depth = cmd.Int("depth")
			opts.Retries = cmd.Int("retries")
			opts.Delay = cmd.Duration("delay")
			opts.RPS = cmd.Int("rps")
			opts.Timeout = time.Duration(cmd.Int("timeout")) * time.Second
			opts.UserAgent = cmd.String("user-agent")
			opts.Concurrency = cmd.Int("concurrency")
			opts.IndentJSON = cmd.Bool("indent")

			data, err := crawler.Analyze(ctx, opts)
			if data == nil {
				return cli.Exit(fmt.Sprintf("error: %v", err), 1)
			}
			fmt.Print(string(data))
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
