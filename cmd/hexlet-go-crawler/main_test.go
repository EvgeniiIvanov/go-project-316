package main

import (
	"context"
	"testing"
	"time"

	"code/crawler"

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"
)

func TestResolveDelay(t *testing.T) {
	cases := []struct {
		name  string
		delay time.Duration
		rps   float64
		want  time.Duration
	}{
		{"rps zero leaves delay unchanged", 500 * time.Millisecond, 0, 500 * time.Millisecond},
		{"rps negative leaves delay unchanged", 500 * time.Millisecond, -5, 500 * time.Millisecond},
		{"positive rps takes priority over delay", time.Second, 5, 200 * time.Millisecond},
		{"rps of 1 means 1s spacing", 0, 1, time.Second},
		{"rps so large that 1s/rps rounds to zero falls back to delay", 500 * time.Millisecond, 1e12, 500 * time.Millisecond},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, resolveDelay(tc.delay, tc.rps))
		})
	}
}

// runBuildOptions parses args (excluding the program name) against a fresh
// *cli.Command wired with the real appFlags, and returns the crawler.Options
// buildOptions would produce for those flags plus the given url.
func runBuildOptions(t *testing.T, url string, args ...string) crawler.Options {
	t.Helper()
	var got crawler.Options
	cmd := &cli.Command{
		Name:  "hexlet-go-crawler",
		Flags: appFlags,
		Action: func(ctx context.Context, cmd *cli.Command) error {
			got = buildOptions(url, cmd)
			return nil
		},
	}
	require.NoError(t, cmd.Run(context.Background(), append([]string{"hexlet-go-crawler"}, args...)))
	return got
}

func TestBuildOptions_Defaults(t *testing.T) {
	opts := runBuildOptions(t, "https://example.com")
	require.Equal(t, "https://example.com", opts.URL)
	require.Equal(t, crawler.DefaultDepth, opts.Depth)
	require.Equal(t, crawler.DefaultRetries, opts.Retries)
	require.Equal(t, crawler.DefaultDelay, opts.Delay)
	require.Equal(t, crawler.DefaultTimeout, opts.Timeout)
	require.Equal(t, crawler.DefaultUserAgent, opts.UserAgent)
	require.Equal(t, crawler.DefaultConcurrency, opts.Concurrency)
	require.Equal(t, crawler.DefaultIndentJSON, opts.IndentJSON)
}

func TestBuildOptions_DelayFlagIsUsedWhenRPSUnset(t *testing.T) {
	opts := runBuildOptions(t, "https://example.com", "--delay", "300ms")
	require.Equal(t, 300*time.Millisecond, opts.Delay)
}

func TestBuildOptions_RPSFlagOverridesDelay(t *testing.T) {
	opts := runBuildOptions(t, "https://example.com", "--delay", "1s", "--rps", "10")
	require.Equal(t, 100*time.Millisecond, opts.Delay)
}

func TestBuildOptions_RPSZeroKeepsDelay(t *testing.T) {
	opts := runBuildOptions(t, "https://example.com", "--delay", "250ms", "--rps", "0")
	require.Equal(t, 250*time.Millisecond, opts.Delay)
}

func TestBuildOptions_AllFlagsAreMapped(t *testing.T) {
	opts := runBuildOptions(t, "https://example.com",
		"--depth", "4",
		"--retries", "7",
		"--timeout", "9s",
		"--user-agent", "custom-agent/1.0",
		"--workers", "3",
		"--indent",
	)
	require.Equal(t, 4, opts.Depth)
	require.Equal(t, 7, opts.Retries)
	require.Equal(t, 9*time.Second, opts.Timeout)
	require.Equal(t, "custom-agent/1.0", opts.UserAgent)
	require.Equal(t, 3, opts.Concurrency)
	require.True(t, opts.IndentJSON)
}
