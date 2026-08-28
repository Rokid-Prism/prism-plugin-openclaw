package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"

	pluginbridge "github.com/Rokid-Prism/prism-plugin-sdk"
	"github.com/Rokid-Prism/prism-plugin-openclaw/openclaw"
)

func main() {
	fs := flag.NewFlagSet("openclaw-adapter", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	gatewayURL := fs.String("gateway-url", envOr("OPENCLAW_GATEWAY_URL", "auto"), "OpenClaw Gateway URL or auto")
	homeDir := fs.String("home", envOr("OPENCLAW_HOME", ""), "OpenClaw home dir")
	if err := fs.Parse(os.Args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(0)
		}
		os.Exit(2)
	}
	server := &pluginbridge.StdioServer{
		Adapter: openclaw.New(openclaw.Config{
			GatewayURL: *gatewayURL,
			HomeDir:    *homeDir,
		}),
		In:  os.Stdin,
		Out: os.Stdout,
	}
	if err := server.Run(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "openclaw pluginbridge plugin failed: %v\n", err)
		os.Exit(1)
	}
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
