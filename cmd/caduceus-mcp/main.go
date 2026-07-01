package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/caduceus/caduceus/internal/app"
	"github.com/caduceus/caduceus/internal/mcp"
	"github.com/caduceus/caduceus/pkg/caduceus"
)

func main() {
	var configPath string
	var version bool
	flag.StringVar(&configPath, "config", "", "path to config.yaml")
	flag.BoolVar(&version, "version", false, "print version")
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "caduceus-mcp %s\n\nUsage:\n  caduceus-mcp [--config path]\n\n", caduceus.Version)
		flag.PrintDefaults()
	}
	flag.Parse()
	if version {
		fmt.Println(caduceus.Version)
		return
	}
	client, err := app.ControlClient(configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "caduceus-mcp:", err)
		os.Exit(1)
	}
	server := mcp.NewServer(client, os.Stdin, os.Stdout)
	if err := server.Serve(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "caduceus-mcp:", err)
		os.Exit(1)
	}
}
