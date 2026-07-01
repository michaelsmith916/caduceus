package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/caduceus/caduceus/internal/app"
	"github.com/caduceus/caduceus/pkg/caduceus"
)

func main() {
	var configPath string
	var version bool
	flag.StringVar(&configPath, "config", "", "path to config.yaml")
	flag.BoolVar(&version, "version", false, "print version")
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "caduceusd %s\n\nUsage:\n  caduceusd [--config path]\n\n", caduceus.Version)
		flag.PrintDefaults()
	}
	flag.Parse()
	if version {
		fmt.Println(caduceus.Version)
		return
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	daemon, err := app.New(ctx, configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "caduceusd:", err)
		os.Exit(1)
	}
	if err := daemon.Start(); err != nil {
		fmt.Fprintln(os.Stderr, "caduceusd:", err)
		_ = daemon.Close(context.Background())
		os.Exit(1)
	}
	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = daemon.Close(shutdownCtx)
}
