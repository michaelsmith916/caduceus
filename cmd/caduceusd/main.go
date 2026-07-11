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
	var configDir string
	var dataDir string
	var serviceName string
	var version bool
	flag.StringVar(&configPath, "config", "", "path to config.yaml")
	flag.StringVar(&configDir, "config-dir", "", "base directory for derived config paths")
	flag.StringVar(&dataDir, "data-dir", "", "base directory for derived data paths")
	flag.StringVar(&serviceName, "service-name", "Caduceus", "Windows service name")
	flag.BoolVar(&version, "version", false, "print version")
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "caduceusd %s\n\nUsage:\n  caduceusd [--config path] [--config-dir path] [--data-dir path] [--service-name name]\n\n", caduceus.Version)
		flag.PrintDefaults()
	}
	flag.Parse()
	if version {
		fmt.Println(caduceus.Version)
		return
	}
	if configDir != "" {
		if err := os.Setenv("CADUCEUS_CONFIG_DIR", configDir); err != nil {
			fmt.Fprintln(os.Stderr, "caduceusd:", err)
			os.Exit(1)
		}
	}
	if dataDir != "" {
		if err := os.Setenv("CADUCEUS_DATA_DIR", dataDir); err != nil {
			fmt.Fprintln(os.Stderr, "caduceusd:", err)
			os.Exit(1)
		}
	}
	if handled, err := runWindowsService(serviceName, configPath); handled {
		if err != nil {
			fmt.Fprintln(os.Stderr, "caduceusd:", err)
			os.Exit(1)
		}
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
