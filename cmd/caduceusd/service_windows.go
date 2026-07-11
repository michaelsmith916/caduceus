//go:build windows

package main

import (
	"context"
	"fmt"
	"time"

	"github.com/caduceus/caduceus/internal/app"
	"golang.org/x/sys/windows/svc"
)

type serviceHandler struct {
	configPath string
	err        error
}

func runWindowsService(serviceName, configPath string) (bool, error) {
	isService, err := svc.IsWindowsService()
	if err != nil {
		return false, fmt.Errorf("detect Windows service session: %w", err)
	}
	if !isService {
		return false, nil
	}

	handler := &serviceHandler{configPath: configPath}
	if err := svc.Run(serviceName, handler); err != nil {
		return true, fmt.Errorf("run Windows service %q: %w", serviceName, err)
	}
	return true, handler.err
}

func (h *serviceHandler) Execute(_ []string, requests <-chan svc.ChangeRequest, status chan<- svc.Status) (bool, uint32) {
	status <- svc.Status{State: svc.StartPending}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	daemon, err := app.New(ctx, h.configPath)
	if err != nil {
		h.err = fmt.Errorf("initialize service: %w", err)
		return false, 1
	}
	if err := daemon.Start(); err != nil {
		h.err = fmt.Errorf("start service: %w", err)
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = daemon.Close(closeCtx)
		closeCancel()
		return false, 1
	}

	accepted := svc.AcceptStop | svc.AcceptShutdown
	current := svc.Status{State: svc.Running, Accepts: accepted}
	status <- current

	for request := range requests {
		switch request.Cmd {
		case svc.Interrogate:
			status <- current
		case svc.Stop, svc.Shutdown:
			status <- svc.Status{State: svc.StopPending}
			cancel()
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			h.err = daemon.Close(closeCtx)
			closeCancel()
			if h.err != nil {
				return false, 1
			}
			return false, 0
		}
	}

	return false, 0
}
