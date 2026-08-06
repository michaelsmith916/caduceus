package app

import (
	"context"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/caduceus/caduceus/internal/config"
	"github.com/caduceus/caduceus/internal/control"
	cryptoutil "github.com/caduceus/caduceus/internal/crypto"
	"github.com/caduceus/caduceus/internal/enrollment"
	"github.com/caduceus/caduceus/internal/localnode"
	"github.com/caduceus/caduceus/internal/logging"
	"github.com/caduceus/caduceus/internal/openai"
	"github.com/caduceus/caduceus/internal/p2p"
	"github.com/caduceus/caduceus/internal/security"
	"github.com/caduceus/caduceus/internal/store"
	"github.com/caduceus/caduceus/internal/tasks"
)

type App struct {
	mu         sync.RWMutex
	ConfigPath string
	Cfg        config.Config
	GroupHash  string
	Store      *store.Store
	Node       *p2p.Node
	Enrollment *enrollment.Manager
	Control    *control.Server
	Log        *logging.Logger
}

func New(ctx context.Context, configPath string) (*App, error) {
	cfg, resolved, err := config.EnsureConfig(configPath)
	if err != nil {
		return nil, err
	}
	log := logging.New(cfg.Logging.Level, cfg.Logging.JSON)
	groupHash := strings.ToLower(strings.TrimSpace(cfg.Security.P2PKeyHash))
	if !validGroupHash(groupHash) {
		key, _, keyErr := cryptoutil.LoadOrCreateSharedKey(cfg.Security.P2PKeyPath)
		if keyErr != nil {
			return nil, keyErr
		}
		groupHash, err = cryptoutil.HashSharedKey(key)
		if err != nil {
			return nil, err
		}
	}
	cfg.Security.P2PKeyHash = groupHash
	priv, peerID, _, err := localnode.LoadOrCreateIdentity(cfg.Node.PrivateKeyPath)
	if err != nil {
		return nil, err
	}
	cfg.Node.PeerID = peerID.String()
	allowed, err := security.LoadAllowedPeers(cfg.Security.AllowedPeersPath)
	if err != nil {
		return nil, err
	}
	st := store.New(cfg.Storage.DataDir)
	if err := st.Init(); err != nil {
		return nil, err
	}
	enrollmentCfg := cfg.Enrollment.TrustedLAN
	enrollmentManager, err := enrollment.New(enrollment.Options{
		Enabled:         enrollmentCfg.Enabled,
		StatePath:       filepath.Join(cfg.Storage.DataDir, "enrollment", "state.json"),
		RequestTTL:      enrollmentCfg.RequestTTL(),
		TokenTTL:        enrollmentCfg.TokenTTL(),
		RateLimit:       enrollmentCfg.RateLimit,
		RateWindow:      enrollmentCfg.RateWindow(),
		AllowedCIDRs:    append([]string(nil), enrollmentCfg.AllowedNetworks...),
		RequireApproval: enrollmentCfg.RequireApproval,
		PersistApproval: func(request enrollment.Request) error {
			return security.AddPeer(cfg.Security.AllowedPeersPath, security.AllowedPeer{
				PeerID:               request.PeerID,
				Name:                 request.DisplayName,
				PublicKeyFingerprint: request.PublicKeyFingerprint,
				TrustLevel:           "trusted-lan",
				Allowed:              true,
				Notes:                "approved trusted-LAN enrollment " + request.ID,
			})
		},
	})
	if err != nil {
		return nil, err
	}
	apiKey := os.Getenv(cfg.OpenAI.APIKeyEnv)
	ai := openai.New(cfg.OpenAI.BaseURL, apiKey, cfg.OpenAI.DefaultModel, cfg.OpenAITimeout())
	node, err := p2p.New(ctx, p2p.Options{
		Config:     cfg,
		Identity:   priv,
		GroupHash:  groupHash,
		Allowed:    allowed,
		Store:      st,
		OpenAI:     ai,
		Logger:     log,
		Enrollment: enrollmentManager,
	})
	if err != nil {
		return nil, err
	}
	token, err := control.EnsureAuthToken(cfg.Control.AuthTokenPath)
	if err != nil {
		_ = node.Close()
		return nil, err
	}
	app := &App{
		ConfigPath: resolved,
		Cfg:        cfg,
		GroupHash:  groupHash,
		Store:      st,
		Node:       node,
		Enrollment: enrollmentManager,
		Log:        log,
	}
	app.Control = control.NewServer(cfg.ControlEndpoint(), token, app)
	return app, nil
}

func (a *App) Start() error {
	if a.Control == nil {
		return errors.New("control server is not configured")
	}
	if err := a.Control.Start(); err != nil {
		return err
	}
	if a.Log != nil {
		a.Log.Info("caduceusd started", "peer_id", a.Node.HostID(), "control", a.Cfg.ControlEndpoint())
	}
	return nil
}

func (a *App) Close(ctx context.Context) error {
	if a.Control != nil {
		_ = a.Control.Stop(ctx)
	}
	if a.Node != nil {
		return a.Node.Close()
	}
	return nil
}

func (a *App) Status(ctx context.Context) (any, error) {
	_ = ctx
	status := a.Node.Status()
	status["control_endpoint"] = a.Cfg.ControlEndpoint()
	status["config_path"] = a.ConfigPath
	status["data_dir"] = a.Cfg.Storage.DataDir
	return status, nil
}

func (a *App) Config(ctx context.Context) (any, error) {
	_ = ctx
	a.mu.RLock()
	defer a.mu.RUnlock()
	cfg := a.Cfg
	cfg.Security.P2PKeyHash = a.GroupHash
	return map[string]any{
		"config_path": a.ConfigPath,
		"config":      cfg,
	}, nil
}

func (a *App) ListWorkers(ctx context.Context) (any, error) {
	_ = ctx
	return map[string]any{"workers": a.Node.ListWorkers()}, nil
}

func (a *App) GetWorker(ctx context.Context, workerID string) (any, error) {
	_ = ctx
	worker, ok := a.Node.GetWorker(workerID)
	if !ok {
		return nil, errors.New("worker not found")
	}
	return worker, nil
}

func (a *App) RunTask(ctx context.Context, req control.RunTaskRequest) (any, error) {
	taskID := strings.TrimSpace(req.TaskID)
	if taskID == "" {
		taskID = tasks.NewID("task")
	}
	kind := strings.TrimSpace(req.Kind)
	if kind == "" {
		kind = tasks.KindPrompt
	}
	taskReq := tasks.Request{
		TaskID:         taskID,
		Kind:           kind,
		Task:           req.Task,
		Constraints:    req.Constraints,
		TimeoutSeconds: req.TimeoutSeconds,
		TrustLevel:     req.TrustLevel,
		WorkerID:       req.WorkerID,
		Idempotent:     req.Idempotent,
		MaxAttempts:    req.MaxAttempts,
	}
	result, err := a.Node.RunTask(ctx, taskReq)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"task_id": result.TaskID,
		"status":  result.Status,
		"result":  result,
	}, nil
}

func (a *App) ListTasks(ctx context.Context, filter control.ListTasksFilter) (any, error) {
	_ = ctx
	all, err := a.Store.ListTasks()
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(filter.Status) == "" {
		return map[string]any{"tasks": all}, nil
	}
	out := make([]tasks.Metadata, 0, len(all))
	for _, task := range all {
		if task.Status == filter.Status {
			out = append(out, task)
		}
	}
	return map[string]any{"tasks": out}, nil
}

func (a *App) GetTaskStatus(ctx context.Context, taskID string) (any, error) {
	_ = ctx
	return a.Store.GetTask(taskID)
}

func (a *App) GetTaskResult(ctx context.Context, taskID string) (any, error) {
	_ = ctx
	return a.Store.GetResult(taskID)
}

func (a *App) GetTaskEvents(ctx context.Context, taskID string, cursor int64) (any, error) {
	_ = ctx
	events, err := a.Store.EventsSince(taskID, cursor)
	if err != nil {
		return nil, err
	}
	return map[string]any{"events": events}, nil
}

func (a *App) GetTaskArtifacts(ctx context.Context, taskID string) (any, error) {
	_ = ctx
	artifacts, err := a.Store.ListArtifacts(taskID)
	if err != nil {
		return nil, err
	}
	return map[string]any{"artifacts": artifacts}, nil
}

func (a *App) CancelTask(ctx context.Context, taskID string) (any, error) {
	if err := a.Node.CancelTask(ctx, taskID); err != nil {
		return nil, err
	}
	return map[string]any{"task_id": taskID, "status": tasks.StatusCanceled}, nil
}

func (a *App) ValidateTask(ctx context.Context, req control.RunTaskRequest) (any, error) {
	_ = ctx
	kind := strings.TrimSpace(req.Kind)
	if kind == "" {
		kind = tasks.KindPrompt
	}
	taskReq := tasks.Request{
		TaskID:         req.TaskID,
		Kind:           kind,
		Task:           req.Task,
		Constraints:    req.Constraints,
		TimeoutSeconds: req.TimeoutSeconds,
		TrustLevel:     req.TrustLevel,
		WorkerID:       req.WorkerID,
		Idempotent:     req.Idempotent,
		MaxAttempts:    req.MaxAttempts,
	}
	if err := a.Node.ValidateTask(taskReq); err != nil {
		return map[string]any{"valid": false, "error": err.Error()}, nil
	}
	return map[string]any{"valid": true}, nil
}

func ControlClient(configPath string) (*control.Client, error) {
	cfg, _, err := config.Load(configPath)
	if err != nil {
		return nil, err
	}
	token, err := control.EnsureAuthToken(cfg.Control.AuthTokenPath)
	if err != nil {
		return nil, err
	}
	return control.NewClient(cfg.ControlEndpoint(), token), nil
}

func validGroupHash(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 32 && strings.ToLower(value) == value
}

func Wait(ctx context.Context) {
	<-ctx.Done()
	time.Sleep(100 * time.Millisecond)
}
