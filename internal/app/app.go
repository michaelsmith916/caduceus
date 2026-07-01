package app

import (
	"context"
	"errors"
	"os"
	"strings"
	"time"

	"github.com/caduceus/caduceus/internal/config"
	"github.com/caduceus/caduceus/internal/control"
	cryptoutil "github.com/caduceus/caduceus/internal/crypto"
	"github.com/caduceus/caduceus/internal/localnode"
	"github.com/caduceus/caduceus/internal/logging"
	"github.com/caduceus/caduceus/internal/openai"
	"github.com/caduceus/caduceus/internal/p2p"
	"github.com/caduceus/caduceus/internal/security"
	"github.com/caduceus/caduceus/internal/store"
	"github.com/caduceus/caduceus/internal/tasks"
)

type App struct {
	ConfigPath string
	Cfg        config.Config
	GroupHash  string
	Store      *store.Store
	Node       *p2p.Node
	Control    *control.Server
	Log        *logging.Logger
}

func New(ctx context.Context, configPath string) (*App, error) {
	cfg, resolved, err := config.EnsureConfig(configPath)
	if err != nil {
		return nil, err
	}
	log := logging.New(cfg.Logging.Level, cfg.Logging.JSON)
	key, _, err := cryptoutil.LoadOrCreateSharedKey(cfg.Security.P2PKeyPath)
	if err != nil {
		return nil, err
	}
	groupHash, err := cryptoutil.HashSharedKey(key)
	if err != nil {
		return nil, err
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
	apiKey := os.Getenv(cfg.OpenAI.APIKeyEnv)
	ai := openai.New(cfg.OpenAI.BaseURL, apiKey, cfg.OpenAI.DefaultModel, cfg.OpenAITimeout())
	node, err := p2p.New(ctx, p2p.Options{
		Config:    cfg,
		Identity:  priv,
		GroupHash: groupHash,
		Allowed:   allowed,
		Store:     st,
		OpenAI:    ai,
		Logger:    log,
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
	taskReq := tasks.Request{
		TaskID:         tasks.NewID("task"),
		Kind:           tasks.KindPrompt,
		Task:           req.Task,
		Constraints:    req.Constraints,
		TimeoutSeconds: req.TimeoutSeconds,
		TrustLevel:     req.TrustLevel,
		WorkerID:       req.WorkerID,
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
	taskReq := tasks.Request{
		Kind:           tasks.KindPrompt,
		Task:           req.Task,
		Constraints:    req.Constraints,
		TimeoutSeconds: req.TimeoutSeconds,
		TrustLevel:     req.TrustLevel,
		WorkerID:       req.WorkerID,
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

func Wait(ctx context.Context) {
	<-ctx.Done()
	time.Sleep(100 * time.Millisecond)
}
