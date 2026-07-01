package p2p

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/caduceus/caduceus/internal/config"
	"github.com/caduceus/caduceus/internal/logging"
	"github.com/caduceus/caduceus/internal/openai"
	"github.com/caduceus/caduceus/internal/security"
	"github.com/caduceus/caduceus/internal/store"
	"github.com/caduceus/caduceus/internal/tasks"
	"github.com/caduceus/caduceus/internal/workers"
	"github.com/caduceus/caduceus/pkg/caduceus"
	"github.com/libp2p/go-libp2p"
	p2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"
	"github.com/libp2p/go-libp2p/p2p/security/noise"
)

type Node struct {
	cfg       config.Config
	host      host.Host
	groupHash string
	allowed   security.AllowedPeers
	store     *store.Store
	ai        *openai.Client
	log       *logging.Logger

	mu      sync.RWMutex
	workers map[string]workers.Worker
	cancels map[string]context.CancelFunc

	mdns io.Closer
}

type Options struct {
	Config    config.Config
	Identity  p2pcrypto.PrivKey
	GroupHash string
	Allowed   security.AllowedPeers
	Store     *store.Store
	OpenAI    *openai.Client
	Logger    *logging.Logger
}

func New(ctx context.Context, opts Options) (*Node, error) {
	if opts.Identity == nil {
		return nil, errors.New("libp2p identity is required")
	}
	if opts.Store == nil {
		return nil, errors.New("store is required")
	}
	if opts.OpenAI == nil {
		return nil, errors.New("openai client is required")
	}
	listenAddrs := opts.Config.Node.ListenAddrs
	if len(listenAddrs) == 0 {
		listenAddrs = []string{"/ip4/0.0.0.0/tcp/0"}
	}
	h, err := libp2p.New(
		libp2p.Identity(opts.Identity),
		libp2p.ListenAddrStrings(listenAddrs...),
		libp2p.Security(noise.ID, noise.New),
	)
	if err != nil {
		return nil, err
	}
	n := &Node{
		cfg:       opts.Config,
		host:      h,
		groupHash: opts.GroupHash,
		allowed:   opts.Allowed,
		store:     opts.Store,
		ai:        opts.OpenAI,
		log:       opts.Logger,
		workers:   map[string]workers.Worker{},
		cancels:   map[string]context.CancelFunc{},
	}
	h.SetStreamHandler(protocol.ID(WorkerProtocol), n.handleWorkerStream)
	h.SetStreamHandler(protocol.ID(TaskProtocol), n.handleTaskStream)
	h.SetStreamHandler(protocol.ID(EventsProtocol), func(s network.Stream) { _ = s.Close() })
	n.registerSelf(ctx)
	if opts.Config.Node.EnableMDNS {
		service := mdns.NewMdnsService(h, opts.Config.Node.MDNSServiceName, &discoveryNotifee{node: n})
		if err := service.Start(); err != nil {
			_ = h.Close()
			return nil, err
		}
		n.mdns = service
	}
	return n, nil
}

func (n *Node) Close() error {
	if n.mdns != nil {
		_ = n.mdns.Close()
	}
	if n.host != nil {
		return n.host.Close()
	}
	return nil
}

func (n *Node) HostID() string {
	return n.host.ID().String()
}

func (n *Node) ListenAddrs() []string {
	out := make([]string, 0, len(n.host.Addrs()))
	for _, addr := range n.host.Addrs() {
		out = append(out, addr.String())
	}
	sort.Strings(out)
	return out
}

func (n *Node) Status() map[string]any {
	return map[string]any{
		"peer_id":      n.HostID(),
		"name":         n.cfg.Node.Name,
		"listen_addrs": n.ListenAddrs(),
		"group_hash":   n.groupHash,
		"workers":      len(n.ListWorkers()),
		"version":      caduceus.Version,
	}
}

func (n *Node) ListWorkers() []workers.Worker {
	n.mu.RLock()
	defer n.mu.RUnlock()
	out := make([]workers.Worker, 0, len(n.workers))
	for _, worker := range n.workers {
		out = append(out, worker)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Local != out[j].Local {
			return out[i].Local
		}
		return out[i].WorkerID < out[j].WorkerID
	})
	return out
}

func (n *Node) GetWorker(workerID string) (workers.Worker, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	w, ok := n.workers[workerID]
	return w, ok
}

func (n *Node) ValidateTask(req tasks.Request) error {
	if strings.TrimSpace(req.Kind) == "" {
		req.Kind = tasks.KindPrompt
	}
	return tasks.ValidateRequest(req)
}

func (n *Node) RunTask(ctx context.Context, req tasks.Request) (tasks.Result, error) {
	if req.TaskID == "" {
		req.TaskID = tasks.NewID("task")
	}
	if req.Kind == "" {
		req.Kind = tasks.KindPrompt
	}
	if req.TrustLevel == "" {
		req.TrustLevel = n.cfg.Security.TrustLevelDefault
	}
	if req.TimeoutSeconds == 0 {
		req.TimeoutSeconds = n.cfg.OpenAI.TimeoutSeconds
	}
	if req.Task.Model == "" {
		req.Task.Model = n.cfg.OpenAI.DefaultModel
	}
	if err := n.ValidateTask(req); err != nil {
		return tasks.Result{}, err
	}
	worker := n.chooseWorker(req.WorkerID)
	if worker.WorkerID == "" {
		return tasks.Result{}, errors.New("no suitable worker found")
	}
	req.WorkerID = worker.WorkerID
	if worker.Local {
		return n.executeLocal(ctx, req, n.HostID(), n.HostID(), nil)
	}
	return n.runRemote(ctx, worker, req)
}

func (n *Node) CancelTask(ctx context.Context, taskID string) error {
	n.mu.Lock()
	cancel := n.cancels[taskID]
	n.mu.Unlock()
	if cancel != nil {
		cancel()
		return nil
	}
	meta, err := n.store.GetTask(taskID)
	if err != nil {
		return err
	}
	if meta.WorkerPeer == "" || meta.WorkerPeer == n.HostID() {
		return errors.New("task is not running locally")
	}
	id, err := peer.Decode(meta.WorkerPeer)
	if err != nil {
		return err
	}
	stream, err := n.host.NewStream(ctx, id, protocol.ID(TaskProtocol))
	if err != nil {
		return err
	}
	defer stream.Close()
	enc := json.NewEncoder(stream)
	env, err := n.envelope(TypeTaskCancel, CancelPayload{TaskID: taskID})
	if err != nil {
		return err
	}
	if err := enc.Encode(env); err != nil {
		return err
	}
	_, _ = n.store.UpdateTask(taskID, func(meta *tasks.Metadata) error {
		meta.Status = tasks.StatusCanceled
		now := time.Now().UTC()
		meta.FinishedAt = &now
		return nil
	})
	_ = n.store.AppendEvent(tasks.NewEvent(taskID, 0, tasks.EventCanceled, "cancel requested", ""))
	return nil
}

func (n *Node) runRemote(ctx context.Context, worker workers.Worker, req tasks.Request) (tasks.Result, error) {
	meta := tasks.Metadata{
		TaskID:        req.TaskID,
		Kind:          req.Kind,
		Status:        tasks.StatusQueued,
		RequesterPeer: n.HostID(),
		WorkerPeer:    worker.PeerID,
		CreatedAt:     time.Now().UTC(),
		Request:       req,
	}
	if err := n.store.SaveTask(meta); err != nil {
		return tasks.Result{}, err
	}
	id, err := peer.Decode(worker.PeerID)
	if err != nil {
		return tasks.Result{}, err
	}
	stream, err := n.host.NewStream(ctx, id, protocol.ID(TaskProtocol))
	if err != nil {
		return tasks.Result{}, err
	}
	defer stream.Close()
	enc := json.NewEncoder(stream)
	dec := json.NewDecoder(stream)
	env, err := n.envelope(TypeTaskRequest, req)
	if err != nil {
		return tasks.Result{}, err
	}
	if err := enc.Encode(env); err != nil {
		return tasks.Result{}, err
	}
	for {
		var incoming Envelope
		if err := dec.Decode(&incoming); err != nil {
			return tasks.Result{}, err
		}
		if err := n.verifyEnvelope(incoming, id); err != nil {
			return tasks.Result{}, err
		}
		switch incoming.Type {
		case TypeTaskAccept, TypeTaskEvent:
			var event tasks.Event
			if err := json.Unmarshal(incoming.Payload, &event); err != nil {
				return tasks.Result{}, err
			}
			_ = n.store.AppendEvent(event)
			_, _ = n.store.UpdateTask(req.TaskID, func(meta *tasks.Metadata) error {
				switch event.EventType {
				case tasks.EventAccepted:
					meta.Status = tasks.StatusAccepted
				case tasks.EventStarted, tasks.EventToken, tasks.EventProgress, tasks.EventArtifact:
					meta.Status = tasks.StatusRunning
					if meta.StartedAt == nil {
						now := event.Timestamp
						meta.StartedAt = &now
					}
				case tasks.EventCompleted:
					meta.Status = tasks.StatusCompleted
					now := event.Timestamp
					meta.FinishedAt = &now
				case tasks.EventFailed:
					meta.Status = tasks.StatusFailed
					meta.Error = event.Message
					now := event.Timestamp
					meta.FinishedAt = &now
				case tasks.EventCanceled:
					meta.Status = tasks.StatusCanceled
					now := event.Timestamp
					meta.FinishedAt = &now
				}
				return nil
			})
		case TypeTaskResult:
			var result tasks.Result
			if err := json.Unmarshal(incoming.Payload, &result); err != nil {
				return tasks.Result{}, err
			}
			if err := n.store.SaveResult(result); err != nil {
				return tasks.Result{}, err
			}
			_, _ = n.store.UpdateTask(req.TaskID, func(meta *tasks.Metadata) error {
				meta.Status = result.Status
				if result.Error != nil {
					meta.Error = fmt.Sprint(result.Error)
				}
				now := time.Now().UTC()
				meta.FinishedAt = &now
				return nil
			})
			return result, nil
		case TypeError:
			var payload ErrorPayload
			_ = json.Unmarshal(incoming.Payload, &payload)
			return tasks.Result{}, errors.New(payload.Message)
		}
	}
}

func (n *Node) executeLocal(ctx context.Context, req tasks.Request, requesterPeer, workerPeer string, send func(kind string, payload any) error) (tasks.Result, error) {
	if req.TaskID == "" {
		req.TaskID = tasks.NewID("task")
	}
	runCtx := ctx
	cancel := func() {}
	if req.TimeoutSeconds > 0 {
		runCtx, cancel = context.WithTimeout(ctx, time.Duration(req.TimeoutSeconds)*time.Second)
	} else {
		runCtx, cancel = context.WithCancel(ctx)
	}
	n.mu.Lock()
	n.cancels[req.TaskID] = cancel
	n.mu.Unlock()
	defer func() {
		cancel()
		n.mu.Lock()
		delete(n.cancels, req.TaskID)
		n.mu.Unlock()
	}()

	now := time.Now().UTC()
	meta := tasks.Metadata{
		TaskID:        req.TaskID,
		Kind:          req.Kind,
		Status:        tasks.StatusAccepted,
		RequesterPeer: requesterPeer,
		WorkerPeer:    workerPeer,
		CreatedAt:     now,
		Request:       req,
	}
	if err := n.store.SaveTask(meta); err != nil {
		return tasks.Result{}, err
	}
	if err := n.emit(req.TaskID, tasks.EventAccepted, "task accepted", "", TypeTaskAccept, send); err != nil {
		return tasks.Result{}, err
	}
	_, _ = n.store.UpdateTask(req.TaskID, func(meta *tasks.Metadata) error {
		meta.Status = tasks.StatusRunning
		now := time.Now().UTC()
		meta.StartedAt = &now
		return nil
	})
	if err := n.emit(req.TaskID, tasks.EventStarted, "task started", "", TypeTaskEvent, send); err != nil {
		return tasks.Result{}, err
	}
	result, err := n.ai.Chat(runCtx, req.TaskID, req.Task, func(event tasks.Event) error {
		if event.EventType == "" {
			event.EventType = tasks.EventToken
		}
		if event.Timestamp.IsZero() {
			event.Timestamp = time.Now().UTC()
		}
		if err := n.store.AppendEvent(event); err != nil {
			return err
		}
		if send != nil {
			return send(TypeTaskEvent, event)
		}
		return nil
	})
	if err != nil {
		status := tasks.StatusFailed
		eventType := tasks.EventFailed
		message := err.Error()
		if errors.Is(runCtx.Err(), context.Canceled) || errors.Is(runCtx.Err(), context.DeadlineExceeded) {
			status = tasks.StatusCanceled
			eventType = tasks.EventCanceled
			message = "task canceled"
		}
		result = tasks.Result{TaskID: req.TaskID, Status: status, Error: message, Artifacts: []tasks.Artifact{}}
		_ = n.store.SaveResult(result)
		_, _ = n.store.UpdateTask(req.TaskID, func(meta *tasks.Metadata) error {
			meta.Status = status
			meta.Error = message
			now := time.Now().UTC()
			meta.FinishedAt = &now
			return nil
		})
		_ = n.emit(req.TaskID, eventType, message, "", TypeTaskEvent, send)
		if send != nil {
			_ = send(TypeTaskResult, result)
		}
		return result, err
	}
	result.TaskID = req.TaskID
	result.Status = tasks.StatusCompleted
	if result.Artifacts == nil {
		result.Artifacts = []tasks.Artifact{}
	}
	if err := n.store.SaveResult(result); err != nil {
		return tasks.Result{}, err
	}
	_, _ = n.store.UpdateTask(req.TaskID, func(meta *tasks.Metadata) error {
		meta.Status = tasks.StatusCompleted
		now := time.Now().UTC()
		meta.FinishedAt = &now
		return nil
	})
	if err := n.emit(req.TaskID, tasks.EventCompleted, "task completed", "", TypeTaskEvent, send); err != nil {
		return tasks.Result{}, err
	}
	if send != nil {
		if err := send(TypeTaskResult, result); err != nil {
			return tasks.Result{}, err
		}
	}
	return result, nil
}

func (n *Node) emit(taskID, eventType, message, delta, envelopeType string, send func(kind string, payload any) error) error {
	event := tasks.NewEvent(taskID, 0, eventType, message, delta)
	if err := n.store.AppendEvent(event); err != nil {
		return err
	}
	if send != nil {
		return send(envelopeType, event)
	}
	return nil
}

func (n *Node) handleWorkerStream(stream network.Stream) {
	defer stream.Close()
	remote := stream.Conn().RemotePeer()
	dec := json.NewDecoder(stream)
	enc := json.NewEncoder(stream)
	var env Envelope
	if err := dec.Decode(&env); err != nil {
		return
	}
	if err := n.verifyEnvelope(env, remote); err != nil {
		_ = n.sendError(enc, "peer_rejected", err.Error())
		return
	}
	if env.Type != TypeWorkerHello {
		_ = n.sendError(enc, "bad_message", "expected worker_hello")
		return
	}
	var worker workers.Worker
	if err := json.Unmarshal(env.Payload, &worker); err != nil {
		_ = n.sendError(enc, "bad_payload", err.Error())
		return
	}
	n.registerWorker(worker, remote.String())
	self := n.selfWorker(context.Background())
	reply, err := n.envelope(TypeWorkerHello, self)
	if err == nil {
		_ = enc.Encode(reply)
	}
}

func (n *Node) handleTaskStream(stream network.Stream) {
	defer stream.Close()
	remote := stream.Conn().RemotePeer()
	dec := json.NewDecoder(stream)
	enc := json.NewEncoder(stream)
	var env Envelope
	if err := dec.Decode(&env); err != nil {
		return
	}
	if err := n.verifyEnvelope(env, remote); err != nil {
		_ = n.sendError(enc, "peer_rejected", err.Error())
		return
	}
	switch env.Type {
	case TypeTaskRequest:
		var req tasks.Request
		if err := json.Unmarshal(env.Payload, &req); err != nil {
			_ = n.sendError(enc, "bad_payload", err.Error())
			return
		}
		if req.TaskID == "" {
			req.TaskID = tasks.NewID("task")
		}
		if req.Kind == "" {
			req.Kind = tasks.KindPrompt
		}
		if req.Task.Model == "" {
			req.Task.Model = n.cfg.OpenAI.DefaultModel
		}
		if err := n.ValidateTask(req); err != nil {
			_ = n.sendError(enc, "invalid_task", err.Error())
			return
		}
		send := func(kind string, payload any) error {
			reply, err := n.envelope(kind, payload)
			if err != nil {
				return err
			}
			return enc.Encode(reply)
		}
		_, err := n.executeLocal(context.Background(), req, remote.String(), n.HostID(), send)
		if err != nil {
			return
		}
	case TypeTaskCancel:
		var payload CancelPayload
		if err := json.Unmarshal(env.Payload, &payload); err != nil {
			_ = n.sendError(enc, "bad_payload", err.Error())
			return
		}
		n.mu.Lock()
		cancel := n.cancels[payload.TaskID]
		n.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		event := tasks.NewEvent(payload.TaskID, 0, tasks.EventCanceled, "cancel requested by remote peer", "")
		_ = n.store.AppendEvent(event)
		reply, _ := n.envelope(TypeTaskEvent, event)
		_ = enc.Encode(reply)
	default:
		_ = n.sendError(enc, "bad_message", "unsupported task message type")
	}
}

func (n *Node) sendError(enc *json.Encoder, code, message string) error {
	env, err := n.envelope(TypeError, ErrorPayload{Code: code, Message: message})
	if err != nil {
		return err
	}
	return enc.Encode(env)
}

func (n *Node) envelope(kind string, payload any) (Envelope, error) {
	return newEnvelope(kind, n.HostID(), n.groupHash, payload)
}

func (n *Node) verifyEnvelope(env Envelope, remote peer.ID) error {
	if env.GroupHash != n.groupHash {
		return errors.New("group hash mismatch")
	}
	if env.SenderPeerID != "" && env.SenderPeerID != remote.String() {
		return errors.New("sender peer id mismatch")
	}
	if remote.String() == n.HostID() {
		return nil
	}
	if !n.allowed.IsAllowed(remote.String(), n.cfg.Security.RequireAllowlist) {
		return errors.New("peer is not in allowlist")
	}
	return nil
}

func (n *Node) chooseWorker(workerID string) workers.Worker {
	workerID = strings.TrimSpace(workerID)
	n.mu.RLock()
	defer n.mu.RUnlock()
	if workerID != "" && workerID != "auto" {
		return n.workers[workerID]
	}
	for _, worker := range n.workers {
		if !worker.Local && worker.Allowed {
			return worker
		}
	}
	return n.workers[n.HostID()]
}

func (n *Node) registerSelf(ctx context.Context) {
	worker := n.selfWorker(ctx)
	n.mu.Lock()
	n.workers[worker.WorkerID] = worker
	n.mu.Unlock()
}

func (n *Node) selfWorker(ctx context.Context) workers.Worker {
	models := []string{n.cfg.OpenAI.DefaultModel}
	modelCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if got, err := n.ai.ListModels(modelCtx); err == nil && len(got) > 0 {
		models = got
	}
	return workers.Worker{
		WorkerID: n.HostID(),
		PeerID:   n.HostID(),
		Name:     n.cfg.Node.Name,
		Capabilities: workers.Capabilities{
			LLM:                n.cfg.Worker.Capabilities.LLM,
			Streaming:          n.cfg.Worker.Capabilities.Streaming,
			Artifacts:          n.cfg.Worker.Capabilities.Artifacts,
			Models:             models,
			MaxConcurrentTasks: n.cfg.Worker.MaxConcurrentTasks,
			Tools:              n.cfg.Worker.Capabilities.Tools,
		},
		ListenAddrs: n.ListenAddrs(),
		Labels:      n.cfg.Worker.Labels,
		Version:     caduceus.Version,
		GroupHash:   n.groupHash,
		LastSeen:    time.Now().UTC(),
		Local:       true,
		Allowed:     true,
		TrustLevel:  n.cfg.Security.TrustLevelDefault,
	}
}

func (n *Node) registerWorker(worker workers.Worker, peerID string) {
	if worker.WorkerID == "" {
		worker.WorkerID = peerID
	}
	worker.PeerID = peerID
	worker.GroupHash = n.groupHash
	worker.LastSeen = time.Now().UTC()
	worker.Local = peerID == n.HostID()
	allowed, ok := n.allowed.Get(peerID)
	worker.Allowed = n.allowed.IsAllowed(peerID, n.cfg.Security.RequireAllowlist)
	if ok && allowed.TrustLevel != "" {
		worker.TrustLevel = allowed.TrustLevel
	} else {
		worker.TrustLevel = n.cfg.Security.TrustLevelDefault
	}
	if !worker.Allowed {
		return
	}
	n.mu.Lock()
	n.workers[worker.WorkerID] = worker
	n.mu.Unlock()
}

type discoveryNotifee struct {
	node *Node
}

func (d *discoveryNotifee) HandlePeerFound(info peer.AddrInfo) {
	if d == nil || d.node == nil {
		return
	}
	if info.ID == d.node.host.ID() {
		return
	}
	if d.node.cfg.Security.RequireAllowlist && !d.node.allowed.IsAllowed(info.ID.String(), true) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	d.node.host.Peerstore().AddAddrs(info.ID, info.Addrs, time.Hour)
	if err := d.node.host.Connect(ctx, info); err != nil {
		if d.node.log != nil {
			d.node.log.Debug("mdns peer connect failed", "peer", info.ID.String(), "error", err.Error())
		}
		return
	}
	stream, err := d.node.host.NewStream(ctx, info.ID, protocol.ID(WorkerProtocol))
	if err != nil {
		return
	}
	defer stream.Close()
	enc := json.NewEncoder(stream)
	dec := json.NewDecoder(stream)
	env, err := d.node.envelope(TypeWorkerHello, d.node.selfWorker(ctx))
	if err != nil {
		return
	}
	if err := enc.Encode(env); err != nil {
		return
	}
	var reply Envelope
	if err := dec.Decode(&reply); err != nil {
		return
	}
	if err := d.node.verifyEnvelope(reply, info.ID); err != nil {
		return
	}
	if reply.Type != TypeWorkerHello {
		return
	}
	var worker workers.Worker
	if err := json.Unmarshal(reply.Payload, &worker); err != nil {
		return
	}
	d.node.registerWorker(worker, info.ID.String())
}
