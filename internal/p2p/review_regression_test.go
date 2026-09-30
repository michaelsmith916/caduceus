package p2p

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/caduceus/caduceus/internal/admission"
	"github.com/caduceus/caduceus/internal/store"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/caduceus/caduceus/internal/enrollment"
	"github.com/caduceus/caduceus/internal/registry"
	"github.com/caduceus/caduceus/internal/security"
	"github.com/caduceus/caduceus/internal/tasks"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	ma "github.com/multiformats/go-multiaddr"
)

// Stop background loops before replacing test clocks/registries. Tests drive
// advertisements and recovery explicitly while retaining real Noise streams.
func reviewQuietNode(t *testing.T) *Node {
	t.Helper()
	node := newEnrollmentTestNode(t, testGroupHash)
	node.phase2.close()
	runtime, err := newPhase2Runtime(context.Background(), node)
	if err != nil {
		t.Fatal(err)
	}
	node.phase2 = runtime
	node.registerSelf(context.Background())
	return node
}

func reviewConnect(t *testing.T, a, b *Node) {
	t.Helper()
	for _, pair := range [][2]*Node{{a, b}, {b, a}} {
		fingerprint, err := fingerprintPublicKey(pair[1].host.Peerstore().PubKey(pair[1].host.ID()))
		if err != nil {
			t.Fatal(err)
		}
		pair[0].mu.RLock()
		allowed := pair[0].allowed
		allowed.Peers = append(append([]security.AllowedPeer(nil), allowed.Peers...), security.AllowedPeer{PeerID: pair[1].HostID(), Allowed: true, TrustLevel: "trusted-lan", PublicKeyFingerprint: fingerprint})
		pair[0].mu.RUnlock()
		pair[0].UpdateAllowedPeers(allowed)
	}
	address, err := ma.NewMultiaddr(enrollmentTestAddress(t, b))
	if err != nil {
		t.Fatal(err)
	}
	info, err := peer.AddrInfoFromP2pAddr(address)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.host.Connect(context.Background(), *info); err != nil {
		t.Fatal(err)
	}
	a.registerWorker(b.selfWorker(context.Background()), b.HostID())
	b.registerWorker(a.selfWorker(context.Background()), a.HostID())
}

func reviewTaskHandler(node *Node, lose bool, observers ...*Node) network.StreamHandler {
	return func(stream network.Stream) {
		defer stream.Close()
		var env Envelope
		if err := newEnvelopeDecoder(stream).Decode(&env); err != nil {
			return
		}
		var assignment TaskAssignment
		if json.Unmarshal(env.Payload, &assignment) != nil {
			return
		}
		attempt := assignment.Attempt
		accepted, _ := node.envelopeForProtocol(stream.Protocol(), TypeTaskAccept, TaskAcceptance{TaskID: attempt.TaskID, AttemptID: attempt.AttemptID, AttemptToken: attempt.AttemptToken, WorkerID: attempt.WorkerID, SessionID: attempt.WorkerSession})
		encoder := json.NewEncoder(stream)
		if encoder.Encode(accepted) != nil {
			return
		}
		if lose {
			if len(observers) > 0 {
				deadline := time.Now().Add(time.Second)
				for time.Now().Before(deadline) {
					meta, err := observers[0].store.GetTask(attempt.TaskID)
					if err == nil && meta.Status == tasks.StatusRunning {
						break
					}
					time.Sleep(time.Millisecond)
				}
			}
			_ = stream.Reset()
			return
		}
		result := tasks.Result{TaskID: attempt.TaskID, AttemptID: attempt.AttemptID, AttemptToken: attempt.AttemptToken, Status: tasks.StatusCompleted, Artifacts: []tasks.Artifact{}}
		reply, _ := node.envelopeForProtocol(stream.Protocol(), TypeTaskResult, TaskResultPayload{AttemptID: attempt.AttemptID, AttemptToken: attempt.AttemptToken, Result: result})
		_ = encoder.Encode(reply)
	}
}

func TestReviewLiveWorkerLossFailsOver(t *testing.T) {
	for _, idempotent := range []bool{true, false} {
		t.Run(map[bool]string{true: "idempotent", false: "non_idempotent"}[idempotent], func(t *testing.T) {
			requester := reviewQuietNode(t)
			a := reviewQuietNode(t)
			b := reviewQuietNode(t)
			reviewConnect(t, requester, a)
			reviewConnect(t, requester, b)
			a.host.SetStreamHandler(protocol.ID(TaskProtocolV2), reviewTaskHandler(a, true, requester))
			b.host.SetStreamHandler(protocol.ID(TaskProtocolV2), reviewTaskHandler(b, false))
			requester.phase2.markAssigned(b.HostID())
			req := requester.prepareTaskRequest(tasks.Request{TaskID: "review-failover", Task: tasks.PromptTask{Prompt: "hello"}, Idempotent: idempotent, MaxAttempts: map[bool]int{true: 2, false: 1}[idempotent], Constraints: tasks.Constraints{AllowedWorkerIDs: []string{a.HostID(), b.HostID()}}})
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			result, err := requester.dispatchTask(ctx, req)
			meta, getErr := requester.store.GetTask(req.TaskID)
			if getErr != nil {
				t.Fatal(getErr)
			}
			if !idempotent {
				if err == nil || len(meta.Attempts) != 1 || meta.Attempts[0].WorkerID != a.HostID() {
					t.Fatalf("unsafe retry: result=%+v err=%v attempts=%+v", result, err, meta.Attempts)
				}
				return
			}
			if err != nil || result.Status != tasks.StatusCompleted || len(meta.Attempts) != 2 {
				t.Fatalf("failover: result=%+v err=%v attempts=%+v", result, err, meta.Attempts)
			}
			if meta.Attempts[0].State != tasks.AttemptStateInterrupted || meta.Attempts[0].FinishedAt == nil || meta.Attempts[1].WorkerID != b.HostID() {
				t.Fatalf("history=%+v", meta.Attempts)
			}
			if err := requester.validateAttemptCommit(meta.Attempts[0]); !errors.Is(err, errAttemptFenced) {
				t.Fatalf("old attempt not fenced: %v", err)
			}
		})
	}
}

func TestReviewStatusPollCannotRestoreRevokedPeer(t *testing.T) {
	coordinator := reviewQuietNode(t)
	applicant := reviewQuietNode(t)
	path := filepath.Join(t.TempDir(), "enrollment.json")
	allowedPath := filepath.Join(t.TempDir(), "allowed.json")
	options := enrollment.Options{Enabled: true, StatePath: path, AllowedCIDRs: []string{"127.0.0.0/8"}, RequireApproval: false, PersistApproval: func(request enrollment.Request) error {
		return security.AddPeer(allowedPath, security.AllowedPeer{PeerID: request.PeerID, Allowed: true, PublicKeyFingerprint: request.PublicKeyFingerprint, TrustLevel: "trusted-lan"})
	}}
	manager, err := enrollment.New(options)
	if err != nil {
		t.Fatal(err)
	}
	invitation, err := manager.CreateInvitation()
	if err != nil {
		t.Fatal(err)
	}
	fingerprint, _ := fingerprintPublicKey(applicant.host.Peerstore().PubKey(applicant.host.ID()))
	request, err := manager.Submit("127.0.0.1", enrollment.Submission{Token: invitation.Token, PeerID: applicant.HostID(), DisplayName: "review applicant", PublicKeyFingerprint: fingerprint, Nonce: "review-nonce"})
	if err != nil {
		t.Fatal(err)
	}
	coordinator.ActivateEnrollment(request)
	// Exercise the documented peers-remove + process restart path, loading
	// finalized enrollment history and the now-empty durable allowlist.
	if err := security.RemovePeer(allowedPath, applicant.HostID()); err != nil {
		t.Fatal(err)
	}
	cfg, identity := coordinator.cfg, coordinator.host.Peerstore().PrivKey(coordinator.host.ID())
	if err := coordinator.Close(); err != nil {
		t.Fatal(err)
	}
	manager, err = enrollment.New(options)
	if err != nil {
		t.Fatal(err)
	}
	allowed, err := security.LoadAllowedPeers(allowedPath)
	if err != nil {
		t.Fatal(err)
	}
	restartedStore := store.NewForRequester(cfg.Storage.DataDir, coordinator.HostID())
	if err := restartedStore.Init(); err != nil {
		t.Fatal(err)
	}
	coordinator, err = New(context.Background(), Options{Config: cfg, Identity: identity, GroupHash: testGroupHash, Allowed: allowed, Store: restartedStore, OpenAI: coordinator.ai, Enrollment: manager})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = coordinator.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := applicant.EnrollmentStatus(ctx, enrollmentTestAddress(t, coordinator), request.ID); err != nil {
		t.Fatal(err)
	}
	if _, _, allowed := coordinator.allowedPeer(applicant.HostID()); allowed {
		t.Fatal("historical approval resurrected revoked authorization")
	}
}

func TestReviewEvictedWorkerRenewsSessionWithoutRestart(t *testing.T) {
	coordinator := reviewQuietNode(t)
	applicant := reviewQuietNode(t)
	now := time.Now().UTC()
	reg, err := registry.New(registry.Options{Now: func() time.Time { return now }, SuspectAfter: time.Second, EvictAfter: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	coordinator.phase2.registry = reg
	reviewConnect(t, applicant, coordinator)
	old := applicant.selfWorker(context.Background())
	now = now.Add(3 * time.Second)
	if len(reg.Sweep()) != 1 {
		t.Fatal("worker not evicted")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := applicant.phase2.sendStatus(ctx, applicant, coordinator.selfWorker(ctx), applicant.phase2.localStatus(applicant)); err != nil {
		t.Fatalf("rejoin failed: %v", err)
	}
	current, ok := reg.Get(applicant.HostID())
	if !ok || current.SessionID == old.SessionID || current.Sequence == 0 {
		t.Fatalf("session not renewed: %+v", current)
	}
	if err := reg.Register(old); !errors.Is(err, registry.ErrOldSession) {
		t.Fatalf("retired session admitted: %v", err)
	}
}

func TestReviewConflictingPendingInboundClaims(t *testing.T) {
	node := reviewQuietNode(t)
	req := node.prepareTaskRequest(tasks.Request{TaskID: "review-claims", Task: tasks.PromptTask{Prompt: "hello"}, Idempotent: true, MaxAttempts: 2})
	first, _ := tasks.NewAttempt(req.TaskID, node.HostID(), node.phase2.sessionID, 1)
	second, _ := tasks.NewAttempt(req.TaskID, node.HostID(), node.phase2.sessionID, 2)
	claimed, err := node.claimInboundAttempt("requester", TaskAssignment{Request: req, Attempt: first}, func() {})
	if err != nil || !claimed {
		t.Fatalf("first claim: %t %v", claimed, err)
	}
	claimed, err = node.claimInboundAttempt("requester", TaskAssignment{Request: req, Attempt: second}, func() {})
	if err != nil || claimed {
		t.Fatalf("conflicting pending claim admitted: %t %v", claimed, err)
	}
	node.releaseInboundAttempt(second)
	if !node.cancelInboundAttempt("requester", CancelPayload{TaskID: req.TaskID, AttemptID: first.AttemptID, AttemptToken: first.AttemptToken}) {
		t.Fatal("nonowner released task claim")
	}
	node.releaseInboundAttempt(first)
	claimed, err = node.claimInboundAttempt("requester", TaskAssignment{Request: req, Attempt: second}, func() {})
	if err != nil || !claimed {
		t.Fatalf("claim not released: %t %v", claimed, err)
	}
	node.releaseInboundAttempt(second)
}

func TestReviewRecoveredQueueWaitsForDiscovery(t *testing.T) {
	requester := reviewQuietNode(t)
	worker := reviewQuietNode(t)
	worker.host.SetStreamHandler(protocol.ID(TaskProtocolV2), reviewTaskHandler(worker, false))
	req := requester.prepareTaskRequest(tasks.Request{TaskID: "review-recovered", TimeoutSeconds: 2, Task: tasks.PromptTask{Prompt: "hello"}, Constraints: tasks.Constraints{AllowedWorkerIDs: []string{worker.HostID()}}})
	if err := requester.store.SaveTask(tasks.Metadata{TaskID: req.TaskID, Kind: req.Kind, Status: tasks.StatusQueued, RequesterPeer: requester.HostID(), Request: req}); err != nil {
		t.Fatal(err)
	}
	if _, err := requester.store.Enqueue(req.TaskID, requester.HostID()); err != nil {
		t.Fatal(err)
	}
	requester.resumePersistedQueue()
	time.Sleep(150 * time.Millisecond)
	if _, queued, err := requester.store.QueuePosition(req.TaskID); err != nil || !queued {
		t.Fatalf("recovered task removed before discovery: %t %v", queued, err)
	}
	reviewConnect(t, requester, worker)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		meta, err := requester.store.GetTask(req.TaskID)
		if err != nil {
			t.Fatal(err)
		}
		if meta.Status == tasks.StatusCompleted {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	meta, _ := requester.store.GetTask(req.TaskID)
	t.Fatalf("task did not resume: %+v", meta)
}

func TestReviewInboundAcceptanceRevalidatesAfterQueueWait(t *testing.T) {
	for _, kind := range []string{"cancellation", "newer_metadata", "renewed_session"} {
		t.Run(kind, func(t *testing.T) {
			node := reviewQuietNode(t)
			req := node.prepareTaskRequest(tasks.Request{TaskID: "review-atomic", Task: tasks.PromptTask{Prompt: "hello"}, Idempotent: true, MaxAttempts: 2})
			first, _ := tasks.NewAttempt(req.TaskID, node.HostID(), node.phase2.currentSession(), 1)
			assignment := TaskAssignment{Request: req, Attempt: first}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			claimed, err := node.claimInboundAttempt("requester", assignment, cancel)
			if err != nil || !claimed {
				t.Fatalf("claim %t %v", claimed, err)
			}
			switch kind {
			case "cancellation":
				cancel()
			case "renewed_session":
				node.phase2.renewSession(node, first.WorkerSession)
			case "newer_metadata":
				second, _ := tasks.NewAttempt(req.TaskID, node.HostID(), first.WorkerSession, 2)
				second.State = tasks.AttemptStateCompleted
				err := node.store.SaveTask(tasks.Metadata{TaskID: req.TaskID, Kind: req.Kind, Status: tasks.StatusCompleted, RequesterPeer: "requester", Request: req, Attempts: []tasks.TaskAttempt{second}})
				if err != nil {
					t.Fatal(err)
				}
			}
			if _, err := node.acceptInboundAttempt(ctx, "requester", assignment); !errors.Is(err, errAttemptFenced) {
				t.Fatalf("queued claim was not revalidated: %v", err)
			}
			if kind == "newer_metadata" {
				meta, _ := node.store.GetTask(req.TaskID)
				if meta.Status != tasks.StatusCompleted || len(meta.Attempts) != 1 {
					t.Fatalf("newer metadata overwritten: %+v", meta)
				}
			}
			node.releaseInboundAttempt(first)
		})
	}
}

func TestReviewRecoveredQueueCancellationAndDeadlineAdvanceFIFO(t *testing.T) {
	for _, kind := range []string{"cancel", "deadline"} {
		t.Run(kind, func(t *testing.T) {
			requester := reviewQuietNode(t)
			worker := reviewQuietNode(t)
			worker.host.SetStreamHandler(protocol.ID(TaskProtocolV2), reviewTaskHandler(worker, false))
			reviewConnect(t, requester, worker)
			for i, id := range []string{"review-unavailable", "review-after"} {
				allowed := []string{"missing-worker"}
				if i == 1 {
					allowed = []string{worker.HostID()}
				}
				req := requester.prepareTaskRequest(tasks.Request{TaskID: id, TimeoutSeconds: 1, Task: tasks.PromptTask{Prompt: "hello"}, Constraints: tasks.Constraints{AllowedWorkerIDs: allowed}})
				if err := requester.store.SaveTask(tasks.Metadata{TaskID: id, Kind: req.Kind, Status: tasks.StatusQueued, RequesterPeer: requester.HostID(), Request: req}); err != nil {
					t.Fatal(err)
				}
				if _, err := requester.store.Enqueue(id, requester.HostID()); err != nil {
					t.Fatal(err)
				}
			}
			requester.resumePersistedQueue()
			if kind == "cancel" {
				if err := requester.CancelTask(context.Background(), "review-unavailable"); err != nil {
					t.Fatal(err)
				}
			}
			deadline := time.Now().Add(2500 * time.Millisecond)
			for time.Now().Before(deadline) {
				second, _ := requester.store.GetTask("review-after")
				if second.Status == tasks.StatusCompleted {
					first, _ := requester.store.GetTask("review-unavailable")
					if first.Status != tasks.StatusCanceled || len(first.Attempts) != 0 {
						t.Fatalf("unavailable task ran: %+v", first)
					}
					return
				}
				time.Sleep(10 * time.Millisecond)
			}
			t.Fatal("cancellation/deadline stalled FIFO")
		})
	}
}

func TestReviewSessionRenewalIgnoresDelayedChallenge(t *testing.T) {
	node := reviewQuietNode(t)
	retired := node.phase2.currentSession()
	node.phase2.renewSession(node, retired)
	current := node.phase2.currentSession()
	node.phase2.renewSession(node, retired)
	if current == retired || node.phase2.currentSession() != current {
		t.Fatal("delayed response rotated current session")
	}
}

func TestReviewMutualEvictionRejoinsOverAuthenticatedConnections(t *testing.T) {
	a, b := reviewQuietNode(t), reviewQuietNode(t)
	reviewConnect(t, a, b)
	oldA, oldB := a.phase2.currentSession(), b.phase2.currentSession()
	a.phase2.registry.Remove(b.HostID())
	b.phase2.registry.Remove(a.HostID())
	// A's first rejoin can still see B's retired session. B's reciprocal
	// challenge renews that session; the next round finishes both registrations.
	a.phase2.advertise(a)
	b.phase2.advertise(b)
	a.phase2.advertise(a)
	seenB, okB := a.phase2.registry.Get(b.HostID())
	seenA, okA := b.phase2.registry.Get(a.HostID())
	if !okA || !okB || seenA.Sequence == 0 || seenB.Sequence == 0 || seenA.SessionID == oldA || seenB.SessionID == oldB {
		t.Fatalf("mutual rejoin failed: A=%+v B=%+v", seenA, seenB)
	}
}

func TestReviewSleepingNodeRenewsItsOwnExpiredSession(t *testing.T) {
	node := reviewQuietNode(t)
	old := node.phase2.currentSession()
	node.phase2.registry.Remove(node.HostID())
	node.phase2.advertise(node)
	current, ok := node.phase2.registry.Get(node.HostID())
	if !ok || current.SessionID == old || current.Sequence == 0 {
		t.Fatalf("local session stayed retired: %+v", current)
	}
}

func TestReviewConflictingStreamsCannotSharePendingTask(t *testing.T) {
	requester, worker := reviewQuietNode(t), reviewQuietNode(t)
	reviewConnect(t, requester, worker)
	worker.phase2.admission.Close()
	controller, err := admission.New(1, 2)
	if err != nil {
		t.Fatal(err)
	}
	worker.phase2.admission = controller
	blocker := worker.prepareTaskRequest(tasks.Request{TaskID: "review-blocker", Task: tasks.PromptTask{Prompt: "block"}})
	lease, rejection, err := controller.Acquire(context.Background(), blocker, worker.phase2.admissionSnapshot(worker, blocker))
	if err != nil || rejection != nil {
		t.Fatalf("blocker: %v %+v", err, rejection)
	}
	defer lease.Release()
	req := worker.prepareTaskRequest(tasks.Request{TaskID: "review-stream-claims", TimeoutSeconds: 3, Task: tasks.PromptTask{Prompt: "hello"}, Idempotent: true, MaxAttempts: 2})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	send := func(number int) (network.Stream, tasks.TaskAttempt) {
		attempt, err := tasks.NewAttempt(req.TaskID, worker.HostID(), worker.phase2.currentSession(), number)
		if err != nil {
			t.Fatal(err)
		}
		stream, err := requester.host.NewStream(ctx, worker.host.ID(), protocol.ID(TaskProtocolV2))
		if err != nil {
			t.Fatal(err)
		}
		_ = stream.SetDeadline(time.Now().Add(3 * time.Second))
		env, err := requester.envelopeForProtocol(stream.Protocol(), TypeTaskRequest, TaskAssignment{Request: req, Attempt: attempt})
		if err != nil {
			t.Fatal(err)
		}
		if err := json.NewEncoder(stream).Encode(env); err != nil {
			t.Fatal(err)
		}
		return stream, attempt
	}
	first, attempt := send(1)
	defer first.Close()
	deadline := time.Now().Add(time.Second)
	for controller.Stats().QueuedTasks != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if controller.Stats().QueuedTasks != 1 {
		t.Fatal("first assignment did not queue")
	}
	second, _ := send(2)
	defer second.Close()
	var reply Envelope
	if err := newEnvelopeDecoder(second).Decode(&reply); err != nil {
		t.Fatal(err)
	}
	var problem tasks.TaskRejection
	if reply.Type != TypeTaskReject || json.Unmarshal(reply.Payload, &problem) != nil || problem.Reason != tasks.RejectStaleAssignment {
		t.Fatalf("conflicting assignment response: %+v", reply)
	}
	if controller.Stats().QueuedTasks != 1 {
		t.Fatal("conflicting token consumed an admission queue slot")
	}
	if !worker.cancelInboundAttempt(requester.HostID(), CancelPayload{TaskID: req.TaskID, AttemptID: attempt.AttemptID, AttemptToken: attempt.AttemptToken}) {
		t.Fatal("owner could not cancel pending assignment")
	}
	if err := newEnvelopeDecoder(first).Decode(&reply); err != nil {
		t.Fatal(err)
	}
	if _, err := worker.store.GetTask(req.TaskID); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("canceled pending task was persisted/executed: %v", err)
	}
}
