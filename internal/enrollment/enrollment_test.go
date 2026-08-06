package enrollment

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestManagerDisabledByDefaultAndValidatesOptions(t *testing.T) {
	manager, err := New(Options{})
	if err != nil {
		t.Fatal(err)
	}
	if manager.Enabled() {
		t.Fatal("zero-value options must leave enrollment disabled")
	}
	if _, err := manager.CreateInvitation(); !errors.Is(err, ErrDisabled) {
		t.Fatalf("CreateInvitation error = %v, want ErrDisabled", err)
	}

	now := time.Now()
	_, err = New(Options{
		Enabled:         true,
		StatePath:       "",
		PersistApproval: func(Request) error { return nil },
		Now:             func() time.Time { return now },
	})
	if !errors.Is(err, ErrInvalidOptions) {
		t.Fatalf("missing state path error = %v, want ErrInvalidOptions", err)
	}
	_, err = New(Options{
		Enabled:         true,
		StatePath:       filepath.Join(t.TempDir(), "state.json"),
		RequestTTL:      -time.Second,
		PersistApproval: func(Request) error { return nil },
	})
	if !errors.Is(err, ErrInvalidOptions) {
		t.Fatalf("negative TTL error = %v, want ErrInvalidOptions", err)
	}
	_, err = New(Options{
		Enabled:         true,
		StatePath:       filepath.Join(t.TempDir(), "state.json"),
		AllowedCIDRs:    []string{"not-a-network"},
		PersistApproval: func(Request) error { return nil },
	})
	if !errors.Is(err, ErrInvalidOptions) {
		t.Fatalf("invalid CIDR error = %v, want ErrInvalidOptions", err)
	}
	_, err = New(Options{
		Enabled:   true,
		StatePath: filepath.Join(t.TempDir(), "state.json"),
	})
	if !errors.Is(err, ErrInvalidOptions) {
		t.Fatalf("missing approval callback error = %v, want ErrInvalidOptions", err)
	}
}

func TestSubmitPersistsSecretFreePendingRequestAndSafeViews(t *testing.T) {
	now := time.Date(2026, 8, 5, 18, 0, 0, 0, time.UTC)
	statePath := filepath.Join(t.TempDir(), "enrollment.json")
	manager := newTestManager(t, statePath, &now, func(Request) error { return nil }, nil)
	invitation := mustInvitation(t, manager)
	submission := testSubmission(invitation.Token, "peer-a", "SHA256:AA11", "nonce-a")

	request, err := manager.Submit("192.168.1.44:4222", submission)
	if err != nil {
		t.Fatal(err)
	}
	if request.Status != StatusPending {
		t.Fatalf("status = %q, want pending", request.Status)
	}
	if request.SourceAddress != "192.168.1.44" {
		t.Fatalf("source = %q, want canonical IP", request.SourceAddress)
	}
	if request.PublicKeyFingerprint != "sha256:aa11" {
		t.Fatalf("fingerprint = %q, want normalized value", request.PublicKeyFingerprint)
	}

	pending, err := manager.ListPending()
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 {
		t.Fatalf("pending count = %d, want 1", len(pending))
	}
	pending[0].PeerID = "mutated"
	again, err := manager.ListPending()
	if err != nil {
		t.Fatal(err)
	}
	if again[0].PeerID != "peer-a" {
		t.Fatal("ListPending returned mutable internal state")
	}

	data, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(data, []byte(invitation.Token)) {
		t.Fatal("persisted enrollment state contains the raw invitation token")
	}
	if !bytes.Contains(data, []byte("\"token_hash\"")) {
		t.Fatal("persisted state should retain only the invitation token hash")
	}
	requestJSON, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(requestJSON, []byte(invitation.Token)) || bytes.Contains(requestJSON, []byte("token_hash")) {
		t.Fatalf("request JSON contains enrollment secret material: %s", requestJSON)
	}
	audit, err := manager.Audit()
	if err != nil {
		t.Fatal(err)
	}
	if len(audit) != 1 || audit[0].Action != "request_submitted" {
		t.Fatalf("unexpected audit records: %#v", audit)
	}
	auditJSON, err := json.Marshal(audit)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(auditJSON, []byte(invitation.Token)) || bytes.Contains(auditJSON, []byte("nonce-a")) {
		t.Fatalf("audit JSON contains token or nonce: %s", auditJSON)
	}
	if got := fmt.Sprint(submission); strings.Contains(got, invitation.Token) || !strings.Contains(got, "[REDACTED]") {
		t.Fatalf("submission string is not redacted: %s", got)
	}
	if runtime.GOOS != "windows" {
		info, err := os.Stat(statePath)
		if err != nil {
			t.Fatal(err)
		}
		if got := info.Mode().Perm(); got != 0o600 {
			t.Fatalf("state mode = %o, want 600", got)
		}
	}
}

func TestSubmitRejectsInvalidExpiredAndDisallowedRequests(t *testing.T) {
	now := time.Date(2026, 8, 5, 18, 0, 0, 0, time.UTC)
	manager := newTestManager(t, filepath.Join(t.TempDir(), "state.json"), &now, func(Request) error { return nil }, nil)
	invitation := mustInvitation(t, manager)

	if _, err := manager.Submit("not-an-ip", testSubmission(invitation.Token, "peer-a", "fp-a", "nonce-a")); !errors.Is(err, ErrInvalidSource) {
		t.Fatalf("invalid source error = %v", err)
	}
	if _, err := manager.Submit("10.0.0.8:9999", testSubmission(invitation.Token, "peer-a", "fp-a", "nonce-a")); !errors.Is(err, ErrSourceNotAllowed) {
		t.Fatalf("disallowed source error = %v", err)
	}
	invalid := testSubmission("not-a-token", "peer-a", "fp-a", "nonce-a")
	if _, err := manager.Submit("192.168.1.2", invalid); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("invalid token error = %v", err)
	}

	tooLong := testSubmission(invitation.Token, "peer-a", "fp-a", "nonce-a")
	tooLong.DisplayName = strings.Repeat("x", maxDisplayNameBytes+1)
	if _, err := manager.Submit("192.168.1.2", tooLong); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("oversized display name error = %v", err)
	}

	now = now.Add(DefaultTokenTTL)
	if _, err := manager.Submit("192.168.1.2", testSubmission(invitation.Token, "peer-a", "fp-a", "nonce-a")); !errors.Is(err, ErrExpired) {
		t.Fatalf("expired token error = %v", err)
	}
}

func TestTokenAndNonceReplayPreventionSurvivesRestart(t *testing.T) {
	now := time.Date(2026, 8, 5, 18, 0, 0, 0, time.UTC)
	statePath := filepath.Join(t.TempDir(), "state.json")
	callback := func(Request) error { return nil }
	manager := newTestManager(t, statePath, &now, callback, nil)
	invitation := mustInvitation(t, manager)
	submission := testSubmission(invitation.Token, "peer-a", "fp-a", "nonce-a")
	if _, err := manager.Submit("192.168.1.2", submission); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Submit("192.168.1.2", testSubmission(invitation.Token, "peer-b", "fp-b", "nonce-b")); !errors.Is(err, ErrReplay) {
		t.Fatalf("same-manager replay error = %v", err)
	}

	restarted := newTestManager(t, statePath, &now, callback, nil)
	if _, err := restarted.Submit("192.168.1.2", testSubmission(invitation.Token, "peer-b", "fp-b", "nonce-b")); !errors.Is(err, ErrReplay) {
		t.Fatalf("restart replay error = %v", err)
	}
	secondInvitation := mustInvitation(t, restarted)
	if _, err := restarted.Submit("192.168.1.2", testSubmission(secondInvitation.Token, "peer-b", "fp-b", "nonce-a")); !errors.Is(err, ErrNonceReplay) {
		t.Fatalf("nonce replay error = %v", err)
	}
}

func TestSubmitRejectsDuplicatePeerAndFingerprint(t *testing.T) {
	now := time.Date(2026, 8, 5, 18, 0, 0, 0, time.UTC)
	manager := newTestManager(t, filepath.Join(t.TempDir(), "state.json"), &now, func(Request) error { return nil }, nil)

	first := mustInvitation(t, manager)
	if _, err := manager.Submit("192.168.1.2", testSubmission(first.Token, "peer-a", "sha256:aa", "nonce-a")); err != nil {
		t.Fatal(err)
	}
	duplicatePeer := mustInvitation(t, manager)
	if _, err := manager.Submit("192.168.1.2", testSubmission(duplicatePeer.Token, "peer-a", "sha256:bb", "nonce-b")); !errors.Is(err, ErrDuplicatePeer) {
		t.Fatalf("duplicate peer error = %v", err)
	}
	duplicateFingerprint := mustInvitation(t, manager)
	if _, err := manager.Submit("192.168.1.2", testSubmission(duplicateFingerprint.Token, "peer-b", "SHA256:AA", "nonce-c")); !errors.Is(err, ErrDuplicateFingerprint) {
		t.Fatalf("duplicate fingerprint error = %v", err)
	}
}

func TestRateLimiterIsPerSourceWindowedAndBounded(t *testing.T) {
	now := time.Date(2026, 8, 5, 18, 0, 0, 0, time.UTC)
	options := func(opts *Options) {
		opts.AllowedCIDRs = nil
		opts.RateLimit = 2
		opts.RateWindow = time.Minute
		opts.MaxRateSources = 2
	}
	manager := newTestManager(t, filepath.Join(t.TempDir(), "state.json"), &now, func(Request) error { return nil }, options)
	bad := testSubmission("invalid", "peer-a", "fp-a", "nonce-a")

	for attempt := 0; attempt < 2; attempt++ {
		if _, err := manager.Submit("192.0.2.1:1", bad); !errors.Is(err, ErrInvalidToken) {
			t.Fatalf("attempt %d error = %v, want invalid token", attempt, err)
		}
	}
	if _, err := manager.Submit("192.0.2.1:1", bad); !errors.Is(err, ErrRateLimited) {
		t.Fatalf("third attempt error = %v, want rate limit", err)
	}
	if _, err := manager.Submit("192.0.2.2", bad); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("second source error = %v", err)
	}
	if _, err := manager.Submit("192.0.2.3", bad); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("third source error = %v", err)
	}
	if got := len(manager.rates); got > 2 {
		t.Fatalf("rate source map size = %d, want <= 2", got)
	}

	now = now.Add(time.Minute + time.Nanosecond)
	if _, err := manager.Submit("192.0.2.1", bad); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("post-window error = %v, want invalid token", err)
	}
}

func TestApprovalAndDenialPersistAuditableDecisions(t *testing.T) {
	now := time.Date(2026, 8, 5, 18, 0, 0, 0, time.UTC)
	var approved []Request
	manager := newTestManager(t, filepath.Join(t.TempDir(), "state.json"), &now, func(request Request) error {
		approved = append(approved, request)
		return nil
	}, nil)

	firstInvitation := mustInvitation(t, manager)
	first, err := manager.Submit("192.168.1.2", testSubmission(firstInvitation.Token, "peer-a", "fp-a", "nonce-a"))
	if err != nil {
		t.Fatal(err)
	}
	got, err := manager.Approve(first.ID, "admin@example")
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != StatusApproved || got.DecidedBy != "admin@example" || got.DecidedAt == nil {
		t.Fatalf("unexpected approved request: %#v", got)
	}
	if len(approved) != 1 || approved[0].Status != StatusApproved {
		t.Fatalf("approval callback received %#v", approved)
	}
	if _, err := manager.Approve(first.ID, "admin@example"); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("repeat approval error = %v", err)
	}

	secondInvitation := mustInvitation(t, manager)
	second, err := manager.Submit("192.168.1.3", testSubmission(secondInvitation.Token, "peer-b", "fp-b", "nonce-b"))
	if err != nil {
		t.Fatal(err)
	}
	denied, err := manager.Deny(second.ID, "security-admin")
	if err != nil {
		t.Fatal(err)
	}
	if denied.Status != StatusDenied || denied.DecidedBy != "security-admin" {
		t.Fatalf("unexpected denied request: %#v", denied)
	}
	if len(approved) != 1 {
		t.Fatal("denial invoked approval callback")
	}
	pending, err := manager.ListPending()
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 0 {
		t.Fatalf("pending requests = %#v, want none", pending)
	}
	audit, err := manager.Audit()
	if err != nil {
		t.Fatal(err)
	}
	if len(audit) != 4 {
		t.Fatalf("audit count = %d, want 4", len(audit))
	}
	if audit[len(audit)-1].Action != "request_denied" || audit[len(audit)-1].Actor != "security-admin" {
		t.Fatalf("unexpected denial audit: %#v", audit[len(audit)-1])
	}
}

func TestAutomaticApprovalUsesPersistenceCallback(t *testing.T) {
	now := time.Date(2026, 8, 5, 18, 0, 0, 0, time.UTC)
	var callbackRequest Request
	manager := newTestManager(t, filepath.Join(t.TempDir(), "state.json"), &now, func(request Request) error {
		callbackRequest = request
		return nil
	}, func(opts *Options) {
		opts.RequireApproval = false
	})
	invitation := mustInvitation(t, manager)
	request, err := manager.Submit("192.168.1.2", testSubmission(invitation.Token, "peer-a", "fp-a", "nonce-a"))
	if err != nil {
		t.Fatal(err)
	}
	if request.Status != StatusApproved || request.DecidedBy != "automatic-policy" {
		t.Fatalf("automatic approval = %#v", request)
	}
	if callbackRequest.ID != request.ID || callbackRequest.Status != StatusApproved {
		t.Fatalf("callback request = %#v", callbackRequest)
	}
}

func TestApprovalCallbackFailureIsPersistedAndRetryableWithoutSecretLeak(t *testing.T) {
	now := time.Date(2026, 8, 5, 18, 0, 0, 0, time.UTC)
	statePath := filepath.Join(t.TempDir(), "state.json")
	fail := true
	callback := func(Request) error {
		if fail {
			return errors.New("callback secret should never be persisted")
		}
		return nil
	}
	manager := newTestManager(t, statePath, &now, callback, nil)
	invitation := mustInvitation(t, manager)
	request, err := manager.Submit("192.168.1.2", testSubmission(invitation.Token, "peer-a", "fp-a", "nonce-a"))
	if err != nil {
		t.Fatal(err)
	}

	failed, err := manager.Approve(request.ID, "admin")
	if !errors.Is(err, ErrApprovalFailed) {
		t.Fatalf("approval error = %v, want ErrApprovalFailed", err)
	}
	if failed.Status != StatusApprovalFailed || failed.LastError != "approval persistence failed" {
		t.Fatalf("failed request = %#v", failed)
	}
	data, readErr := os.ReadFile(statePath)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if bytes.Contains(data, []byte("callback secret")) || bytes.Contains(data, []byte(invitation.Token)) {
		t.Fatalf("failed state contains secret material: %s", data)
	}

	restarted := newTestManager(t, statePath, &now, callback, nil)
	pending, err := restarted.ListPending()
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || pending[0].Status != StatusApprovalFailed {
		t.Fatalf("restarted pending state = %#v", pending)
	}
	fail = false
	approved, err := restarted.Approve(request.ID, "admin")
	if err != nil {
		t.Fatal(err)
	}
	if approved.Status != StatusApproved {
		t.Fatalf("retried approval status = %q", approved.Status)
	}
}

func TestApprovalFinalPersistFailureLeavesDurableAuditedIntentAndRecovers(t *testing.T) {
	now := time.Date(2026, 8, 5, 18, 0, 0, 0, time.UTC)
	statePath := filepath.Join(t.TempDir(), "state.json")
	authorized := false
	callbackCalls := 0
	callback := func(Request) error {
		callbackCalls++
		authorized = true
		return nil
	}
	manager := newTestManager(t, statePath, &now, callback, nil)
	invitation := mustInvitation(t, manager)
	request, err := manager.Submit("192.168.1.2", testSubmission(invitation.Token, "peer-a", "fp-a", "nonce-a"))
	if err != nil {
		t.Fatal(err)
	}

	injected := errors.New("injected final state write failure")
	persistCalls := 0
	manager.writeState = func(path string, value any) error {
		persistCalls++
		if persistCalls == 2 {
			return injected
		}
		return atomicWriteJSON(path, value)
	}
	approving, err := manager.Approve(request.ID, "admin")
	if !errors.Is(err, injected) {
		t.Fatalf("approval error = %v, want injected persistence failure", err)
	}
	if !authorized || callbackCalls != 1 {
		t.Fatalf("authorization callback state = %v, calls = %d", authorized, callbackCalls)
	}
	if approving.Status != StatusApproving {
		t.Fatalf("returned status = %q, want approving", approving.Status)
	}

	data, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatal(err)
	}
	var persisted persistedState
	if err := json.Unmarshal(data, &persisted); err != nil {
		t.Fatal(err)
	}
	if len(persisted.Requests) != 1 || persisted.Requests[0].Status != StatusApproving {
		t.Fatalf("persisted request = %#v, want durable approving intent", persisted.Requests)
	}
	lastAudit := persisted.Audit[len(persisted.Audit)-1]
	if lastAudit.Action != "approval_started" || lastAudit.Result != "in_progress" || lastAudit.Actor != "admin" {
		t.Fatalf("persisted approval intent audit = %#v", lastAudit)
	}
	if bytes.Contains(data, []byte("request_approved")) {
		t.Fatalf("state falsely finalized approval: %s", data)
	}

	restarted := newTestManager(t, statePath, &now, callback, nil)
	recovered, err := restarted.Get(request.ID)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.Status != StatusApproved || recovered.DecidedBy != "admin" {
		t.Fatalf("recovered request = %#v", recovered)
	}
	if callbackCalls != 2 {
		t.Fatalf("authorization callback calls = %d, want idempotent recovery call", callbackCalls)
	}
	audit, err := restarted.Audit()
	if err != nil {
		t.Fatal(err)
	}
	lastAudit = audit[len(audit)-1]
	if lastAudit.Action != "request_approved" || lastAudit.Result != "approved" {
		t.Fatalf("recovered approval audit = %#v", lastAudit)
	}
}

func TestPendingRequestExpiresAndCannotBeApproved(t *testing.T) {
	now := time.Date(2026, 8, 5, 18, 0, 0, 0, time.UTC)
	options := func(opts *Options) {
		opts.RequestTTL = 30 * time.Second
	}
	manager := newTestManager(t, filepath.Join(t.TempDir(), "state.json"), &now, func(Request) error { return nil }, options)
	invitation := mustInvitation(t, manager)
	request, err := manager.Submit("192.168.1.2", testSubmission(invitation.Token, "peer-a", "fp-a", "nonce-a"))
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(30 * time.Second)
	pending, err := manager.ListPending()
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 0 {
		t.Fatalf("expired request remained pending: %#v", pending)
	}
	expired, err := manager.Get(request.ID)
	if err != nil {
		t.Fatal(err)
	}
	if expired.Status != StatusExpired {
		t.Fatalf("status = %q, want expired", expired.Status)
	}
	if _, err := manager.Approve(request.ID, "admin"); !errors.Is(err, ErrExpired) {
		t.Fatalf("expired approval error = %v", err)
	}
}

func TestAllowedCIDRsSupportIPv6TransportAddresses(t *testing.T) {
	now := time.Date(2026, 8, 5, 18, 0, 0, 0, time.UTC)
	manager := newTestManager(t, filepath.Join(t.TempDir(), "state.json"), &now, func(Request) error { return nil }, func(opts *Options) {
		opts.AllowedCIDRs = []string{"fd00::/8"}
	})
	invitation := mustInvitation(t, manager)
	request, err := manager.Submit("[fd00::42]:37391", testSubmission(invitation.Token, "peer-v6", "fp-v6", "nonce-v6"))
	if err != nil {
		t.Fatal(err)
	}
	if request.SourceAddress != "fd00::42" {
		t.Fatalf("source = %q, want fd00::42", request.SourceAddress)
	}
}

func newTestManager(
	t *testing.T,
	statePath string,
	now *time.Time,
	callback func(Request) error,
	mutate func(*Options),
) *Manager {
	t.Helper()
	options := Options{
		Enabled:         true,
		StatePath:       statePath,
		RequestTTL:      DefaultRequestTTL,
		TokenTTL:        DefaultTokenTTL,
		RateLimit:       100,
		RateWindow:      time.Minute,
		MaxRateSources:  32,
		AllowedCIDRs:    []string{"192.168.0.0/16"},
		RequireApproval: true,
		PersistApproval: callback,
		Now: func() time.Time {
			return *now
		},
	}
	if mutate != nil {
		mutate(&options)
	}
	manager, err := New(options)
	if err != nil {
		t.Fatal(err)
	}
	return manager
}

func mustInvitation(t *testing.T, manager *Manager) Invitation {
	t.Helper()
	invitation, err := manager.CreateInvitation()
	if err != nil {
		t.Fatal(err)
	}
	if invitation.Token == "" || invitation.ID == "" {
		t.Fatalf("invalid invitation: %#v", invitation)
	}
	return invitation
}

func testSubmission(token, peerID, fingerprint, nonce string) Submission {
	return Submission{
		Token:                token,
		PeerID:               peerID,
		DisplayName:          peerID + " display",
		PublicKeyFingerprint: fingerprint,
		Nonce:                nonce,
	}
}
