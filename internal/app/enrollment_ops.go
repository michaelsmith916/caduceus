package app

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/caduceus/caduceus/internal/config"
	"github.com/caduceus/caduceus/internal/control"
	"github.com/caduceus/caduceus/internal/enrollment"
	"github.com/caduceus/caduceus/internal/p2p"
	"github.com/caduceus/caduceus/internal/security"
)

func (a *App) CreateEnrollmentInvitation(ctx context.Context) (any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	invitation, err := a.Enrollment.CreateInvitation()
	if err != nil {
		return nil, err
	}
	addresses := make([]string, 0, len(a.Node.ListenAddrs()))
	for _, address := range a.Node.ListenAddrs() {
		addresses = append(addresses, strings.TrimRight(address, "/")+"/p2p/"+a.Node.HostID())
	}
	return map[string]any{
		"invitation":          invitation,
		"coordinator_peer_id": a.Node.HostID(),
		"coordinator_addrs":   addresses,
	}, nil
}

func (a *App) ListEnrollmentRequests(ctx context.Context) (any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	requests, err := a.Enrollment.ListPending()
	if err != nil {
		return nil, err
	}
	return map[string]any{"requests": requests}, nil
}

func (a *App) GetEnrollmentRequest(ctx context.Context, requestID string) (any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return a.Enrollment.Get(requestID)
}

func (a *App) ApproveEnrollment(ctx context.Context, requestID, actor string) (any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(actor) == "" {
		actor = "local-control"
	}
	request, err := a.Enrollment.Approve(requestID, actor)
	if err != nil {
		return nil, err
	}
	a.Node.ActivateEnrollment(request)
	return request, nil
}

func (a *App) DenyEnrollment(ctx context.Context, requestID, actor string) (any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(actor) == "" {
		actor = "local-control"
	}
	return a.Enrollment.Deny(requestID, actor)
}

func (a *App) EnrollmentAudit(ctx context.Context) (any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	records, err := a.Enrollment.Audit()
	if err != nil {
		return nil, err
	}
	return map[string]any{"audit": records}, nil
}

func (a *App) SubmitEnrollment(ctx context.Context, coordinatorAddress, token, displayName string) (p2p.EnrollmentWireDecision, error) {
	decision, err := a.Node.RequestEnrollment(ctx, coordinatorAddress, token, displayName)
	if err != nil {
		return p2p.EnrollmentWireDecision{}, err
	}
	if decision.Status == string(enrollment.StatusApproved) {
		err = a.applyEnrollmentDecision(ctx, coordinatorAddress, decision)
	}
	return decision, err
}

func (a *App) CheckEnrollment(ctx context.Context, coordinatorAddress, requestID string) (p2p.EnrollmentWireDecision, error) {
	decision, err := a.Node.EnrollmentStatus(ctx, coordinatorAddress, requestID)
	if err != nil {
		return p2p.EnrollmentWireDecision{}, err
	}
	if decision.Status == string(enrollment.StatusApproved) {
		if err := a.applyEnrollmentDecision(ctx, coordinatorAddress, decision); err != nil {
			return p2p.EnrollmentWireDecision{}, err
		}
	}
	return decision, nil
}

func (a *App) SubmitEnrollmentRequest(ctx context.Context, request control.EnrollmentSubmitRequest) (any, error) {
	return a.SubmitEnrollment(ctx, request.CoordinatorAddress, request.Token, request.DisplayName)
}

func (a *App) CheckEnrollmentRequest(ctx context.Context, request control.EnrollmentStatusRequest) (any, error) {
	return a.CheckEnrollment(ctx, request.CoordinatorAddress, request.RequestID)
}

// ApplyEnrollmentDecision atomically updates each individual config file and
// rolls both back to their exact prior values if any later activation step
// fails. The decision contains no private key or invitation token.
func (a *App) ApplyEnrollmentDecision(coordinatorAddress string, decision p2p.EnrollmentWireDecision) error {
	return a.applyEnrollmentDecision(context.Background(), coordinatorAddress, decision)
}

func (a *App) applyEnrollmentDecision(ctx context.Context, coordinatorAddress string, decision p2p.EnrollmentWireDecision) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if decision.Status != string(enrollment.StatusApproved) {
		return errors.New("enrollment decision is not approved")
	}
	if !validGroupHash(decision.GroupHash) || strings.TrimSpace(decision.CoordinatorPeerID) == "" || strings.TrimSpace(decision.CoordinatorFingerprint) == "" {
		return errors.New("approved enrollment decision is missing valid coordinator credentials")
	}

	a.mu.Lock()
	oldConfig := a.Cfg
	oldPeers, err := security.LoadAllowedPeers(oldConfig.Security.AllowedPeersPath)
	if err != nil {
		a.mu.Unlock()
		return err
	}
	newConfig := oldConfig
	newConfig.Security.P2PKeyHash = decision.GroupHash
	newConfig.Enrollment.TrustedLAN.CoordinatorAddress = coordinatorAddress
	coordinator := security.AllowedPeer{
		PeerID:               decision.CoordinatorPeerID,
		Name:                 "Caduceus coordinator",
		PublicKeyFingerprint: decision.CoordinatorFingerprint,
		TrustLevel:           "trusted-lan",
		Allowed:              true,
		Notes:                "coordinator approved by trusted-LAN enrollment",
	}
	if err := a.Node.ValidateEnrollmentCoordinator(decision.GroupHash, coordinator, coordinatorAddress, decision.CoordinatorAddrs); err != nil {
		a.mu.Unlock()
		return err
	}
	if err := commitEnrollmentMembership(a.ConfigPath, oldConfig, newConfig, oldPeers, coordinator, func() error {
		return a.Node.ApplyEnrollmentMembership(decision.GroupHash, coordinator)
	}); err != nil {
		a.mu.Unlock()
		return err
	}
	a.Cfg = newConfig
	a.GroupHash = decision.GroupHash
	a.mu.Unlock()

	// Network failure does not roll back the durable approval. A subsequent
	// status poll can retry this idempotent announcement, while a restart still
	// loads the approved group and allowlist.
	if err := a.Node.AnnounceEnrollmentMembership(ctx, coordinator, coordinatorAddress, decision.CoordinatorAddrs); err != nil {
		return fmt.Errorf("enrollment membership saved but immediate coordinator join failed: %w", err)
	}
	return nil
}

func commitEnrollmentMembership(configPath string, oldConfig, newConfig config.Config, oldPeers security.AllowedPeers, coordinator security.AllowedPeer, activate func() error) error {
	rollback := func(cause error) error {
		configErr := config.Save(configPath, oldConfig)
		peersErr := security.SaveAllowedPeers(oldConfig.Security.AllowedPeersPath, oldPeers)
		return errors.Join(cause, wrapRollback("configuration", configErr), wrapRollback("allowlist", peersErr))
	}
	if err := config.Save(configPath, newConfig); err != nil {
		return err
	}
	if err := security.AddPeer(newConfig.Security.AllowedPeersPath, coordinator); err != nil {
		return rollback(err)
	}
	if activate != nil {
		if err := activate(); err != nil {
			return rollback(err)
		}
	}
	return nil
}

func wrapRollback(name string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("rollback %s: %w", name, err)
}
