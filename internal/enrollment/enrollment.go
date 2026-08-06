package enrollment

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

const (
	DefaultRequestTTL     = 10 * time.Minute
	DefaultTokenTTL       = 5 * time.Minute
	DefaultRateLimit      = 5
	DefaultRateWindow     = time.Minute
	DefaultMaxRateSources = 1024

	maxStateBytes       = 16 << 20
	maxPeerIDBytes      = 256
	maxDisplayNameBytes = 128
	maxFingerprintBytes = 256
	maxNonceBytes       = 256
	maxTokenBytes       = 512
	maxActorBytes       = 128
	maxSourceBytes      = 255
)

var (
	ErrDisabled             = errors.New("trusted-LAN enrollment is disabled")
	ErrInvalidOptions       = errors.New("invalid enrollment options")
	ErrInvalidRequest       = errors.New("invalid enrollment request")
	ErrInvalidSource        = errors.New("invalid enrollment source")
	ErrSourceNotAllowed     = errors.New("enrollment source is not allowed")
	ErrRateLimited          = errors.New("enrollment rate limit exceeded")
	ErrInvalidToken         = errors.New("invalid enrollment token")
	ErrExpired              = errors.New("enrollment request or token expired")
	ErrReplay               = errors.New("enrollment token has already been used")
	ErrNonceReplay          = errors.New("enrollment nonce has already been used")
	ErrDuplicatePeer        = errors.New("duplicate enrollment peer ID")
	ErrDuplicateFingerprint = errors.New("duplicate enrollment public-key fingerprint")
	ErrNotFound             = errors.New("enrollment request not found")
	ErrInvalidState         = errors.New("invalid enrollment request state")
	ErrApprovalFailed       = errors.New("enrollment approval persistence failed")
)

type Status string

const (
	StatusPending        Status = "pending"
	StatusApproving      Status = "approving"
	StatusApproved       Status = "approved"
	StatusDenied         Status = "denied"
	StatusExpired        Status = "expired"
	StatusApprovalFailed Status = "approval_failed"
)

type Options struct {
	Enabled         bool
	StatePath       string
	RequestTTL      time.Duration
	TokenTTL        time.Duration
	RateLimit       int
	RateWindow      time.Duration
	MaxRateSources  int
	AllowedCIDRs    []string
	RequireApproval bool
	// PersistApproval durably authorizes an approved request. It must be
	// idempotent because a write-ahead approval intent is replayed after a
	// crash or a failure finalizing the enrollment state.
	PersistApproval func(Request) error
	Now             func() time.Time
	Random          io.Reader
}

type Invitation struct {
	ID        string    `json:"id"`
	Token     string    `json:"token"`
	ExpiresAt time.Time `json:"expires_at"`
}

type Submission struct {
	Token                string `json:"token"`
	PeerID               string `json:"peer_id"`
	DisplayName          string `json:"display_name"`
	PublicKeyFingerprint string `json:"public_key_fingerprint"`
	Nonce                string `json:"nonce"`
}

func (s Submission) String() string {
	return fmt.Sprintf(
		"enrollment.Submission{PeerID:%q DisplayName:%q PublicKeyFingerprint:%q Nonce:%q Token:[REDACTED]}",
		s.PeerID,
		s.DisplayName,
		s.PublicKeyFingerprint,
		s.Nonce,
	)
}

type Request struct {
	ID                   string     `json:"id"`
	PeerID               string     `json:"peer_id"`
	DisplayName          string     `json:"display_name"`
	PublicKeyFingerprint string     `json:"public_key_fingerprint"`
	SourceAddress        string     `json:"source_address"`
	Nonce                string     `json:"nonce"`
	Status               Status     `json:"status"`
	RequestedAt          time.Time  `json:"requested_at"`
	UpdatedAt            time.Time  `json:"updated_at"`
	ExpiresAt            time.Time  `json:"expires_at"`
	DecidedAt            *time.Time `json:"decided_at,omitempty"`
	DecidedBy            string     `json:"decided_by,omitempty"`
	LastError            string     `json:"last_error,omitempty"`
}

type AuditRecord struct {
	ID                   string    `json:"id"`
	RequestID            string    `json:"request_id"`
	Action               string    `json:"action"`
	Result               string    `json:"result"`
	PeerID               string    `json:"peer_id"`
	DisplayName          string    `json:"display_name"`
	PublicKeyFingerprint string    `json:"public_key_fingerprint"`
	SourceAddress        string    `json:"source_address"`
	Actor                string    `json:"actor,omitempty"`
	Timestamp            time.Time `json:"timestamp"`
	ExpiresAt            time.Time `json:"expires_at"`
	Detail               string    `json:"detail,omitempty"`
}

type storedInvitation struct {
	ID              string     `json:"id"`
	TokenHash       string     `json:"token_hash"`
	CreatedAt       time.Time  `json:"created_at"`
	ExpiresAt       time.Time  `json:"expires_at"`
	UsedAt          *time.Time `json:"used_at,omitempty"`
	UsedByRequestID string     `json:"used_by_request_id,omitempty"`
}

type persistedState struct {
	Version     int                `json:"version"`
	Invitations []storedInvitation `json:"invitations"`
	Requests    []Request          `json:"requests"`
	Audit       []AuditRecord      `json:"audit"`
}

type rateEntry struct {
	Hits     []time.Time
	LastSeen time.Time
}

type Manager struct {
	mu         sync.Mutex
	opts       Options
	networks   []*net.IPNet
	state      persistedState
	writeState func(string, any) error
	rates      map[string]*rateEntry
}

func New(opts Options) (*Manager, error) {
	applyDefaults(&opts)
	m := &Manager{
		opts:       opts,
		state:      emptyState(),
		writeState: atomicWriteJSON,
		rates:      make(map[string]*rateEntry),
	}
	if !opts.Enabled {
		return m, nil
	}
	if err := validateOptions(opts); err != nil {
		return nil, err
	}
	for _, raw := range opts.AllowedCIDRs {
		raw = strings.TrimSpace(raw)
		_, network, err := net.ParseCIDR(raw)
		if err != nil {
			return nil, fmt.Errorf("%w: allowed CIDR %q: %v", ErrInvalidOptions, raw, err)
		}
		m.networks = append(m.networks, network)
	}
	if err := m.load(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	err := m.recoverApprovalsLocked(m.now())
	if err == nil {
		err = m.expireLocked(m.now())
	}
	m.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return m, nil
}

func NewManager(opts Options) (*Manager, error) {
	return New(opts)
}

func applyDefaults(opts *Options) {
	if opts.RequestTTL == 0 {
		opts.RequestTTL = DefaultRequestTTL
	}
	if opts.TokenTTL == 0 {
		opts.TokenTTL = DefaultTokenTTL
	}
	if opts.RateLimit == 0 {
		opts.RateLimit = DefaultRateLimit
	}
	if opts.RateWindow == 0 {
		opts.RateWindow = DefaultRateWindow
	}
	if opts.MaxRateSources == 0 {
		opts.MaxRateSources = DefaultMaxRateSources
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Random == nil {
		opts.Random = rand.Reader
	}
}

func validateOptions(opts Options) error {
	switch {
	case strings.TrimSpace(opts.StatePath) == "":
		return fmt.Errorf("%w: state path is required when enrollment is enabled", ErrInvalidOptions)
	case opts.RequestTTL <= 0:
		return fmt.Errorf("%w: request TTL must be positive", ErrInvalidOptions)
	case opts.TokenTTL <= 0:
		return fmt.Errorf("%w: token TTL must be positive", ErrInvalidOptions)
	case opts.RateLimit <= 0:
		return fmt.Errorf("%w: rate limit must be positive", ErrInvalidOptions)
	case opts.RateWindow <= 0:
		return fmt.Errorf("%w: rate window must be positive", ErrInvalidOptions)
	case opts.MaxRateSources <= 0:
		return fmt.Errorf("%w: maximum rate sources must be positive", ErrInvalidOptions)
	case opts.PersistApproval == nil:
		return fmt.Errorf("%w: approval persistence callback is required", ErrInvalidOptions)
	}
	return nil
}

func emptyState() persistedState {
	return persistedState{
		Version:     1,
		Invitations: []storedInvitation{},
		Requests:    []Request{},
		Audit:       []AuditRecord{},
	}
}

func (m *Manager) Enabled() bool {
	return m != nil && m.opts.Enabled
}

func (m *Manager) CreateInvitation() (Invitation, error) {
	if m == nil {
		return Invitation{}, ErrDisabled
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.opts.Enabled {
		return Invitation{}, ErrDisabled
	}

	id, err := m.uniqueIDLocked("inv", func(candidate string) bool {
		for _, invitation := range m.state.Invitations {
			if invitation.ID == candidate {
				return true
			}
		}
		return false
	})
	if err != nil {
		return Invitation{}, err
	}
	token, tokenHash, err := m.uniqueTokenLocked()
	if err != nil {
		return Invitation{}, err
	}
	now := m.now()
	stored := storedInvitation{
		ID:        id,
		TokenHash: tokenHash,
		CreatedAt: now,
		ExpiresAt: now.Add(m.opts.TokenTTL),
	}
	m.state.Invitations = append(m.state.Invitations, stored)
	if err := m.persistLocked(); err != nil {
		m.state.Invitations = m.state.Invitations[:len(m.state.Invitations)-1]
		return Invitation{}, err
	}
	return Invitation{ID: id, Token: token, ExpiresAt: stored.ExpiresAt}, nil
}

func (m *Manager) Submit(sourceAddress string, submission Submission) (Request, error) {
	if m == nil {
		return Request{}, ErrDisabled
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.opts.Enabled {
		return Request{}, ErrDisabled
	}

	now := m.now()
	if err := m.expireLocked(now); err != nil {
		return Request{}, err
	}
	source, ip, err := parseSourceAddress(sourceAddress)
	if err != nil {
		return Request{}, err
	}
	if !m.allowRateLocked(source, now) {
		return Request{}, ErrRateLimited
	}
	if !m.sourceAllowed(ip) {
		return Request{}, ErrSourceNotAllowed
	}
	submission, err = normalizeSubmission(submission)
	if err != nil {
		return Request{}, err
	}

	invitationIndex := m.matchInvitationLocked(submission.Token)
	if invitationIndex < 0 {
		return Request{}, ErrInvalidToken
	}
	invitation := &m.state.Invitations[invitationIndex]
	if invitation.UsedAt != nil {
		return Request{}, ErrReplay
	}
	if !now.Before(invitation.ExpiresAt) {
		return Request{}, ErrExpired
	}
	for _, existing := range m.state.Requests {
		if existing.Nonce == submission.Nonce {
			return Request{}, ErrNonceReplay
		}
		if blocksDuplicate(existing.Status) && existing.PeerID == submission.PeerID {
			return Request{}, ErrDuplicatePeer
		}
		if blocksDuplicate(existing.Status) &&
			strings.EqualFold(existing.PublicKeyFingerprint, submission.PublicKeyFingerprint) {
			return Request{}, ErrDuplicateFingerprint
		}
	}

	requestID, err := m.uniqueIDLocked("req", func(candidate string) bool {
		for _, request := range m.state.Requests {
			if request.ID == candidate {
				return true
			}
		}
		return false
	})
	if err != nil {
		return Request{}, err
	}
	auditID, err := m.uniqueAuditIDLocked()
	if err != nil {
		return Request{}, err
	}
	request := Request{
		ID:                   requestID,
		PeerID:               submission.PeerID,
		DisplayName:          submission.DisplayName,
		PublicKeyFingerprint: submission.PublicKeyFingerprint,
		SourceAddress:        source,
		Nonce:                submission.Nonce,
		Status:               StatusPending,
		RequestedAt:          now,
		UpdatedAt:            now,
		ExpiresAt:            now.Add(m.opts.RequestTTL),
	}
	oldInvitation := cloneInvitation(*invitation)
	invitation.UsedAt = timePointer(now)
	invitation.UsedByRequestID = request.ID
	m.state.Requests = append(m.state.Requests, request)
	m.state.Audit = append(m.state.Audit, AuditRecord{
		ID:                   auditID,
		RequestID:            request.ID,
		Action:               "request_submitted",
		Result:               "pending",
		PeerID:               request.PeerID,
		DisplayName:          request.DisplayName,
		PublicKeyFingerprint: request.PublicKeyFingerprint,
		SourceAddress:        request.SourceAddress,
		Timestamp:            now,
		ExpiresAt:            request.ExpiresAt,
	})
	if err := m.persistLocked(); err != nil {
		*invitation = oldInvitation
		m.state.Requests = m.state.Requests[:len(m.state.Requests)-1]
		m.state.Audit = m.state.Audit[:len(m.state.Audit)-1]
		return Request{}, err
	}
	if !m.opts.RequireApproval {
		return m.approveLocked(request.ID, "automatic-policy", now)
	}
	return cloneRequest(request), nil
}

func (m *Manager) SubmitRequest(sourceAddress string, submission Submission) (Request, error) {
	return m.Submit(sourceAddress, submission)
}

func (m *Manager) ListPending() ([]Request, error) {
	if m == nil {
		return nil, ErrDisabled
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.opts.Enabled {
		return nil, ErrDisabled
	}
	if err := m.expireLocked(m.now()); err != nil {
		return nil, err
	}
	out := make([]Request, 0)
	for _, request := range m.state.Requests {
		if request.Status == StatusPending || request.Status == StatusApproving || request.Status == StatusApprovalFailed {
			out = append(out, cloneRequest(request))
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].RequestedAt.Equal(out[j].RequestedAt) {
			return out[i].ID < out[j].ID
		}
		return out[i].RequestedAt.Before(out[j].RequestedAt)
	})
	return out, nil
}

func (m *Manager) Get(requestID string) (Request, error) {
	if m == nil {
		return Request{}, ErrDisabled
	}
	requestID, err := validateIdentifier("request ID", requestID, 128)
	if err != nil {
		return Request{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.opts.Enabled {
		return Request{}, ErrDisabled
	}
	if err := m.expireLocked(m.now()); err != nil {
		return Request{}, err
	}
	index := m.requestIndexLocked(requestID)
	if index < 0 {
		return Request{}, ErrNotFound
	}
	return cloneRequest(m.state.Requests[index]), nil
}

func (m *Manager) Approve(requestID, approvedBy string) (Request, error) {
	if m == nil {
		return Request{}, ErrDisabled
	}
	var err error
	requestID, err = validateIdentifier("request ID", requestID, 128)
	if err != nil {
		return Request{}, err
	}
	approvedBy, err = validateIdentifier("approving identity", approvedBy, maxActorBytes)
	if err != nil {
		return Request{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.opts.Enabled {
		return Request{}, ErrDisabled
	}
	now := m.now()
	if err := m.expireLocked(now); err != nil {
		return Request{}, err
	}
	return m.approveLocked(requestID, approvedBy, now)
}

func (m *Manager) approveLocked(requestID, approvedBy string, now time.Time) (Request, error) {
	index := m.requestIndexLocked(requestID)
	if index < 0 {
		return Request{}, ErrNotFound
	}
	current := m.state.Requests[index]
	if current.Status == StatusExpired {
		return cloneRequest(current), ErrExpired
	}
	if current.Status == StatusApproving {
		return m.finishApprovalLocked(index, now)
	}
	if current.Status != StatusPending && current.Status != StatusApprovalFailed {
		return cloneRequest(current), ErrInvalidState
	}

	auditID, err := m.uniqueAuditIDLocked()
	if err != nil {
		return cloneRequest(current), err
	}
	approving := cloneRequest(current)
	approving.Status = StatusApproving
	approving.UpdatedAt = now
	approving.DecidedAt = nil
	approving.DecidedBy = approvedBy
	approving.LastError = ""
	m.state.Requests[index] = approving
	m.state.Audit = append(m.state.Audit, auditForRequest(
		auditID,
		approving,
		"approval_started",
		"in_progress",
		approvedBy,
		now,
		"authorization update pending",
	))
	if err := m.persistLocked(); err != nil {
		m.state.Requests[index] = current
		m.state.Audit = m.state.Audit[:len(m.state.Audit)-1]
		return cloneRequest(current), err
	}
	return m.finishApprovalLocked(index, now)
}

func (m *Manager) finishApprovalLocked(index int, now time.Time) (Request, error) {
	current := cloneRequest(m.state.Requests[index])
	if current.Status != StatusApproving {
		return current, ErrInvalidState
	}
	auditIndex := m.approvalAuditIndexLocked(current.ID)
	if auditIndex < 0 {
		return current, errors.New("invalid enrollment state: approving request has no audit intent")
	}
	startedAudit := m.state.Audit[auditIndex]
	candidate := cloneRequest(current)
	candidate.Status = StatusApproved
	candidate.UpdatedAt = now
	candidate.DecidedAt = timePointer(now)
	candidate.LastError = ""
	if err := m.opts.PersistApproval(cloneRequest(candidate)); err != nil {
		failed := cloneRequest(current)
		failed.Status = StatusApprovalFailed
		failed.UpdatedAt = now
		failed.DecidedAt = nil
		failed.LastError = "approval persistence failed"
		m.state.Requests[index] = failed
		m.state.Audit[auditIndex] = auditForRequest(
			startedAudit.ID,
			failed,
			"approval_failed",
			"failed",
			failed.DecidedBy,
			now,
			"trust configuration was not changed",
		)
		if persistErr := m.persistLocked(); persistErr != nil {
			m.state.Requests[index] = current
			m.state.Audit[auditIndex] = startedAudit
			return cloneRequest(current), errors.Join(ErrApprovalFailed, persistErr)
		}
		return cloneRequest(failed), ErrApprovalFailed
	}
	m.state.Requests[index] = candidate
	m.state.Audit[auditIndex] = auditForRequest(
		startedAudit.ID,
		candidate,
		"request_approved",
		"approved",
		candidate.DecidedBy,
		now,
		"",
	)
	if err := m.persistLocked(); err != nil {
		m.state.Requests[index] = current
		m.state.Audit[auditIndex] = startedAudit
		return cloneRequest(current), fmt.Errorf("finalize enrollment approval: %w", err)
	}
	return cloneRequest(candidate), nil
}

func (m *Manager) recoverApprovalsLocked(now time.Time) error {
	for index := range m.state.Requests {
		if m.state.Requests[index].Status != StatusApproving {
			continue
		}
		if _, err := m.finishApprovalLocked(index, now); err != nil && !errors.Is(err, ErrApprovalFailed) {
			return fmt.Errorf("recover enrollment approval %q: %w", m.state.Requests[index].ID, err)
		}
	}
	return nil
}

func (m *Manager) approvalAuditIndexLocked(requestID string) int {
	for index := len(m.state.Audit) - 1; index >= 0; index-- {
		record := m.state.Audit[index]
		if record.RequestID == requestID && record.Action == "approval_started" {
			return index
		}
	}
	return -1
}

func (m *Manager) Deny(requestID, deniedBy string) (Request, error) {
	if m == nil {
		return Request{}, ErrDisabled
	}
	var err error
	requestID, err = validateIdentifier("request ID", requestID, 128)
	if err != nil {
		return Request{}, err
	}
	deniedBy, err = validateIdentifier("denying identity", deniedBy, maxActorBytes)
	if err != nil {
		return Request{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.opts.Enabled {
		return Request{}, ErrDisabled
	}
	now := m.now()
	if err := m.expireLocked(now); err != nil {
		return Request{}, err
	}
	index := m.requestIndexLocked(requestID)
	if index < 0 {
		return Request{}, ErrNotFound
	}
	current := m.state.Requests[index]
	if current.Status == StatusExpired {
		return cloneRequest(current), ErrExpired
	}
	if current.Status != StatusPending && current.Status != StatusApprovalFailed {
		return cloneRequest(current), ErrInvalidState
	}
	auditID, err := m.uniqueAuditIDLocked()
	if err != nil {
		return cloneRequest(current), err
	}
	denied := cloneRequest(current)
	denied.Status = StatusDenied
	denied.UpdatedAt = now
	denied.DecidedAt = timePointer(now)
	denied.DecidedBy = deniedBy
	denied.LastError = ""
	m.state.Requests[index] = denied
	m.state.Audit = append(m.state.Audit, auditForRequest(
		auditID,
		denied,
		"request_denied",
		"denied",
		deniedBy,
		now,
		"",
	))
	if err := m.persistLocked(); err != nil {
		m.state.Requests[index] = current
		m.state.Audit = m.state.Audit[:len(m.state.Audit)-1]
		return cloneRequest(current), err
	}
	return cloneRequest(denied), nil
}

func (m *Manager) Audit() ([]AuditRecord, error) {
	if m == nil {
		return nil, ErrDisabled
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.opts.Enabled {
		return nil, ErrDisabled
	}
	if err := m.expireLocked(m.now()); err != nil {
		return nil, err
	}
	out := append([]AuditRecord(nil), m.state.Audit...)
	return out, nil
}

func (m *Manager) now() time.Time {
	return m.opts.Now().UTC()
}

func (m *Manager) sourceAllowed(ip net.IP) bool {
	if len(m.networks) == 0 {
		return true
	}
	for _, network := range m.networks {
		if network.Contains(ip) {
			return true
		}
	}
	return false
}

func (m *Manager) allowRateLocked(source string, now time.Time) bool {
	cutoff := now.Add(-m.opts.RateWindow)
	for key, entry := range m.rates {
		entry.Hits = timestampsAfter(entry.Hits, cutoff)
		if len(entry.Hits) == 0 && !entry.LastSeen.After(cutoff) {
			delete(m.rates, key)
		}
	}
	entry := m.rates[source]
	if entry == nil {
		if len(m.rates) >= m.opts.MaxRateSources {
			oldestKey := ""
			var oldest time.Time
			for key, candidate := range m.rates {
				if oldestKey == "" || candidate.LastSeen.Before(oldest) ||
					(candidate.LastSeen.Equal(oldest) && key < oldestKey) {
					oldestKey = key
					oldest = candidate.LastSeen
				}
			}
			delete(m.rates, oldestKey)
		}
		entry = &rateEntry{}
		m.rates[source] = entry
	}
	entry.Hits = timestampsAfter(entry.Hits, cutoff)
	entry.LastSeen = now
	if len(entry.Hits) >= m.opts.RateLimit {
		return false
	}
	entry.Hits = append(entry.Hits, now)
	return true
}

func timestampsAfter(values []time.Time, cutoff time.Time) []time.Time {
	out := values[:0]
	for _, value := range values {
		if value.After(cutoff) {
			out = append(out, value)
		}
	}
	return out
}

func (m *Manager) matchInvitationLocked(token string) int {
	sum := sha256.Sum256([]byte(token))
	candidate := []byte(hex.EncodeToString(sum[:]))
	match := -1
	for i := range m.state.Invitations {
		stored := []byte(strings.ToLower(m.state.Invitations[i].TokenHash))
		if len(stored) == len(candidate) && subtle.ConstantTimeCompare(stored, candidate) == 1 {
			match = i
		}
	}
	return match
}

func (m *Manager) requestIndexLocked(requestID string) int {
	for i := range m.state.Requests {
		if m.state.Requests[i].ID == requestID {
			return i
		}
	}
	return -1
}

func (m *Manager) uniqueTokenLocked() (string, string, error) {
	for attempt := 0; attempt < 8; attempt++ {
		token, err := randomHex(m.opts.Random, 32)
		if err != nil {
			return "", "", err
		}
		sum := sha256.Sum256([]byte(token))
		hash := hex.EncodeToString(sum[:])
		duplicate := false
		for _, invitation := range m.state.Invitations {
			if invitation.TokenHash == hash {
				duplicate = true
				break
			}
		}
		if !duplicate {
			return token, hash, nil
		}
	}
	return "", "", errors.New("could not generate a unique enrollment token")
}

func (m *Manager) uniqueIDLocked(prefix string, exists func(string) bool) (string, error) {
	for attempt := 0; attempt < 8; attempt++ {
		raw, err := randomHex(m.opts.Random, 16)
		if err != nil {
			return "", err
		}
		candidate := prefix + "-" + raw
		if !exists(candidate) {
			return candidate, nil
		}
	}
	return "", fmt.Errorf("could not generate a unique %s ID", prefix)
}

func (m *Manager) uniqueAuditIDLocked() (string, error) {
	return m.uniqueIDLocked("audit", func(candidate string) bool {
		for _, record := range m.state.Audit {
			if record.ID == candidate {
				return true
			}
		}
		return false
	})
}

func (m *Manager) expireLocked(now time.Time) error {
	originalRequests := cloneRequests(m.state.Requests)
	originalAuditLength := len(m.state.Audit)
	changed := false
	for i := range m.state.Requests {
		request := &m.state.Requests[i]
		if request.Status != StatusPending && request.Status != StatusApprovalFailed {
			continue
		}
		if now.Before(request.ExpiresAt) {
			continue
		}
		auditID, err := m.uniqueAuditIDLocked()
		if err != nil {
			m.state.Requests = originalRequests
			m.state.Audit = m.state.Audit[:originalAuditLength]
			return err
		}
		request.Status = StatusExpired
		request.UpdatedAt = now
		request.DecidedAt = timePointer(now)
		request.DecidedBy = "system"
		request.LastError = ""
		m.state.Audit = append(m.state.Audit, auditForRequest(
			auditID,
			*request,
			"request_expired",
			"expired",
			"system",
			now,
			"",
		))
		changed = true
	}
	if !changed {
		return nil
	}
	if err := m.persistLocked(); err != nil {
		m.state.Requests = originalRequests
		m.state.Audit = m.state.Audit[:originalAuditLength]
		return err
	}
	return nil
}

func auditForRequest(
	id string,
	request Request,
	action string,
	result string,
	actor string,
	now time.Time,
	detail string,
) AuditRecord {
	return AuditRecord{
		ID:                   id,
		RequestID:            request.ID,
		Action:               action,
		Result:               result,
		PeerID:               request.PeerID,
		DisplayName:          request.DisplayName,
		PublicKeyFingerprint: request.PublicKeyFingerprint,
		SourceAddress:        request.SourceAddress,
		Actor:                actor,
		Timestamp:            now,
		ExpiresAt:            request.ExpiresAt,
		Detail:               detail,
	}
}

func normalizeSubmission(submission Submission) (Submission, error) {
	var err error
	if len(submission.Token) == 0 || len(submission.Token) > maxTokenBytes {
		return Submission{}, fmt.Errorf("%w: token length is invalid", ErrInvalidToken)
	}
	submission.PeerID, err = validateIdentifier("peer ID", submission.PeerID, maxPeerIDBytes)
	if err != nil {
		return Submission{}, err
	}
	submission.DisplayName, err = validateIdentifier("display name", submission.DisplayName, maxDisplayNameBytes)
	if err != nil {
		return Submission{}, err
	}
	submission.PublicKeyFingerprint, err = validateIdentifier(
		"public-key fingerprint",
		submission.PublicKeyFingerprint,
		maxFingerprintBytes,
	)
	if err != nil {
		return Submission{}, err
	}
	submission.PublicKeyFingerprint = strings.ToLower(submission.PublicKeyFingerprint)
	submission.Nonce, err = validateIdentifier("nonce", submission.Nonce, maxNonceBytes)
	if err != nil {
		return Submission{}, err
	}
	return submission, nil
}

func validateIdentifier(name, value string, maximum int) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", fmt.Errorf("%w: %s is required", ErrInvalidRequest, name)
	}
	if !utf8.ValidString(value) || len(value) > maximum {
		return "", fmt.Errorf("%w: %s exceeds its size limit or is not UTF-8", ErrInvalidRequest, name)
	}
	for _, r := range value {
		if r < 0x20 || r == 0x7f {
			return "", fmt.Errorf("%w: %s contains control characters", ErrInvalidRequest, name)
		}
	}
	return value, nil
}

func parseSourceAddress(source string) (string, net.IP, error) {
	source = strings.TrimSpace(source)
	if source == "" || len(source) > maxSourceBytes {
		return "", nil, ErrInvalidSource
	}
	host := source
	if parsedHost, _, err := net.SplitHostPort(source); err == nil {
		host = parsedHost
	} else {
		host = strings.TrimPrefix(strings.TrimSuffix(host, "]"), "[")
	}
	if zone := strings.LastIndex(host, "%"); zone >= 0 {
		host = host[:zone]
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return "", nil, ErrInvalidSource
	}
	return ip.String(), ip, nil
}

func blocksDuplicate(status Status) bool {
	return status == StatusPending || status == StatusApproving || status == StatusApprovalFailed || status == StatusApproved
}

func randomHex(reader io.Reader, size int) (string, error) {
	raw := make([]byte, size)
	if _, err := io.ReadFull(reader, raw); err != nil {
		return "", fmt.Errorf("generate secure enrollment random value: %w", err)
	}
	return hex.EncodeToString(raw), nil
}

func cloneInvitation(invitation storedInvitation) storedInvitation {
	if invitation.UsedAt != nil {
		value := *invitation.UsedAt
		invitation.UsedAt = &value
	}
	return invitation
}

func cloneRequest(request Request) Request {
	if request.DecidedAt != nil {
		value := *request.DecidedAt
		request.DecidedAt = &value
	}
	return request
}

func cloneRequests(requests []Request) []Request {
	out := make([]Request, len(requests))
	for i := range requests {
		out[i] = cloneRequest(requests[i])
	}
	return out
}

func timePointer(value time.Time) *time.Time {
	copy := value
	return &copy
}

func (m *Manager) load() error {
	data, err := os.ReadFile(m.opts.StatePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			m.state = emptyState()
			return nil
		}
		return fmt.Errorf("read enrollment state: %w", err)
	}
	if len(data) > maxStateBytes {
		return errors.New("enrollment state exceeds size limit")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var loaded persistedState
	if err := decoder.Decode(&loaded); err != nil {
		return fmt.Errorf("decode enrollment state: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("decode enrollment state: trailing JSON value")
	}
	if err := validatePersistedState(loaded); err != nil {
		return err
	}
	if loaded.Invitations == nil {
		loaded.Invitations = []storedInvitation{}
	}
	if loaded.Requests == nil {
		loaded.Requests = []Request{}
	}
	if loaded.Audit == nil {
		loaded.Audit = []AuditRecord{}
	}
	m.state = loaded
	return nil
}

func validatePersistedState(state persistedState) error {
	if state.Version != 1 {
		return fmt.Errorf("unsupported enrollment state version %d", state.Version)
	}
	invitationIDs := make(map[string]struct{}, len(state.Invitations))
	for _, invitation := range state.Invitations {
		if _, err := validateIdentifier("invitation ID", invitation.ID, 128); err != nil {
			return fmt.Errorf("invalid enrollment state: %w", err)
		}
		if _, ok := invitationIDs[invitation.ID]; ok {
			return errors.New("invalid enrollment state: duplicate invitation ID")
		}
		invitationIDs[invitation.ID] = struct{}{}
		hash, err := hex.DecodeString(invitation.TokenHash)
		if err != nil || len(hash) != sha256.Size {
			return errors.New("invalid enrollment state: malformed token hash")
		}
		if invitation.CreatedAt.IsZero() || invitation.ExpiresAt.IsZero() ||
			!invitation.ExpiresAt.After(invitation.CreatedAt) {
			return errors.New("invalid enrollment state: invitation timestamps are invalid")
		}
		if (invitation.UsedAt == nil) != (invitation.UsedByRequestID == "") {
			return errors.New("invalid enrollment state: incomplete invitation use record")
		}
	}

	requestIDs := make(map[string]struct{}, len(state.Requests))
	nonces := make(map[string]struct{}, len(state.Requests))
	activePeers := make(map[string]struct{}, len(state.Requests))
	activeFingerprints := make(map[string]struct{}, len(state.Requests))
	for _, request := range state.Requests {
		if _, err := validateIdentifier("request ID", request.ID, 128); err != nil {
			return fmt.Errorf("invalid enrollment state: %w", err)
		}
		if _, ok := requestIDs[request.ID]; ok {
			return errors.New("invalid enrollment state: duplicate request ID")
		}
		requestIDs[request.ID] = struct{}{}
		normalized, err := normalizeSubmission(Submission{
			Token:                "persisted",
			PeerID:               request.PeerID,
			DisplayName:          request.DisplayName,
			PublicKeyFingerprint: request.PublicKeyFingerprint,
			Nonce:                request.Nonce,
		})
		if err != nil || normalized.PeerID != request.PeerID ||
			normalized.DisplayName != request.DisplayName ||
			normalized.PublicKeyFingerprint != request.PublicKeyFingerprint ||
			normalized.Nonce != request.Nonce {
			return errors.New("invalid enrollment state: malformed request identity")
		}
		source, _, err := parseSourceAddress(request.SourceAddress)
		if err != nil || source != request.SourceAddress {
			return errors.New("invalid enrollment state: malformed request source")
		}
		if request.RequestedAt.IsZero() || request.UpdatedAt.IsZero() || request.ExpiresAt.IsZero() ||
			!request.ExpiresAt.After(request.RequestedAt) || request.UpdatedAt.Before(request.RequestedAt) {
			return errors.New("invalid enrollment state: request timestamps are invalid")
		}
		if _, exists := nonces[request.Nonce]; exists {
			return errors.New("invalid enrollment state: duplicate request nonce")
		}
		nonces[request.Nonce] = struct{}{}
		switch request.Status {
		case StatusPending:
			if request.DecidedAt != nil || request.DecidedBy != "" || request.LastError != "" {
				return errors.New("invalid enrollment state: pending request has decision metadata")
			}
		case StatusApproving:
			if request.DecidedAt != nil || request.DecidedBy == "" || request.LastError != "" {
				return errors.New("invalid enrollment state: malformed approval intent")
			}
		case StatusApprovalFailed:
			if request.DecidedAt != nil || request.DecidedBy == "" ||
				request.LastError != "approval persistence failed" {
				return errors.New("invalid enrollment state: malformed failed approval")
			}
		case StatusApproved, StatusDenied, StatusExpired:
			if request.DecidedAt == nil || request.DecidedBy == "" || request.LastError != "" {
				return errors.New("invalid enrollment state: terminal request lacks decision metadata")
			}
		default:
			return fmt.Errorf("invalid enrollment state: unknown request status %q", request.Status)
		}
		if blocksDuplicate(request.Status) {
			if _, exists := activePeers[request.PeerID]; exists {
				return errors.New("invalid enrollment state: duplicate active peer ID")
			}
			activePeers[request.PeerID] = struct{}{}
			if _, exists := activeFingerprints[request.PublicKeyFingerprint]; exists {
				return errors.New("invalid enrollment state: duplicate active public-key fingerprint")
			}
			activeFingerprints[request.PublicKeyFingerprint] = struct{}{}
		}
	}
	linkedRequests := make(map[string]struct{}, len(state.Requests))
	for _, invitation := range state.Invitations {
		if invitation.UsedByRequestID == "" {
			continue
		}
		if _, ok := requestIDs[invitation.UsedByRequestID]; !ok {
			return errors.New("invalid enrollment state: invitation references an unknown request")
		}
		if _, duplicate := linkedRequests[invitation.UsedByRequestID]; duplicate {
			return errors.New("invalid enrollment state: request uses multiple invitations")
		}
		linkedRequests[invitation.UsedByRequestID] = struct{}{}
	}
	if len(linkedRequests) != len(state.Requests) {
		return errors.New("invalid enrollment state: request has no invitation")
	}

	auditIDs := make(map[string]struct{}, len(state.Audit))
	approvalStarts := make(map[string]int)
	for _, record := range state.Audit {
		if _, err := validateIdentifier("audit ID", record.ID, 128); err != nil {
			return fmt.Errorf("invalid enrollment state: %w", err)
		}
		if _, ok := auditIDs[record.ID]; ok {
			return errors.New("invalid enrollment state: duplicate audit ID")
		}
		auditIDs[record.ID] = struct{}{}
		if _, ok := requestIDs[record.RequestID]; !ok {
			return errors.New("invalid enrollment state: audit references an unknown request")
		}
		if _, err := validateIdentifier("audit action", record.Action, 64); err != nil {
			return fmt.Errorf("invalid enrollment state: %w", err)
		}
		if _, err := validateIdentifier("audit result", record.Result, 64); err != nil {
			return fmt.Errorf("invalid enrollment state: %w", err)
		}
		if _, err := validateIdentifier("audit peer ID", record.PeerID, maxPeerIDBytes); err != nil {
			return fmt.Errorf("invalid enrollment state: %w", err)
		}
		if _, err := validateIdentifier("audit display name", record.DisplayName, maxDisplayNameBytes); err != nil {
			return fmt.Errorf("invalid enrollment state: %w", err)
		}
		if _, err := validateIdentifier("audit fingerprint", record.PublicKeyFingerprint, maxFingerprintBytes); err != nil {
			return fmt.Errorf("invalid enrollment state: %w", err)
		}
		if source, _, err := parseSourceAddress(record.SourceAddress); err != nil ||
			source != record.SourceAddress {
			return errors.New("invalid enrollment state: malformed audit source")
		}
		if record.Actor != "" {
			if _, err := validateIdentifier("audit actor", record.Actor, maxActorBytes); err != nil {
				return fmt.Errorf("invalid enrollment state: %w", err)
			}
		}
		if record.Timestamp.IsZero() || record.ExpiresAt.IsZero() || len(record.Detail) > 256 ||
			containsControlText(record.Detail) {
			return errors.New("invalid enrollment state: malformed audit metadata")
		}
		switch record.Action {
		case "request_submitted", "request_approved", "request_denied", "request_expired", "approval_failed":
		case "approval_started":
			approvalStarts[record.RequestID]++
		default:
			return errors.New("invalid enrollment state: unknown audit action")
		}
	}
	for _, request := range state.Requests {
		if request.Status == StatusApproving && approvalStarts[request.ID] != 1 {
			return errors.New("invalid enrollment state: approving request lacks a unique audit intent")
		}
	}
	return nil
}

func (m *Manager) persistLocked() error {
	writer := m.writeState
	if writer == nil {
		writer = atomicWriteJSON
	}
	return writer(m.opts.StatePath, m.state)
}

func atomicWriteJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("encode enrollment state: %w", err)
	}
	data = append(data, '\n')
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return fmt.Errorf("create enrollment state directory: %w", err)
	}
	file, err := os.CreateTemp(directory, ".enrollment-*.tmp")
	if err != nil {
		return fmt.Errorf("create enrollment state temporary file: %w", err)
	}
	tempPath := file.Name()
	defer os.Remove(tempPath)
	if err := file.Chmod(0o600); err != nil {
		file.Close()
		return fmt.Errorf("secure enrollment state temporary file: %w", err)
	}
	if _, err := file.Write(data); err != nil {
		file.Close()
		return fmt.Errorf("write enrollment state: %w", err)
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return fmt.Errorf("sync enrollment state: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close enrollment state: %w", err)
	}
	if err := replaceFileAtomic(tempPath, path); err != nil {
		return fmt.Errorf("replace enrollment state: %w", err)
	}
	if err := syncParentDirectory(directory); err != nil {
		return fmt.Errorf("sync enrollment state directory: %w", err)
	}
	return nil
}

func containsControlText(value string) bool {
	for _, r := range value {
		if r < 0x20 || r == 0x7f {
			return true
		}
	}
	return false
}
