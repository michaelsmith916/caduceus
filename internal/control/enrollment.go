package control

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/caduceus/caduceus/internal/enrollment"
)

const maxEnrollmentBodyBytes = 64 << 10

// EnrollmentAdminBackend is optional: a daemon exposes enrollment
// administration only when its backend implements this interface.
type EnrollmentAdminBackend interface {
	CreateEnrollmentInvitation(context.Context) (any, error)
	ListEnrollmentRequests(context.Context) (any, error)
	GetEnrollmentRequest(context.Context, string) (any, error)
	ApproveEnrollment(context.Context, string, string) (any, error)
	DenyEnrollment(context.Context, string, string) (any, error)
	EnrollmentAudit(context.Context) (any, error)
}

// EnrollmentApplicantBackend is kept separate from administration so a
// backend may expose only the side of the enrollment flow that it supports.
type EnrollmentApplicantBackend interface {
	SubmitEnrollmentRequest(context.Context, EnrollmentSubmitRequest) (any, error)
	CheckEnrollmentRequest(context.Context, EnrollmentStatusRequest) (any, error)
}

type EnrollmentDecisionRequest struct {
	Actor string `json:"actor,omitempty"`
}

type EnrollmentSubmitRequest struct {
	CoordinatorAddress string `json:"coordinator_address"`
	Token              string `json:"token"`
	DisplayName        string `json:"display_name,omitempty"`
}

type EnrollmentStatusRequest struct {
	CoordinatorAddress string `json:"coordinator_address"`
	RequestID          string `json:"request_id"`
}

func (s *Server) handleEnrollmentInvitations(_ http.ResponseWriter, r *http.Request) Response {
	if r.Method != http.MethodPost {
		return Failure("method_not_allowed", "method not allowed", nil)
	}
	backend, ok := s.backend.(EnrollmentAdminBackend)
	if !ok {
		return Failure("not_supported", "trusted-LAN enrollment administration is not supported by this backend", nil)
	}
	data, err := backend.CreateEnrollmentInvitation(r.Context())
	return fromEnrollment(data, err)
}

func (s *Server) handleEnrollmentRequests(w http.ResponseWriter, r *http.Request) Response {
	backend, ok := s.backend.(EnrollmentAdminBackend)
	if !ok {
		return Failure("not_supported", "trusted-LAN enrollment administration is not supported by this backend", nil)
	}
	rest := strings.Trim(strings.TrimPrefix(r.URL.Path, "/v1/enrollment/requests"), "/")
	if rest == "" {
		if r.Method != http.MethodGet {
			return Failure("method_not_allowed", "method not allowed", nil)
		}
		data, err := backend.ListEnrollmentRequests(r.Context())
		return fromEnrollment(data, err)
	}
	parts := strings.Split(rest, "/")
	if len(parts) > 2 || !validEnrollmentIdentifier(parts[0]) {
		return Failure("bad_request", "a valid enrollment request id is required", nil)
	}
	requestID := parts[0]
	if len(parts) == 1 {
		if r.Method != http.MethodGet {
			return Failure("method_not_allowed", "method not allowed", nil)
		}
		data, err := backend.GetEnrollmentRequest(r.Context(), requestID)
		return fromEnrollment(data, err)
	}
	if r.Method != http.MethodPost {
		return Failure("method_not_allowed", "method not allowed", nil)
	}
	var decision EnrollmentDecisionRequest
	if err := decodeEnrollmentJSON(w, r, &decision, true); err != nil {
		return Failure("bad_request", "invalid JSON body", nil)
	}
	switch parts[1] {
	case "approve":
		data, err := backend.ApproveEnrollment(r.Context(), requestID, decision.Actor)
		return fromEnrollment(data, err)
	case "deny":
		data, err := backend.DenyEnrollment(r.Context(), requestID, decision.Actor)
		return fromEnrollment(data, err)
	default:
		return Failure("not_found", "unknown enrollment request action", nil)
	}
}

func (s *Server) handleEnrollmentAudit(_ http.ResponseWriter, r *http.Request) Response {
	if r.Method != http.MethodGet {
		return Failure("method_not_allowed", "method not allowed", nil)
	}
	backend, ok := s.backend.(EnrollmentAdminBackend)
	if !ok {
		return Failure("not_supported", "trusted-LAN enrollment administration is not supported by this backend", nil)
	}
	data, err := backend.EnrollmentAudit(r.Context())
	return fromEnrollment(data, err)
}

func (s *Server) handleEnrollmentSubmit(w http.ResponseWriter, r *http.Request) Response {
	if r.Method != http.MethodPost {
		return Failure("method_not_allowed", "method not allowed", nil)
	}
	backend, ok := s.backend.(EnrollmentApplicantBackend)
	if !ok {
		return Failure("not_supported", "trusted-LAN enrollment requests are not supported by this backend", nil)
	}
	var request EnrollmentSubmitRequest
	if err := decodeEnrollmentJSON(w, r, &request, false); err != nil {
		return Failure("bad_request", "invalid enrollment request", nil)
	}
	request.CoordinatorAddress = strings.TrimSpace(request.CoordinatorAddress)
	request.DisplayName = strings.TrimSpace(request.DisplayName)
	if request.CoordinatorAddress == "" || strings.TrimSpace(request.Token) == "" || len(request.CoordinatorAddress) > 2048 || len(request.Token) > 512 || len(request.DisplayName) > 128 {
		return Failure("bad_request", "invalid enrollment request", nil)
	}
	data, err := backend.SubmitEnrollmentRequest(r.Context(), request)
	return fromEnrollment(data, err)
}

func (s *Server) handleEnrollmentStatus(w http.ResponseWriter, r *http.Request) Response {
	if r.Method != http.MethodPost {
		return Failure("method_not_allowed", "method not allowed", nil)
	}
	backend, ok := s.backend.(EnrollmentApplicantBackend)
	if !ok {
		return Failure("not_supported", "trusted-LAN enrollment requests are not supported by this backend", nil)
	}
	var request EnrollmentStatusRequest
	if err := decodeEnrollmentJSON(w, r, &request, false); err != nil {
		return Failure("bad_request", "invalid enrollment status request", nil)
	}
	request.CoordinatorAddress = strings.TrimSpace(request.CoordinatorAddress)
	request.RequestID = strings.TrimSpace(request.RequestID)
	if request.CoordinatorAddress == "" || len(request.CoordinatorAddress) > 2048 || !validEnrollmentIdentifier(request.RequestID) {
		return Failure("bad_request", "invalid enrollment status request", nil)
	}
	data, err := backend.CheckEnrollmentRequest(r.Context(), request)
	return fromEnrollment(data, err)
}

func decodeEnrollmentJSON(w http.ResponseWriter, r *http.Request, target any, allowEmpty bool) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxEnrollmentBodyBytes)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	err := decoder.Decode(target)
	if allowEmpty && errors.Is(err, io.EOF) {
		return nil
	}
	if err != nil {
		return err
	}
	return ensureJSONEOF(decoder)
}

func validEnrollmentIdentifier(value string) bool {
	value = strings.TrimSpace(value)
	return value != "" && len(value) <= 512 && !strings.ContainsAny(value, "/\\\x00\r\n")
}

func fromEnrollment(data any, err error) Response {
	if err == nil {
		return Success(data)
	}
	code, message := enrollmentError(err)
	return Failure(code, message, nil)
}

func enrollmentError(err error) (string, string) {
	switch {
	case errors.Is(err, enrollment.ErrDisabled):
		return "enrollment_disabled", "trusted-LAN enrollment is disabled"
	case errors.Is(err, enrollment.ErrExpired):
		return "request_expired", "the enrollment request or invitation has expired"
	case errors.Is(err, enrollment.ErrNotFound):
		return "not_found", "enrollment request not found"
	case errors.Is(err, enrollment.ErrInvalidState):
		return "invalid_state", "the enrollment request is not pending"
	case errors.Is(err, enrollment.ErrApprovalFailed):
		return "approval_failed", "enrollment approval could not be persisted"
	case errors.Is(err, enrollment.ErrRateLimited):
		return "rate_limited", "too many enrollment requests"
	case errors.Is(err, enrollment.ErrSourceNotAllowed), errors.Is(err, enrollment.ErrInvalidSource):
		return "source_not_allowed", "the enrollment source is not allowed"
	case errors.Is(err, enrollment.ErrInvalidToken), errors.Is(err, enrollment.ErrReplay), errors.Is(err, enrollment.ErrNonceReplay),
		errors.Is(err, enrollment.ErrDuplicatePeer), errors.Is(err, enrollment.ErrDuplicateFingerprint), errors.Is(err, enrollment.ErrInvalidRequest):
		return "enrollment_rejected", "the enrollment request was rejected"
	default:
		return "request_failed", "the enrollment operation failed"
	}
}
