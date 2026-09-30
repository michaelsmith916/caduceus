package control

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/caduceus/caduceus/internal/enrollment"
)

type enrollmentBackendStub struct {
	Backend
	invitation any
	requests   any
	request    any
	audit      any
	decision   any
	submitted  EnrollmentSubmitRequest
	checked    EnrollmentStatusRequest
	requestID  string
	actor      string
	action     string
	err        error
}

func (b *enrollmentBackendStub) CreateEnrollmentInvitation(context.Context) (any, error) {
	return b.invitation, b.err
}

func (b *enrollmentBackendStub) ListEnrollmentRequests(context.Context) (any, error) {
	return b.requests, b.err
}

func (b *enrollmentBackendStub) GetEnrollmentRequest(_ context.Context, requestID string) (any, error) {
	b.requestID = requestID
	return b.request, b.err
}

func (b *enrollmentBackendStub) ApproveEnrollment(_ context.Context, requestID, actor string) (any, error) {
	b.requestID, b.actor, b.action = requestID, actor, "approve"
	return b.decision, b.err
}

func (b *enrollmentBackendStub) DenyEnrollment(_ context.Context, requestID, actor string) (any, error) {
	b.requestID, b.actor, b.action = requestID, actor, "deny"
	return b.decision, b.err
}

func (b *enrollmentBackendStub) EnrollmentAudit(context.Context) (any, error) {
	return b.audit, b.err
}

func (b *enrollmentBackendStub) SubmitEnrollmentRequest(_ context.Context, request EnrollmentSubmitRequest) (any, error) {
	b.submitted = request
	return b.decision, b.err
}

func (b *enrollmentBackendStub) CheckEnrollmentRequest(_ context.Context, request EnrollmentStatusRequest) (any, error) {
	b.checked = request
	return b.decision, b.err
}

func TestEnrollmentAdministrationRoutes(t *testing.T) {
	backend := &enrollmentBackendStub{
		invitation: map[string]any{"invitation": map[string]any{"token": "one-time-secret"}},
		requests:   map[string]any{"requests": []any{}},
		request:    map[string]any{"id": "req-1"},
		decision:   map[string]any{"id": "req-1", "status": "approved"},
		audit:      map[string]any{"audit": []any{}},
	}
	server := NewServer("unused", "", backend)
	tests := []struct {
		method string
		path   string
		body   string
	}{
		{http.MethodPost, "/v1/enrollment/invitations", ""},
		{http.MethodGet, "/v1/enrollment/requests", ""},
		{http.MethodGet, "/v1/enrollment/requests/req-1", ""},
		{http.MethodPost, "/v1/enrollment/requests/req-1/approve", `{"actor":"operator"}`},
		{http.MethodPost, "/v1/enrollment/requests/req-1/deny", `{}`},
		{http.MethodGet, "/v1/enrollment/audit", ""},
	}
	for _, test := range tests {
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(test.method, test.path, strings.NewReader(test.body))
		var handler func(http.ResponseWriter, *http.Request) Response
		switch {
		case test.path == "/v1/enrollment/invitations":
			handler = server.handleEnrollmentInvitations
		case test.path == "/v1/enrollment/audit":
			handler = server.handleEnrollmentAudit
		default:
			handler = server.handleEnrollmentRequests
		}
		server.wrap(handler).ServeHTTP(recorder, request)
		if recorder.Code != http.StatusOK {
			t.Fatalf("%s %s status=%d body=%s", test.method, test.path, recorder.Code, recorder.Body.String())
		}
	}
	if backend.requestID != "req-1" || backend.action != "deny" {
		t.Fatalf("unexpected final decision call: id=%q action=%q", backend.requestID, backend.action)
	}
}

func TestEnrollmentApplicantRoutesDoNotEchoToken(t *testing.T) {
	token := "secret-invitation-value"
	backend := &enrollmentBackendStub{decision: map[string]any{"request_id": "req-1", "status": "pending"}}
	server := NewServer("unused", "", backend)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/enrollment/submit", strings.NewReader(
		`{"coordinator_address":" /ip4/192.0.2.2/tcp/23142/p2p/peer ","token":"`+token+`","display_name":" laptop "}`,
	))
	server.wrap(server.handleEnrollmentSubmit).ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK || backend.submitted.Token != token || backend.submitted.DisplayName != "laptop" {
		t.Fatalf("submit status=%d request=%#v body=%s", recorder.Code, backend.submitted, recorder.Body.String())
	}
	if strings.Contains(recorder.Body.String(), token) {
		t.Fatalf("successful submission response echoed invitation token: %s", recorder.Body.String())
	}

	backend.err = fmt.Errorf("backend accidentally included %s: %w", token, enrollment.ErrInvalidToken)
	recorder = httptest.NewRecorder()
	request = httptest.NewRequest(http.MethodPost, "/v1/enrollment/submit", strings.NewReader(
		`{"coordinator_address":"/ip4/192.0.2.2/tcp/23142/p2p/peer","token":"`+token+`"}`,
	))
	server.wrap(server.handleEnrollmentSubmit).ServeHTTP(recorder, request)
	if strings.Contains(recorder.Body.String(), token) {
		t.Fatalf("failed submission response leaked invitation token: %s", recorder.Body.String())
	}
	var response Response
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Error == nil || response.Error.Code != "enrollment_rejected" {
		t.Fatalf("unexpected rejection response: %#v", response)
	}

	backend.err = nil
	recorder = httptest.NewRecorder()
	request = httptest.NewRequest(http.MethodPost, "/v1/enrollment/status", strings.NewReader(
		`{"coordinator_address":"/ip4/192.0.2.2/tcp/23142/p2p/peer","request_id":"req-1"}`,
	))
	server.wrap(server.handleEnrollmentStatus).ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK || backend.checked.RequestID != "req-1" {
		t.Fatalf("status poll status=%d request=%#v body=%s", recorder.Code, backend.checked, recorder.Body.String())
	}
}

func TestEnrollmentHandlersRejectInvalidBodiesAndUnsupportedBackends(t *testing.T) {
	backend := &enrollmentBackendStub{}
	server := NewServer("unused", "", backend)
	for _, body := range []string{
		`{"coordinator_address":"x","token":"y","unknown":true}`,
		`{"coordinator_address":"","token":"y"}`,
		`{"coordinator_address":"x","token":"y"} {}`,
	} {
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPost, "/v1/enrollment/submit", strings.NewReader(body))
		server.wrap(server.handleEnrollmentSubmit).ServeHTTP(recorder, request)
		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("body=%q status=%d response=%s", body, recorder.Code, recorder.Body.String())
		}
	}

	unsupported := NewServer("unused", "", legacyBackend{Backend: backend})
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/v1/enrollment/requests", nil)
	unsupported.wrap(unsupported.handleEnrollmentRequests).ServeHTTP(recorder, request)
	if recorder.Code != http.StatusNotImplemented {
		t.Fatalf("unsupported status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestEnrollmentErrorCodesAreStableAndSanitized(t *testing.T) {
	tests := []struct {
		err  error
		code string
	}{
		{enrollment.ErrDisabled, "enrollment_disabled"},
		{enrollment.ErrExpired, "request_expired"},
		{enrollment.ErrNotFound, "not_found"},
		{enrollment.ErrInvalidState, "invalid_state"},
		{enrollment.ErrApprovalFailed, "approval_failed"},
		{enrollment.ErrRateLimited, "rate_limited"},
		{enrollment.ErrSourceNotAllowed, "source_not_allowed"},
		{enrollment.ErrInvalidToken, "enrollment_rejected"},
		{errors.New("sensitive implementation detail"), "request_failed"},
	}
	for _, test := range tests {
		response := fromEnrollment(nil, fmt.Errorf("wrapped secret: %w", test.err))
		if response.OK || response.Error == nil || response.Error.Code != test.code {
			t.Fatalf("error %v produced %#v", test.err, response)
		}
		if strings.Contains(response.Error.Message, "secret") {
			t.Fatalf("error %v leaked wrapped detail: %#v", test.err, response)
		}
	}
}
