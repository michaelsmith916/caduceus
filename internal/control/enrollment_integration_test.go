package control

import (
	"context"
	"testing"
	"time"
)

func TestEnrollmentClientAndRegisteredRoutes(t *testing.T) {
	backend := &enrollmentBackendStub{
		invitation: map[string]any{"invitation": map[string]any{"token": "one-time"}},
		requests:   map[string]any{"requests": []any{}},
		request:    map[string]any{"id": "req-1"},
		decision:   map[string]any{"request_id": "req-1", "status": "pending"},
		audit:      map[string]any{"audit": []any{}},
	}
	server := NewServer("127.0.0.1:0", "control-token", backend)
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := server.Stop(ctx); err != nil {
			t.Errorf("stop server: %v", err)
		}
	}()
	client := NewClient("http://"+server.listener.Addr().String(), "control-token")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	operations := []func() (Response, error){
		func() (Response, error) { return client.CreateEnrollmentInvitation(ctx) },
		func() (Response, error) { return client.ListEnrollmentRequests(ctx) },
		func() (Response, error) { return client.GetEnrollmentRequest(ctx, "req-1") },
		func() (Response, error) { return client.ApproveEnrollment(ctx, "req-1", "operator") },
		func() (Response, error) { return client.DenyEnrollment(ctx, "req-1", "operator") },
		func() (Response, error) { return client.EnrollmentAudit(ctx) },
		func() (Response, error) {
			return client.SubmitEnrollment(ctx, EnrollmentSubmitRequest{
				CoordinatorAddress: "/ip4/192.0.2.2/tcp/23142/p2p/peer",
				Token:              "one-time",
				DisplayName:        "laptop",
			})
		},
		func() (Response, error) {
			return client.CheckEnrollment(ctx, EnrollmentStatusRequest{
				CoordinatorAddress: "/ip4/192.0.2.2/tcp/23142/p2p/peer",
				RequestID:          "req-1",
			})
		},
	}
	for index, operation := range operations {
		response, err := operation()
		if err != nil {
			t.Fatalf("operation %d: %v", index, err)
		}
		if !response.OK {
			t.Fatalf("operation %d response: %#v", index, response)
		}
	}
	if backend.submitted.Token != "one-time" || backend.checked.RequestID != "req-1" {
		t.Fatalf("unexpected applicant requests: submit=%#v status=%#v", backend.submitted, backend.checked)
	}
}
