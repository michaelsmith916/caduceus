package mcp

import (
	"context"
	"encoding/json"
	"github.com/caduceus/caduceus/internal/control"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestReviewDisabledHermesModeBlocksEnrollmentTools(t *testing.T) {
	for _, tool := range []string{"caduceus.list_enrollment_requests", "caduceus.approve_enrollment", "caduceus.deny_enrollment"} {
		t.Run(tool, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/v1/config" {
					t.Errorf("disabled integration invoked %s", r.URL.Path)
				}
				_ = json.NewEncoder(w).Encode(control.Success(map[string]any{"config": map[string]any{"enrollment": map[string]any{"trusted_lan": map[string]any{"hermes_mode": "disabled"}}}}))
			}))
			defer server.Close()
			args := json.RawMessage(`{"request_id":"review-request"}`)
			if tool == "caduceus.list_enrollment_requests" {
				args = json.RawMessage(`{}`)
			}
			result, err := NewServer(control.NewClient(server.URL, ""), nil, nil).callTool(context.Background(), tool, args)
			if err != nil {
				t.Fatal(err)
			}
			response := toolStructuredResponse(t, result)
			if response.OK || response.Error == nil || response.Error.Code != "hermes_mode_disabled" {
				t.Fatalf("disabled integration succeeded: %+v", response)
			}
		})
	}
}
