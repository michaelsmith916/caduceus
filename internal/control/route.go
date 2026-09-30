package control

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/caduceus/caduceus/internal/tasks"
)

const maxRouteExplainBodyBytes = 1 << 20

// RouteExplainBackend is intentionally separate from Backend so adding the
// read-only route explanation endpoint does not break existing Backend
// implementations. Backends opt in by implementing this interface.
type RouteExplainBackend interface {
	ExplainRoute(ctx context.Context, req tasks.Request) (any, error)
}

func (s *Server) handleExplainRoute(w http.ResponseWriter, r *http.Request) Response {
	if r.Method != http.MethodPost {
		return Failure("method_not_allowed", "method not allowed", nil)
	}
	backend, ok := s.backend.(RouteExplainBackend)
	if !ok {
		return Failure("not_supported", "route explanation is not supported by this backend", nil)
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxRouteExplainBodyBytes)
	var req tasks.Request
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		return Failure("bad_request", "invalid JSON body", map[string]any{"error": err.Error()})
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return Failure("bad_request", "invalid JSON body", map[string]any{"error": err.Error()})
	}
	if err := tasks.ValidateRequest(req); err != nil {
		return Failure("bad_request", "invalid task request", map[string]any{"error": err.Error()})
	}

	data, err := backend.ExplainRoute(r.Context(), req)
	return from(data, err)
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("multiple JSON values are not allowed")
		}
		return err
	}
	return nil
}
