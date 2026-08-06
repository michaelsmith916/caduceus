package control

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/caduceus/caduceus/internal/tasks"
)

type Client struct {
	endpoint string
	token    string
	client   *http.Client
	baseURL  string
}

func NewClient(endpoint, token string) *Client {
	c := &Client{endpoint: endpoint, token: token}
	if strings.HasPrefix(endpoint, "unix://") {
		socket := strings.TrimPrefix(endpoint, "unix://")
		c.baseURL = "http://unix"
		c.client = &http.Client{
			Timeout: 0,
			Transport: &http.Transport{
				DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
					var d net.Dialer
					return d.DialContext(ctx, "unix", socket)
				},
			},
		}
		return c
	}
	c.baseURL = strings.TrimRight(endpoint, "/")
	c.client = &http.Client{Timeout: 0}
	return c
}

func (c *Client) Status(ctx context.Context) (Response, error) {
	return c.get(ctx, "/v1/status")
}

func (c *Client) Config(ctx context.Context) (Response, error) {
	return c.get(ctx, "/v1/config")
}

func (c *Client) ListWorkers(ctx context.Context) (Response, error) {
	return c.get(ctx, "/v1/workers")
}

func (c *Client) GetWorker(ctx context.Context, workerID string) (Response, error) {
	return c.get(ctx, "/v1/workers/"+url.PathEscape(workerID))
}

func (c *Client) RunTask(ctx context.Context, req RunTaskRequest) (Response, error) {
	return c.post(ctx, "/v1/tasks/run", req)
}

func (c *Client) ValidateTask(ctx context.Context, req RunTaskRequest) (Response, error) {
	return c.post(ctx, "/v1/tasks/validate", req)
}

func (c *Client) ExplainRoute(ctx context.Context, req tasks.Request) (Response, error) {
	return c.post(ctx, "/v1/route/explain", req)
}

func (c *Client) CreateEnrollmentInvitation(ctx context.Context) (Response, error) {
	return c.post(ctx, "/v1/enrollment/invitations", map[string]any{})
}

func (c *Client) ListEnrollmentRequests(ctx context.Context) (Response, error) {
	return c.get(ctx, "/v1/enrollment/requests")
}

func (c *Client) GetEnrollmentRequest(ctx context.Context, requestID string) (Response, error) {
	return c.get(ctx, "/v1/enrollment/requests/"+url.PathEscape(requestID))
}

func (c *Client) ApproveEnrollment(ctx context.Context, requestID, actor string) (Response, error) {
	return c.post(ctx, "/v1/enrollment/requests/"+url.PathEscape(requestID)+"/approve", EnrollmentDecisionRequest{Actor: actor})
}

func (c *Client) DenyEnrollment(ctx context.Context, requestID, actor string) (Response, error) {
	return c.post(ctx, "/v1/enrollment/requests/"+url.PathEscape(requestID)+"/deny", EnrollmentDecisionRequest{Actor: actor})
}

func (c *Client) EnrollmentAudit(ctx context.Context) (Response, error) {
	return c.get(ctx, "/v1/enrollment/audit")
}

func (c *Client) SubmitEnrollment(ctx context.Context, req EnrollmentSubmitRequest) (Response, error) {
	return c.post(ctx, "/v1/enrollment/submit", req)
}

func (c *Client) CheckEnrollment(ctx context.Context, req EnrollmentStatusRequest) (Response, error) {
	return c.post(ctx, "/v1/enrollment/status", req)
}

func (c *Client) ListTasks(ctx context.Context, status string) (Response, error) {
	path := "/v1/tasks"
	if status != "" {
		path += "?status=" + url.QueryEscape(status)
	}
	return c.get(ctx, path)
}

func (c *Client) GetTaskStatus(ctx context.Context, taskID string) (Response, error) {
	return c.get(ctx, "/v1/tasks/"+url.PathEscape(taskID)+"/status")
}

func (c *Client) GetTaskResult(ctx context.Context, taskID string) (Response, error) {
	return c.get(ctx, "/v1/tasks/"+url.PathEscape(taskID)+"/result")
}

func (c *Client) GetTaskEvents(ctx context.Context, taskID string, cursor int64) (Response, error) {
	return c.get(ctx, fmt.Sprintf("/v1/tasks/%s/events?cursor=%d", url.PathEscape(taskID), cursor))
}

func (c *Client) GetTaskArtifacts(ctx context.Context, taskID string) (Response, error) {
	return c.get(ctx, "/v1/tasks/"+url.PathEscape(taskID)+"/artifacts")
}

func (c *Client) CancelTask(ctx context.Context, taskID string) (Response, error) {
	return c.post(ctx, "/v1/tasks/"+url.PathEscape(taskID)+"/cancel", map[string]any{})
}

func (c *Client) get(ctx context.Context, path string) (Response, error) {
	return c.do(ctx, http.MethodGet, path, nil)
}

func (c *Client) post(ctx context.Context, path string, body any) (Response, error) {
	data, err := json.Marshal(body)
	if err != nil {
		return Response{}, err
	}
	return c.do(ctx, http.MethodPost, path, bytes.NewReader(data))
}

func (c *Client) do(ctx context.Context, method, path string, body io.Reader) (Response, error) {
	client := c.client
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, body)
	if err != nil {
		return Response{}, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := client.Do(req)
	if err != nil {
		return Response{}, err
	}
	defer resp.Body.Close()
	var out Response
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return Response{}, err
	}
	return out, nil
}
