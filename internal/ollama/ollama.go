package ollama

import (
	"context"
	"net/http"
	"os/exec"
	"strings"
	"time"
)

type Status struct {
	BinaryFound  bool   `json:"binary_found"`
	BinaryPath   string `json:"binary_path,omitempty"`
	APIReachable bool   `json:"api_reachable"`
	BaseURL      string `json:"base_url"`
	Message      string `json:"message"`
}

func Check(ctx context.Context, baseURL string) Status {
	if strings.TrimSpace(baseURL) == "" {
		baseURL = "http://127.0.0.1:11434/v1"
	}
	status := Status{BaseURL: baseURL}
	if path, err := exec.LookPath("ollama"); err == nil {
		status.BinaryFound = true
		status.BinaryPath = path
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(baseURL, "/")+"/models", nil)
	if err == nil {
		client := &http.Client{Timeout: 2 * time.Second}
		resp, err := client.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			status.APIReachable = resp.StatusCode >= 200 && resp.StatusCode < 500
		}
	}
	switch {
	case status.BinaryFound && status.APIReachable:
		status.Message = "Ollama binary found and OpenAI-compatible endpoint is reachable."
	case status.BinaryFound:
		status.Message = "Ollama binary found, but the OpenAI-compatible endpoint is not reachable. Start Ollama and ensure it exposes /v1."
	default:
		status.Message = "Ollama was not found. Install it only if you want the default local backend."
	}
	return status
}
