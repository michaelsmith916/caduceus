package openai

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/caduceus/caduceus/internal/tasks"
)

type Client struct {
	BaseURL    string
	APIKey     string
	Model      string
	HTTPClient *http.Client
}

type EventCallback func(tasks.Event) error

func New(baseURL, apiKey, model string, timeout time.Duration) *Client {
	if strings.TrimSpace(baseURL) == "" {
		baseURL = "http://127.0.0.1:11434/v1"
	}
	if strings.TrimSpace(model) == "" {
		model = "llama3.2"
	}
	if timeout <= 0 {
		timeout = 300 * time.Second
	}
	return &Client{
		BaseURL: strings.TrimRight(baseURL, "/"),
		APIKey:  apiKey,
		Model:   model,
		HTTPClient: &http.Client{
			Timeout: timeout,
		},
	}
}

func (c *Client) Chat(ctx context.Context, taskID string, prompt tasks.PromptTask, emit EventCallback) (tasks.Result, error) {
	if strings.TrimSpace(prompt.Model) == "" {
		prompt.Model = c.Model
	}
	if strings.TrimSpace(prompt.Model) == "" {
		return tasks.Result{}, errors.New("model is required")
	}
	reqBody := chatRequest{
		Model:       prompt.Model,
		Messages:    messages(prompt),
		Temperature: prompt.Temperature,
		MaxTokens:   prompt.MaxTokens,
		Stream:      prompt.Stream,
	}
	body, err := json.Marshal(reqBody)
	if err != nil {
		return tasks.Result{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint("/chat/completions"), bytes.NewReader(body))
	if err != nil {
		return tasks.Result{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.APIKey)
	}
	resp, err := c.httpClient().Do(req)
	if err != nil {
		return tasks.Result{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return tasks.Result{}, fmt.Errorf("openai chat failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(data)))
	}
	if prompt.Stream {
		return c.readStream(resp.Body, taskID, emit)
	}
	return readCompletion(resp.Body, taskID)
}

func (c *Client) ListModels(ctx context.Context) ([]string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.endpoint("/models"), nil)
	if err != nil {
		return nil, err
	}
	if c.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.APIKey)
	}
	resp, err := c.httpClient().Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("list models failed: status=%d", resp.StatusCode)
	}
	var payload struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(payload.Data))
	for _, model := range payload.Data {
		if model.ID != "" {
			out = append(out, model.ID)
		}
	}
	return out, nil
}

func (c *Client) readStream(r io.Reader, taskID string, emit EventCallback) (tasks.Result, error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	var output strings.Builder
	var usage tasks.Usage
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, ":") {
			continue
		}
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if data == "[DONE]" {
			break
		}
		var chunk chatChunk
		if err := json.Unmarshal([]byte(data), &chunk); err != nil {
			return tasks.Result{}, fmt.Errorf("parse stream chunk: %w", err)
		}
		if chunk.Usage != nil {
			usage = toUsage(*chunk.Usage)
		}
		for _, choice := range chunk.Choices {
			delta := choice.Delta.Content
			if delta == "" {
				delta = choice.Message.Content
			}
			if delta == "" {
				continue
			}
			output.WriteString(delta)
			if emit != nil {
				if err := emit(tasks.NewEvent(taskID, 0, tasks.EventToken, "", delta)); err != nil {
					return tasks.Result{}, err
				}
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return tasks.Result{}, err
	}
	return tasks.Result{
		TaskID:     taskID,
		Status:     tasks.StatusCompleted,
		OutputText: output.String(),
		Error:      nil,
		Usage:      usage,
		Artifacts:  []tasks.Artifact{},
	}, nil
}

func readCompletion(r io.Reader, taskID string) (tasks.Result, error) {
	var payload chatCompletion
	if err := json.NewDecoder(r).Decode(&payload); err != nil {
		return tasks.Result{}, err
	}
	var output strings.Builder
	for _, choice := range payload.Choices {
		output.WriteString(choice.Message.Content)
	}
	return tasks.Result{
		TaskID:     taskID,
		Status:     tasks.StatusCompleted,
		OutputText: output.String(),
		Error:      nil,
		Usage:      toUsage(payload.Usage),
		Artifacts:  []tasks.Artifact{},
	}, nil
}

func (c *Client) endpoint(path string) string {
	base := strings.TrimRight(c.BaseURL, "/")
	u, err := url.Parse(base)
	if err != nil || u.Scheme == "" {
		return base + path
	}
	return base + path
}

func (c *Client) httpClient() *http.Client {
	if c.HTTPClient != nil {
		return c.HTTPClient
	}
	return http.DefaultClient
}

func messages(prompt tasks.PromptTask) []chatMessage {
	out := []chatMessage{}
	if strings.TrimSpace(prompt.System) != "" {
		out = append(out, chatMessage{Role: "system", Content: prompt.System})
	}
	out = append(out, chatMessage{Role: "user", Content: prompt.Prompt})
	return out
}

func toUsage(u usage) tasks.Usage {
	return tasks.Usage{
		PromptTokens:     u.PromptTokens,
		CompletionTokens: u.CompletionTokens,
		TotalTokens:      u.TotalTokens,
	}
}

type chatRequest struct {
	Model       string        `json:"model"`
	Messages    []chatMessage `json:"messages"`
	Temperature *float64      `json:"temperature,omitempty"`
	MaxTokens   int           `json:"max_tokens,omitempty"`
	Stream      bool          `json:"stream"`
}

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

type chatCompletion struct {
	Choices []struct {
		Message chatMessage `json:"message"`
	} `json:"choices"`
	Usage usage `json:"usage"`
}

type chatChunk struct {
	Choices []struct {
		Delta   chatMessage `json:"delta"`
		Message chatMessage `json:"message"`
	} `json:"choices"`
	Usage *usage `json:"usage"`
}
