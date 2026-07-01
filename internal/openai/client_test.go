package openai

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/caduceus/caduceus/internal/tasks"
)

func TestChatNonStreaming(t *testing.T) {
	srv := newHTTPTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		fmt.Fprint(w, `{"choices":[{"message":{"role":"assistant","content":"hello"}}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`)
	}))
	defer srv.Close()

	client := New(srv.URL+"/v1", "", "test-model", time.Second)
	result, err := client.Chat(context.Background(), "task-1", tasks.PromptTask{Prompt: "hi"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.OutputText != "hello" {
		t.Fatalf("unexpected output %q", result.OutputText)
	}
	if result.Usage.TotalTokens != 2 {
		t.Fatalf("unexpected usage %#v", result.Usage)
	}
}

func TestChatStreaming(t *testing.T) {
	srv := newHTTPTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"he\"}}]}\n\n")
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"llo\"}}]}\n\n")
		fmt.Fprint(w, "data: [DONE]\n\n")
	}))
	defer srv.Close()

	client := New(srv.URL, "", "test-model", time.Second)
	var tokens string
	result, err := client.Chat(context.Background(), "task-1", tasks.PromptTask{Prompt: "hi", Stream: true}, func(e tasks.Event) error {
		tokens += e.Delta
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.OutputText != "hello" || tokens != "hello" {
		t.Fatalf("unexpected output result=%q tokens=%q", result.OutputText, tokens)
	}
}

func newHTTPTestServer(t *testing.T, handler http.Handler) *httptest.Server {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("local TCP listeners are unavailable in this environment: %v", err)
	}
	srv := httptest.NewUnstartedServer(handler)
	srv.Listener = ln
	srv.Start()
	return srv
}
