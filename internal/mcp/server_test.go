package mcp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestReadMessageJSONLines(t *testing.T) {
	body, framing, err := readMessage(bufio.NewReader(strings.NewReader("  {\"jsonrpc\":\"2.0\"}\r\n")))
	if err != nil {
		t.Fatal(err)
	}
	if framing != framingJSONLines {
		t.Fatalf("framing = %v, want JSON lines", framing)
	}
	if got := string(body); got != `{"jsonrpc":"2.0"}` {
		t.Fatalf("body = %q", got)
	}
}

func TestReadMessageContentLengthCompatibility(t *testing.T) {
	input := "Content-Length: 17\r\nContent-Type: application/json\r\n\r\n{\"jsonrpc\":\"2.0\"}"
	body, framing, err := readMessage(bufio.NewReader(strings.NewReader(input)))
	if err != nil {
		t.Fatal(err)
	}
	if framing != framingContentLength {
		t.Fatalf("framing = %v, want Content-Length", framing)
	}
	if got := string(body); got != `{"jsonrpc":"2.0"}` {
		t.Fatalf("body = %q", got)
	}
}

func TestServerUsesJSONLinesForStandardClient(t *testing.T) {
	request := `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05"}}` + "\n"
	var output bytes.Buffer
	server := NewServer(nil, strings.NewReader(request), &output)

	if err := server.Serve(context.Background()); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(output.String(), "Content-Length:") {
		t.Fatalf("standard response used legacy framing: %q", output.String())
	}
	if strings.Count(output.String(), "\n") != 1 {
		t.Fatalf("response is not one JSON line: %q", output.String())
	}

	var response rpcResponse
	if err := json.Unmarshal(bytes.TrimSpace(output.Bytes()), &response); err != nil {
		t.Fatal(err)
	}
	if response.Error != nil {
		t.Fatalf("initialize error: %+v", response.Error)
	}
}

func TestServerHandlesJSONLineNotificationBeforeRequest(t *testing.T) {
	input := strings.Join([]string{
		`{"jsonrpc":"2.0","method":"notifications/initialized","params":{}}`,
		`{"jsonrpc":"2.0","id":2,"method":"ping","params":{}}`,
		"",
	}, "\n")
	var output bytes.Buffer
	server := NewServer(nil, strings.NewReader(input), &output)

	if err := server.Serve(context.Background()); err != nil {
		t.Fatal(err)
	}
	if strings.Count(output.String(), "\n") != 1 {
		t.Fatalf("notification produced an unexpected response: %q", output.String())
	}

	var response rpcResponse
	if err := json.Unmarshal(bytes.TrimSpace(output.Bytes()), &response); err != nil {
		t.Fatal(err)
	}
	if string(response.ID) != "2" {
		t.Fatalf("response id = %s, want 2", response.ID)
	}
}

func TestServerPreservesLegacyResponseFraming(t *testing.T) {
	request, err := MarshalFrame(map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "ping",
	})
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	server := NewServer(nil, bytes.NewReader(request), &output)

	if err := server.Serve(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(output.String(), "Content-Length:") {
		t.Fatalf("legacy response framing was not preserved: %q", output.String())
	}
}
