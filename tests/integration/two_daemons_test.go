package integration

import "testing"

func TestTwoDaemonPromptFlow(t *testing.T) {
	t.Skip("Phase I integration scaffold: start two caduceusd instances with separate config/data dirs, allowlist each peer, point one at a fake OpenAI-compatible server, then assert discovery, streamed events, and final result.")
}
