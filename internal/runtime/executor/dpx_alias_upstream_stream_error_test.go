package executor

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/tidwall/gjson"
)

// fable52-w3: an Anthropic SSE `error` event mid-stream on an aliased response
// reached the client as HTTP 400 contentalias:upstream_stream_error
// (invalid_request_error), which the harness reads as a lane-shape failure.
// The stream chunk error must carry the upstream status and error type.
func TestDPXAliasUpstreamStreamErrorEventKeepsStatus(t *testing.T) {
	for _, tc := range []struct {
		kind   string
		status int
	}{
		{"overloaded_error", 529},
		{"rate_limit_error", http.StatusTooManyRequests},
		{"api_error", http.StatusInternalServerError},
	} {
		t.Run(tc.kind, func(t *testing.T) {
			e, req, opts := aliasFixture(t)
			opts.Stream = true
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte("event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_offline\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"" + req.Model + "\",\"content\":[],\"usage\":{\"input_tokens\":3,\"output_tokens\":0}}}\n\n"))
				_, _ = w.Write([]byte("event: ping\ndata: {\"type\":\"ping\"}\n\n"))
				_, _ = w.Write([]byte("event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"" + tc.kind + "\",\"message\":\"Overloaded\"}}\n\n"))
				w.(http.Flusher).Flush()
			}))
			defer upstream.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			result, err := e.ExecuteStream(ctx, offlineAuth(upstream.URL), req, opts)
			if err != nil {
				t.Fatalf("headers were 200; error must arrive on the stream, got %v", err)
			}
			var payload strings.Builder
			var streamErr error
			for chunk := range result.Chunks {
				payload.Write(chunk.Payload)
				if chunk.Err != nil {
					streamErr = chunk.Err
				}
			}
			if strings.Contains(payload.String(), "upstream_stream_error") {
				t.Fatalf("masked marker in payload: %s", payload.String())
			}
			if !strings.Contains(payload.String(), "message_start") {
				t.Fatalf("completed prefix not forwarded before error: %q", payload.String())
			}
			var sc interface{ StatusCode() int }
			if streamErr == nil || !errors.As(streamErr, &sc) || sc.StatusCode() != tc.status {
				t.Fatalf("want status %d stream error, got %v", tc.status, streamErr)
			}
			msg := streamErr.Error()
			if strings.Contains(msg, "contentalias:") || gjson.Get(msg, "error.type").String() != tc.kind || gjson.Get(msg, "error.message").String() != "Overloaded" {
				t.Fatalf("upstream error not preserved: %q", msg)
			}
		})
	}
}
