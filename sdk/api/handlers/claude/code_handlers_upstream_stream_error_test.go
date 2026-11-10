package claude

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/MoeForge/moego/ojson"
	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/contentalias"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/api/handlers"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v8/sdk/config"
)

// fable52-w3: the client-facing shape of a contentalias UpstreamStreamError.
// Before any byte is sent the client gets the upstream status (529/429) and
// error type; after headers are flushed it gets one SSE `error` event carrying
// the upstream type, never a 400 / contentalias:upstream_stream_error.
func TestClaudeMessagesHandlerContentAliasUpstreamStreamError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	const messageStart = "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[],\"model\":\"m\",\"stop_reason\":null,\"usage\":{\"input_tokens\":10,\"output_tokens\":0}}}\n\n"
	for _, tc := range []struct {
		name, kind string
		status     int
		committed  bool
	}{
		{"overloaded_before_headers", "overloaded_error", 529, false},
		{"rate_limit_before_headers", "rate_limit_error", http.StatusTooManyRequests, false},
		{"overloaded_mid_stream", "overloaded_error", 529, true},
		{"rate_limit_mid_stream", "rate_limit_error", http.StatusTooManyRequests, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			chunks := make(chan coreexecutor.StreamChunk, 2)
			executor := &preFTKeepAliveClaudeExecutor{chunks: chunks}
			manager := coreauth.NewManager(nil, nil, nil)
			manager.RegisterExecutor(executor)
			model := "upstream-stream-error-" + tc.name
			auth := &coreauth.Auth{ID: model + "-auth", Provider: executor.Identifier(), Status: coreauth.StatusActive}
			if _, err := manager.Register(context.Background(), auth); err != nil {
				t.Fatalf("register auth: %v", err)
			}
			registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: model}})
			t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(auth.ID) })

			if tc.committed {
				chunks <- coreexecutor.StreamChunk{Payload: []byte(messageStart)}
			}
			upErr := contentalias.UpstreamStreamErrorFromEvent([]byte(`{"type":"error","error":{"type":"` + tc.kind + `","message":"Overloaded"}}`))
			if upErr.StatusCode() != tc.status {
				t.Fatalf("status map: %d want %d", upErr.StatusCode(), tc.status)
			}
			chunks <- coreexecutor.StreamChunk{Err: upErr}
			close(chunks)

			h := NewClaudeCodeAPIHandler(handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager))
			router := gin.New()
			router.POST("/v1/messages", h.ClaudeMessages)
			req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"model":"`+model+`","messages":[{"role":"user","content":"hi"}],"stream":true}`))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)

			body := rec.Body.String()
			if strings.Contains(body, "upstream_stream_error") || strings.Contains(body, "contentalias:") || strings.Contains(body, "invalid_request_error") {
				t.Fatalf("masked upstream error: %d %q", rec.Code, body)
			}
			if !tc.committed {
				if rec.Code != tc.status {
					t.Fatalf("HTTP status %d want %d; body=%q", rec.Code, tc.status, body)
				}
				if got := ojson.GetBytes([]byte(body), "error.type").String(); got != tc.kind {
					t.Fatalf("error.type %q want %q; body=%q", got, tc.kind, body)
				}
				return
			}
			if rec.Code != http.StatusOK {
				t.Fatalf("committed stream status %d", rec.Code)
			}
			if strings.Count(body, "event: error\n") != 1 || !strings.Contains(body, `"type":"`+tc.kind+`"`) || !strings.Contains(body, "Overloaded") {
				t.Fatalf("want one upstream error event of type %s; body=%q", tc.kind, body)
			}
		})
	}
}
