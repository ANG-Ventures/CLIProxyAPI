package executor

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/usage"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/translator"
	"github.com/tidwall/gjson"
)

// aliasSeamHooks records the request bytes response hooks are handed.
type aliasSeamHooks struct {
	mu       sync.Mutex
	requests [][]byte
}

func (*aliasSeamHooks) NormalizeRequest(_ context.Context, _, _ translator.Format, _ string, body []byte, _ bool) []byte {
	return body
}

func (*aliasSeamHooks) TranslateRequest(context.Context, translator.Format, translator.Format, string, []byte, bool) ([]byte, bool) {
	return nil, false
}

func (h *aliasSeamHooks) NormalizeResponseBefore(_ context.Context, _, _ translator.Format, _ string, _, requestRawJSON, body []byte, _ bool) []byte {
	h.mu.Lock()
	h.requests = append(h.requests, bytes.Clone(requestRawJSON))
	h.mu.Unlock()
	return body
}

func (*aliasSeamHooks) TranslateResponse(context.Context, translator.Format, translator.Format, string, []byte, []byte, []byte, bool) ([]byte, bool) {
	return nil, false
}

func (*aliasSeamHooks) NormalizeResponseAfter(_ context.Context, _, _ translator.Format, _ string, _, _, body []byte, _ bool) []byte {
	return body
}

// F2 (t_32413204): response hooks pair the request with a RESTORED response, so
// the request they see must be the un-aliased one, on both executor paths.
func TestDPXAliasResponseHooksSeeUnaliasedRequest(t *testing.T) {
	// The claude->claude stream path forwards events without the translator, so
	// the stream case runs translated (the hooks only run inside the translator).
	for _, tc := range []struct{ stream, translated bool }{{false, false}, {false, true}, {true, true}} {
		stream := tc.stream
		t.Run(fmt.Sprintf("stream=%t/translated=%t", tc.stream, tc.translated), func(t *testing.T) {
			hooks := &aliasSeamHooks{}
			translator.SetPluginHooks(hooks)
			t.Cleanup(func() { translator.SetPluginHooks(nil) })
			e, req, opts := aliasFixture(t)
			if stream {
				req.Payload = append([]byte(`{"stream":true,`), req.Payload[1:]...)
				opts.OriginalRequest = req.Payload
				opts.Stream = true
			}
			if tc.translated {
				opts.ResponseFormat = translator.FromString("openai")
			}
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				raw, _ := io.ReadAll(r.Body)
				if !bytes.Contains(raw, []byte("dpx_v1_")) {
					t.Error("upstream body not aliased; test premise broken")
				}
				name := gjson.GetBytes(raw, "tools.0.name").String()
				key := ""
				gjson.GetBytes(raw, "tools.0.input_schema.properties").ForEach(func(k, v gjson.Result) bool { key = k.String(); return false })
				if gjson.GetBytes(raw, "stream").Bool() {
					w.Header().Set("Content-Type", "text/event-stream")
					w.Write(aliasSSE(name, key, req.Model))
				} else {
					w.Header().Set("Content-Type", "application/json")
					fmt.Fprintf(w, `{"content":[{"type":"tool_use","id":"c","name":%q,"input":{%q:"/tmp/Hermes"}}],"usage":{"input_tokens":5,"output_tokens":7}}`, name, key)
				}
			}))
			defer upstream.Close()
			auth := offlineAuth(upstream.URL)
			if stream {
				response, err := e.ExecuteStream(context.Background(), auth, req, opts)
				if err != nil {
					t.Fatal(err)
				}
				for chunk := range response.Chunks {
					if chunk.Err != nil {
						t.Fatal(chunk.Err)
					}
				}
			} else if _, err := e.Execute(context.Background(), auth, req, opts); err != nil {
				t.Fatal(err)
			}
			hooks.mu.Lock()
			defer hooks.mu.Unlock()
			if len(hooks.requests) == 0 {
				t.Fatal("response hook never ran")
			}
			for _, got := range hooks.requests {
				if bytes.Contains(got, []byte("dpx_v1_")) {
					t.Fatalf("response hook saw aliased request: %s", got)
				}
				if gjson.GetBytes(got, "tools.0.name").String() != "Read" {
					t.Fatalf("response hook request lost the original tool name: %s", got)
				}
			}
		})
	}
}

type aliasSplitUsageSink struct{ records chan usage.Record }

func (s *aliasSplitUsageSink) HandleUsage(_ context.Context, r usage.Record) {
	if r.AuthID == "alias-split-usage" {
		select {
		case s.records <- r:
		default:
		}
	}
}

// F3 (t_32413204): on the non-stream alias path over an SSE upstream, a usage
// frame split over two data: lines is counted once, fully. The alias path
// publishes usage before validateClaudeStreamingResponse (inherited fleet
// order), and that validation rejects the split frame, so the request itself
// still fails with 502 exactly as upstream v8.0.53 does; the usage record it
// leaves behind must carry the full split frame.
func TestDPXAliasNonStreamCountsSplitUsageFrame(t *testing.T) {
	sink := &aliasSplitUsageSink{make(chan usage.Record, 8)}
	usage.RegisterNamedPlugin("alias-split-usage-observer", sink)
	usage.StartDefault(context.Background())
	e, req, opts := aliasFixture(t)
	// A non-claude response format makes the executor stream from upstream.
	opts.ResponseFormat = translator.FromString("openai")
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		if !gjson.GetBytes(raw, "stream").Bool() {
			t.Error("expected an upstream stream; test premise broken")
		}
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "event: message_start\n"+
			`data: {"type":"message_start","message":{"id":"m","type":"message","role":"assistant","model":"`+req.Model+`","content":[],"usage":{"input_tokens":5,"output_tokens":0}}}`+"\n\n"+
			"event: content_block_start\n"+
			`data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`+"\n\n"+
			"event: content_block_delta\n"+
			`data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"ok"}}`+"\n\n"+
			"event: content_block_stop\n"+
			`data: {"type":"content_block_stop","index":0}`+"\n\n"+
			"event: message_delta\n"+
			`data: {"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},`+"\n"+
			`data: "usage":{"output_tokens":7}}`+"\n\n"+
			"event: message_stop\n"+
			`data: {"type":"message_stop"}`+"\n\n")
	}))
	defer upstream.Close()
	auth := offlineAuth(upstream.URL)
	auth.ID = "alias-split-usage"
	if _, err := e.Execute(context.Background(), auth, req, opts); err == nil || !strings.Contains(err.Error(), "malformed stream data") {
		t.Fatalf("expected the upstream split-frame validation error, got %v", err)
	}
	select {
	case record := <-sink.records:
		if record.Detail.InputTokens != 5 || record.Detail.OutputTokens != 7 {
			t.Fatalf("split usage frame under-counted: %+v", record.Detail)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no usage record")
	}
	select {
	case record := <-sink.records:
		t.Fatalf("usage published twice: %+v", record.Detail)
	case <-time.After(200 * time.Millisecond):
	}
}
