package contentalias

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
)

// fable52-w3: an Anthropic SSE `error` event mid-stream (overloaded_error /
// rate_limit_error / api_error) used to be discarded and replaced by
// Error("upstream_stream_error"), a 400 that the harness reads as "this lane
// cannot serve the request shape" and so left the lane for the session. The
// rewriter must surface the upstream error type, message and real status.

func upstreamErrorEvent(kind, message string) []byte {
	b, _ := json.Marshal(map[string]any{"type": "error", "error": map[string]string{"type": kind, "message": message}})
	return []byte("event: error\ndata: " + string(b) + "\n\n")
}

func streamPrefixWithOpenBlock() []byte {
	var out []byte
	for _, v := range []any{
		map[string]any{"type": "message_start", "message": map[string]any{"content": []any{}, "usage": map[string]int{"input_tokens": 7}}},
		map[string]any{"type": "content_block_start", "index": 0, "content_block": map[string]any{"type": "text", "text": ""}},
		map[string]any{"type": "content_block_delta", "index": 0, "delta": map[string]string{"type": "text_delta", "text": "partial "}},
		map[string]any{"type": "ping"},
	} {
		out = append(out, event(v)...)
	}
	return out
}

func TestStreamUpstreamErrorEventKeepsStatusAndType(t *testing.T) {
	cases := []struct {
		kind   string
		status int
	}{
		{"overloaded_error", 529},
		{"rate_limit_error", 429},
		{"api_error", 500},
		{"authentication_error", 401},
		{"permission_error", 403},
		{"not_found_error", 404},
		{"invalid_request_error", 400},
		{"some_future_error", 502},
		{"", 502},
	}
	for _, tc := range cases {
		t.Run(tc.kind, func(t *testing.T) {
			s := testSession(t)
			_, m, err := Prepare([]byte(requestFixture), s)
			if err != nil {
				t.Fatal(err)
			}
			raw := append(streamPrefixWithOpenBlock(), upstreamErrorEvent(tc.kind, "Overloaded")...)
			for split := 0; split <= len(raw); split++ {
				st := m.NewStream()
				a, errA := st.Feed(raw[:split])
				var b []byte
				var errB error
				if errA == nil {
					b, errB = st.Feed(raw[split:])
				}
				err := errA
				if err == nil {
					err = errB
				}
				var up *UpstreamStreamError
				if !errors.As(err, &up) {
					t.Fatalf("split %d: want *UpstreamStreamError, got %T %v", split, err, err)
				}
				if up.StatusCode() != tc.status {
					t.Fatalf("split %d: status %d want %d", split, up.StatusCode(), tc.status)
				}
				if up.Type() != tc.kind {
					t.Fatalf("split %d: type %q want %q", split, up.Type(), tc.kind)
				}
				msg := up.Error()
				if strings.Contains(msg, "upstream_stream_error") || strings.Contains(msg, "contentalias:") {
					t.Fatalf("split %d: masked error %q", split, msg)
				}
				var body struct {
					Type  string `json:"type"`
					Error struct{ Type, Message string }
				}
				if json.Unmarshal([]byte(msg), &body) != nil || body.Type != "error" || body.Error.Type != tc.kind || body.Error.Message != "Overloaded" {
					t.Fatalf("split %d: upstream body not preserved: %q", split, msg)
				}
				if tc.kind != "invalid_request_error" && body.Error.Type == "invalid_request_error" {
					t.Fatalf("split %d: upstream fault relabelled invalid_request_error", split)
				}
				// Complete frames before the error are released in order; the
				// error event itself is not (the caller emits it once).
				got := append(a, b...)
				if !bytes.Contains(got, []byte(`"message_start"`)) {
					t.Fatalf("split %d: completed prefix not released: %q", split, got)
				}
				if bytes.Contains(got, []byte(`"type":"error"`)) {
					t.Fatalf("split %d: error event leaked into payload", split)
				}
				// Stream is terminal after the upstream error.
				if _, err := st.Feed([]byte("data: {\"type\":\"ping\"}\n\n")); err == nil {
					t.Fatalf("split %d: stream accepted data after upstream error", split)
				}
			}
		})
	}
}

// The executor path reads through Reader/ReaderObserved; the error must keep
// its type through the io.Reader adaptor (no rewrap into Error).
func TestReaderSurfacesUpstreamStreamError(t *testing.T) {
	s := testSession(t)
	_, m, err := Prepare([]byte(requestFixture), s)
	if err != nil {
		t.Fatal(err)
	}
	raw := append(streamPrefixWithOpenBlock(), upstreamErrorEvent("overloaded_error", "Overloaded")...)
	got, err := io.ReadAll(m.ReaderObserved(bytes.NewReader(raw), func([]byte) {}))
	var up *UpstreamStreamError
	if !errors.As(err, &up) || up.StatusCode() != 529 {
		t.Fatalf("want 529 UpstreamStreamError, got %T %v", err, err)
	}
	var sc interface{ StatusCode() int }
	if !errors.As(err, &sc) || sc.StatusCode() == 400 {
		t.Fatalf("status must not be 400: %v", err)
	}
	if !bytes.Contains(got, []byte(`"message_start"`)) {
		t.Fatalf("prefix not released before error: %q", got)
	}
	if _, err := m.RestoreSSE(raw); !errors.As(err, &up) || up.StatusCode() != 529 {
		t.Fatalf("RestoreSSE: want 529 UpstreamStreamError, got %v", err)
	}
}

// An upstream error message that carries an alias is restored to the caller's
// vocabulary, like the non-2xx-at-headers path (RestoreErrorBody).
func TestUpstreamStreamErrorRestoresAliases(t *testing.T) {
	s := testSession(t)
	wire, m, err := Prepare([]byte(requestFixture), s)
	if err != nil {
		t.Fatal(err)
	}
	n, _ := parse(wire)
	alias := n.get("tools").items[0].get("name").str()
	raw := append(event(map[string]any{"type": "message_start", "message": map[string]any{"content": []any{}}}), upstreamErrorEvent("api_error", "tool "+alias+" failed")...)
	_, err = m.NewStream().Feed(raw)
	var up *UpstreamStreamError
	if !errors.As(err, &up) || up.StatusCode() != 500 {
		t.Fatalf("want 500, got %v", err)
	}
	if strings.Contains(up.Error(), alias) || !strings.Contains(up.Error(), "tool Bash failed") {
		t.Fatalf("alias not restored: %q", up.Error())
	}
}

// Genuine shape violations keep their contentalias 400 codes.
func TestStreamShapeViolationsStay400(t *testing.T) {
	s := testSession(t)
	_, m, err := Prepare([]byte(requestFixture), s)
	if err != nil {
		t.Fatal(err)
	}
	ms := event(map[string]any{"type": "message_start", "message": map[string]any{"content": []any{}}})
	cases := map[string][]byte{
		"stream_order": append(append([]byte{}, ms...), ms...),
		"stream_event": append(append([]byte{}, ms...), event(map[string]any{"type": "bogus"})...),
	}
	for code, raw := range cases {
		_, err := m.NewStream().Feed(raw)
		var e Error
		if !errors.As(err, &e) || string(e) != code || e.StatusCode() != 400 {
			t.Fatalf("%s: got %T %v", code, err, err)
		}
	}
	st := m.NewStream()
	if _, err := st.Feed(ms); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Finish(); err == nil || err.Error() != "contentalias:stream_eof" {
		t.Fatalf("stream_eof: got %v", err)
	}
}
