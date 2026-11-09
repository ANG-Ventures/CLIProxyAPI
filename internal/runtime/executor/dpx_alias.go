package executor

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/contentalias"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/runtime/executor/helps"
	execpkg "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/translator"
)

func (e *ClaudeExecutor) dpxAliasEnabled() bool { return e.cfg != nil && e.cfg.DPXContentAlias.Enabled }

// dpxLaneGateEnabled is separate from aliasing (t_40200f8a): the d-family
// lane gate + W1 wirelog run on a re-sign-only unit (alias OFF — apx is the
// one aliasing layer per Ace 2026-09-29). Gated on the alias SECTION being
// declared (lane set or wirelog spool), so a legacy config without the
// section is untouched byte-for-byte.
func (e *ClaudeExecutor) dpxLaneGateEnabled() bool { return helps.DPXSectionDeclared(e.cfg) }

// dpxUnsafeDaemonConfig is the D4 guard (spec one-cliproxyapi-lineage §5 D4/D7,
// §7 Phase 2 Negative (a), AC9): the shape a declared d-family unit must have
// before the alias seam serves anything. Arms, each with its own test
// (dpx_daemon_shape_test.go):
//
//   - retry/log shape (original): request-log, debug, request-retry != 0,
//     max-retry-credentials != 1;
//
// and, when aliasing is ON (a re-sign-only unit with alias off carries no
// binding and is not an alias daemon):
//
//   - binding incomplete: principal, session-id or store-directory empty;
//   - more than one credential: AuthFileCount (the `*.json` count under
//     auth-dir snapshotted by the config loader; see config.DPXContentAlias)
//     above 1, or unreadable — one principal = one credential, the shared proxy
//     (many creds, bearer key, 0.0.0.0) can never be this daemon;
//   - routing.prompt-cache-policy is not the literal "off": anything else
//     (absent, "of", "Shadow", "shadow ", or shadow/enforce explicitly) would
//     run the resolver and write prompt fingerprints of the PRE-alias payload.
//
// The predicate reads the config snapshot only; no filesystem access on the
// request path.
func dpxUnsafeDaemonConfig(cfg *config.Config) bool {
	if cfg == nil {
		return true
	}
	if cfg.RequestLog || cfg.Debug || cfg.RequestRetry != 0 || cfg.MaxRetryCredentials != 1 {
		return true
	}
	alias := cfg.DPXContentAlias
	if !alias.Enabled {
		return false
	}
	if alias.Principal == "" || alias.SessionID == "" || alias.StoreDirectory == "" {
		return true
	}
	if alias.AuthFileCount > 1 || alias.AuthFileCount == config.DPXAuthDirUnreadable {
		return true
	}
	if !execpkg.PromptCachePolicyAliasSafe(cfg.Routing.PromptCachePolicy) {
		return true
	}
	return false
}

// prepareDPXAlias returns ctx carrying the Prepare timing for the wirelog row
// (t_997bc8a2) when it aliased; otherwise ctx unchanged.
func (e *ClaudeExecutor) prepareDPXAlias(ctx context.Context, raw []byte, opts execpkg.Options, native, cloaked bool) (context.Context, []byte, *contentalias.RequestMap, error) {
	if !e.dpxLaneGateEnabled() {
		return ctx, raw, nil, nil
	}
	if opts.SourceFormat != translator.FromString("claude") || !native || cloaked {
		return ctx, nil, nil, contentalias.Error("native_route_required")
	}
	if dpxUnsafeDaemonConfig(e.cfg) {
		return ctx, nil, nil, contentalias.Error("unsafe_daemon_config")
	}
	cfg := e.cfg.DPXContentAlias
	userAgent := ""
	if opts.Headers != nil {
		userAgent = opts.Headers.Get("User-Agent")
	}
	// The lane gate runs even when aliasing is off (a re-sign-only d-family
	// unit still admits only the entrypoints its lanes declare, t_40200f8a).
	if _, reason := helps.ResolveDPXRequestLane(cfg.EffectiveLanes(), raw, userAgent); reason != "" {
		return ctx, nil, nil, contentalias.Error(reason)
	}
	if !e.dpxAliasEnabled() {
		return ctx, raw, nil, nil
	}
	start := time.Now()
	session, err := contentalias.Open(cfg.StoreDirectory, contentalias.Binding{Principal: cfg.Principal, Session: cfg.SessionID, Version: cfg.Version}, contentalias.DefaultManifest())
	if err != nil {
		return ctx, nil, nil, err
	}
	wire, m, err := contentalias.Prepare(raw, session)
	ctx = helps.WithDPXPrepareTiming(ctx, helps.DPXPrepareTiming{Total: time.Since(start), LockWait: session.LockWait()})
	return ctx, wire, m, err
}

// dpxWirelogClient adds the W1 wirelog row writer to a declared d-family
// unit's upstream client (the alias SECTION present — lane and/or spool —
// even with alias enabled=false, the re-sign-only shape). Off unless
// wirelog-spool is set.
func (e *ClaudeExecutor) dpxWirelogClient(client *http.Client) *http.Client {
	if !e.dpxLaneGateEnabled() || e.cfg.DPXContentAlias.WirelogSpool == "" {
		return client
	}
	cfg := e.cfg.DPXContentAlias
	return helps.DPXWirelogClient(client, helps.DPXWirelogConfig{Spool: cfg.WirelogSpool, Lane: cfg.WirelogLane, Lanes: cfg.Lanes, Sub: cfg.WirelogSub, BrandWords: contentalias.DefaultManifest().Words,
		Bodies: cfg.WirelogBodies, BodySpool: cfg.WirelogBodySpool, MinFreeGB: cfg.WirelogMinFreeGB})
}

// validateDPXFinalBody enforces that nothing after aliasing changes the upstream
// body except CPA's native CCH signing of the Claude Code billing block: the
// " cch=00000;" placeholder insertion and the five signed digits. Every other
// byte of the aliased body must reach upstream unchanged.
func validateDPXFinalBody(before, after []byte, m *contentalias.RequestMap) error {
	if m == nil || bytes.Equal(before, after) {
		return nil
	}
	if bytes.Equal(withoutDPXCCHField(before), withoutDPXCCHField(after)) {
		return nil
	}
	return contentalias.Error("post_alias_mutation")
}

// withoutDPXCCHField returns body with the billing block's " cch=XXXXX;" field
// removed, or with only its digits zeroed when the field is not space-led.
func withoutDPXCCHField(body []byte) []byte {
	offset, ok := claudeBillingCCHDigitsOffset(body)
	if !ok {
		return body
	}
	const field = " cch="
	start, end := offset-len(field), offset+claudeCCHLength+1
	if start < 0 || end > len(body) || string(body[start:offset]) != field || body[end-1] != ';' {
		out := bytes.Clone(body)
		copy(out[offset:offset+claudeCCHLength], claudeCCHZero)
		return out
	}
	out := make([]byte, 0, len(body)-(end-start))
	out = append(out, body[:start]...)
	return append(out, body[end:]...)
}

// dpxAliasHTTPError is a non-2xx upstream answer on an aliased request. msg is
// the upstream error body with every known alias restored (RestoreErrorBody);
// it is empty only when the body could not be read, and the generic marker is
// returned instead. Before t_d27c2365 the body was always dropped, which hid
// the real upstream reason behind contentalias:upstream_failure.
type dpxAliasHTTPError struct {
	code int
	msg  string
}

func (e dpxAliasHTTPError) Error() string {
	if e.msg != "" {
		return e.msg
	}
	return "contentalias:upstream_failure"
}
func (e dpxAliasHTTPError) StatusCode() int       { return e.code }
func (e dpxAliasHTTPError) IsRequestScoped() bool { return true }

// dpxAliasUpstreamError reads (bounded) and decodes a non-2xx upstream body and
// returns it with the request's aliases restored. The response body is closed.
func dpxAliasUpstreamError(resp *http.Response, m *contentalias.RequestMap) dpxAliasHTTPError {
	defer resp.Body.Close()
	out := dpxAliasHTTPError{code: resp.StatusCode}
	body, err := decodeResponseBody(resp.Body, claudeResponseContentEncoding(resp.Header))
	if err != nil {
		return out
	}
	raw, err := io.ReadAll(io.LimitReader(body, 1<<20))
	if err != nil {
		return out
	}
	out.msg = string(m.RestoreErrorBody(raw))
	return out
}

// observeClaudeSSEUsage feeds a buffered Claude SSE body to usage: each line,
// plus each event's joined multi-line data: frame (upstream v8.0.53 shape), so
// the alias path counts a frame split across lines.
func observeClaudeSSEUsage(buf *helps.StreamUsageBuffer, data []byte) {
	var eventBuf bytes.Buffer
	for _, line := range bytes.Split(data, []byte("\n")) {
		eventBuf.Write(line)
		eventBuf.WriteByte('\n')
		if len(bytes.TrimSpace(line)) == 0 {
			buf.ObserveClaudeSSEEvent(eventBuf.Bytes())
			eventBuf.Reset()
		}
		buf.ObserveClaudeStream(line)
	}
	if eventBuf.Len() > 0 {
		buf.ObserveClaudeSSEEvent(eventBuf.Bytes())
	}
}
