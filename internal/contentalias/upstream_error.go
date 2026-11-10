package contentalias

import "net/http"

// UpstreamStreamError is an Anthropic SSE `error` event received mid-stream on
// an aliased response. It carries the upstream error JSON (aliases restored)
// and the HTTP status Anthropic uses for that error type, so the caller sees
// the real fault (529 overloaded, 429 rate limit, 5xx) rather than a 400.
// Before this type the rewriter returned Error("upstream_stream_error"), whose
// StatusCode is 400 and whose message relabelled the fault as
// invalid_request_error.
type UpstreamStreamError struct {
	body   []byte
	event  []byte // original SSE frame, retained for already-started streams
	kind   string
	status int
}

func newUpstreamStreamError(body []byte, kind string) *UpstreamStreamError {
	return &UpstreamStreamError{body: append([]byte(nil), body...), kind: kind, status: UpstreamErrorStatus(kind)}
}

// UpstreamStreamErrorFromEvent builds the error from an upstream SSE `error`
// event's data JSON (used by tests and callers outside the rewriter).
func UpstreamStreamErrorFromEvent(data []byte) *UpstreamStreamError {
	kind := ""
	if n, err := parse(data); err == nil {
		kind = n.get("error").get("type").str()
	}
	return newUpstreamStreamError(data, kind)
}

// UpstreamErrorStatus maps an Anthropic error type to its HTTP status.
// Unknown or missing types map to 502: the fault is upstream, never the request.
func UpstreamErrorStatus(kind string) int {
	switch kind {
	case "overloaded_error":
		return 529
	case "api_error":
		return http.StatusInternalServerError
	case "rate_limit_error":
		return http.StatusTooManyRequests
	case "authentication_error":
		return http.StatusUnauthorized
	case "permission_error":
		return http.StatusForbidden
	case "not_found_error":
		return http.StatusNotFound
	case "invalid_request_error":
		return http.StatusBadRequest
	case "request_too_large":
		return http.StatusRequestEntityTooLarge
	case "timeout_error":
		return http.StatusGatewayTimeout
	case "billing_error":
		return http.StatusPaymentRequired
	default:
		return http.StatusBadGateway
	}
}

// Error returns the upstream error event JSON verbatim (aliases restored), the
// same shape the Claude handler parses for the client-facing error type/message.
func (e *UpstreamStreamError) Error() string { return string(e.body) }

// ResponseBody exposes the upstream error JSON to handlers that prefer it.
func (e *UpstreamStreamError) ResponseBody() []byte { return append([]byte(nil), e.body...) }

// SSEErrorEvent returns the original upstream SSE frame, for a response whose
// headers have already been sent. The frame is not reserialized or relabelled.
func (e *UpstreamStreamError) SSEErrorEvent() []byte { return append([]byte(nil), e.event...) }

// StatusCode is the HTTP status Anthropic uses for this error type.
func (e *UpstreamStreamError) StatusCode() int { return e.status }

// Type is the upstream error type (e.g. "overloaded_error"); "" if absent.
func (e *UpstreamStreamError) Type() string { return e.kind }

// IsRequestScoped matches dpxAliasHTTPError (the non-2xx-at-headers path):
// the alias layer never rotates the unit's single credential on its own.
func (e *UpstreamStreamError) IsRequestScoped() bool { return true }
