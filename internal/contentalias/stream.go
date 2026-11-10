package contentalias

import (
	"bytes"
	"encoding/json"
	"errors"
	"strconv"
	"unicode/utf8"
)

const maxPending = 8 << 20
const maxTool = 4 << 20

type queued struct {
	raw     []byte
	ready   bool
	comment bool
}
type blockState struct {
	kind   string
	start  *queued
	deltas []*queued
	input  []byte
	text   textDecoder
	marker string // t_7bd8c9b9: undeclared alias-shaped tool name -> unknownToolMarker
	raw    bool   // t_3f4b6bd1: undeclared raw (non-alias) tool name, passed through verbatim
}
type Stream struct {
	m                      *RequestMap
	buffer                 []byte
	queue                  []*queued
	blocks                 map[int]*blockState
	seen                   map[int]bool
	started, ended, failed bool
	terminal               bool
	queuedBytes            int
	markers                int
}

func (m *RequestMap) NewStream() *Stream {
	return &Stream{m: m, blocks: map[int]*blockState{}, seen: map[int]bool{}}
}
func (s *Stream) fail(err error) ([]byte, error) {
	s.failed = true
	s.buffer = nil
	s.queue = nil
	s.blocks = nil
	s.seen = nil
	s.queuedBytes = 0
	return nil, err
}
func (s *Stream) enqueue(q *queued) {
	s.queue = append(s.queue, q)
	s.queuedBytes += len(q.raw) + 64
}
func (s *Stream) update(q *queued, raw []byte) {
	s.queuedBytes += len(raw) - len(q.raw)
	q.raw = raw
}
func (s *Stream) pending() int {
	// Charge overhead too; active-block scans are bounded by 64, queue work O(1).
	n := len(s.buffer) + s.queuedBytes + len(s.seen)*64
	for _, b := range s.blocks {
		n += len(b.input) + len(b.text.pending) + 256
	}
	return n
}
func (s *Stream) Feed(raw []byte) ([]byte, error) {
	if s.m == nil {
		return bytes.Clone(raw), nil
	}
	if s.failed {
		return nil, Error("stream_failed")
	}
	if s.pending()+len(raw) > maxPending {
		return s.fail(Error("stream_limit"))
	}
	s.buffer = append(s.buffer, raw...)
	var out []byte
	for {
		boundary := bytes.Index(s.buffer, []byte("\n\n"))
		width := 2
		crlf := bytes.Index(s.buffer, []byte("\r\n\r\n"))
		if crlf >= 0 && (boundary < 0 || crlf < boundary) {
			boundary = crlf
			width = 4
		}
		if boundary < 0 {
			break
		}
		event := bytes.Clone(s.buffer[:boundary+width])
		s.buffer = s.buffer[boundary+width:]
		if err := s.handle(event); err != nil {
			var upstream *UpstreamStreamError
			if errors.As(err, &upstream) {
				// Release what was already complete (e.g. message_start, finished
				// blocks) so a client that has seen headers gets the upstream error
				// in stream order; incomplete (unready) blocks are never released.
				for len(s.queue) > 0 && s.queue[0].ready {
					out = append(out, s.queue[0].raw...)
					s.queuedBytes -= len(s.queue[0].raw) + 64
					s.queue[0] = nil
					s.queue = s.queue[1:]
				}
				s.fail(err)
				return out, err
			}
			return s.fail(err)
		}
		// Check restored expansion BEFORE releasing any executable frame.
		if len(out)+s.pending() > maxPending {
			return s.fail(Error("stream_limit"))
		}
		for len(s.queue) > 0 && s.queue[0].ready {
			out = append(out, s.queue[0].raw...)
			s.queuedBytes -= len(s.queue[0].raw) + 64
			s.queue[0] = nil
			s.queue = s.queue[1:]
		}
	}
	return out, nil
}
func eventData(raw []byte) ([]byte, error) {
	if !utf8.Valid(raw) {
		return nil, Error("stream_utf8")
	}
	for i, b := range raw {
		if b == '\r' && (i+1 == len(raw) || raw[i+1] != '\n') {
			return nil, Error("stream_line_ending")
		}
	}
	var lines [][]byte
	for _, line := range bytes.Split(raw, []byte("\n")) {
		line = bytes.TrimSuffix(line, []byte("\r"))
		if bytes.HasPrefix(line, []byte("data:")) {
			v := line[5:]
			v = bytes.TrimPrefix(v, []byte(" "))
			lines = append(lines, v)
		}
	}
	if len(lines) == 0 {
		return nil, nil
	}
	return bytes.Join(lines, []byte("\n")), nil
}
func emitEvent(data []byte) []byte {
	n, err := parse(data)
	if err != nil {
		return nil
	}
	return append(append([]byte("event: "+n.get("type").str()+"\ndata: "), data...), []byte("\n\n")...)
}
func deltaEvent(index int, text string) []byte {
	raw, _ := json.Marshal(map[string]any{"type": "content_block_delta", "index": index, "delta": map[string]string{"type": "text_delta", "text": text}})
	return emitEvent(raw)
}
func (s *Stream) handle(raw []byte) error {
	data, err := eventData(raw)
	if err != nil {
		return err
	}
	if data == nil {
		if len(s.queue) > 0 {
			last := s.queue[len(s.queue)-1]
			if last.comment && len(last.raw)+len(raw) <= 65536 {
				last.raw = append(last.raw, raw...)
				s.queuedBytes += len(raw)
				return nil
			}
		}
		s.enqueue(&queued{raw: raw, ready: true, comment: true})
		return nil
	}
	if s.ended {
		return Error("stream_after_stop")
	}
	n, err := parse(data)
	if err != nil {
		return err
	}
	typ := n.get("type").str()
	if typ == "" {
		return Error("stream_event")
	}
	for _, line := range bytes.Split(raw, []byte("\n")) {
		line = bytes.TrimSuffix(line, []byte("\r"))
		if bytes.HasPrefix(line, []byte("event:")) && string(bytes.TrimSpace(line[6:])) != typ {
			return Error("stream_event_mismatch")
		}
	}
	if (typ == "content_block_start" || typ == "content_block_delta" || typ == "content_block_stop") && n.get("index") == nil {
		return Error("stream_index")
	}
	q := &queued{raw: raw, ready: true}
	index := 0
	if in := n.get("index"); in != nil {
		index, err = strconv.Atoi(string(data[in.start:in.end]))
		if err != nil || index < 0 {
			return Error("stream_index")
		}
	}
	switch typ {
	case "message_start":
		if content := n.get("message").get("content"); content != nil && (content.kind != '[' || len(content.items) != 0) {
			return Error("stream_initial_content")
		}
		if s.started {
			return Error("stream_order")
		}
		s.started = true
	case "content_block_start":
		if !s.started || s.terminal || s.seen[index] || len(s.blocks) >= 64 {
			return Error("stream_order")
		}
		s.seen[index] = true
		block := n.get("content_block")
		if block == nil {
			return Error("stream_block")
		}
		b := &blockState{kind: block.get("type").str(), start: q, text: textDecoder{m: s.m}}
		if b.kind == "text" && block.has("signature") {
			b.kind = "signed_text"
		}
		s.blocks[index] = b
		if b.kind == "tool_reference" || b.kind == "tool_result" || b.kind == "tool_search_tool_result" {
			edits := []edit{}
			if err := s.m.referenceEdits(block, nil, true, &edits); err != nil {
				return err
			}
			if len(edits) > 0 {
				// Only reference-bearing containers join the typed reference
				// protocol; other result blocks/deltas retain opaque handling.
				b.kind = "tool_reference"
				changed, err := apply(data, edits)
				if err != nil {
					return err
				}
				q.raw = emitEvent(changed)
			}
		}
		if b.kind == "tool_use" {
			name := block.get("name").str()
			if original, ok := s.m.reverse[name]; !ok || !s.m.allowed[original] {
				if !aliasShapedToolName(name) {
					b.raw = true
				} else {
					s.markers++
					b.marker = unknownToolMarker(s.markers)
				}
			}
			if block.has("signature") {
				return Error("signed_tool_block")
			}
			input := block.get("input")
			if input == nil || input.kind != '{' || len(input.fields) != 0 {
				return Error("stream_initial_input")
			}
			q.ready = b.raw
		}
		if b.kind == "text" {
			initial := block.get("text").str()
			decoded, err := b.text.feed(initial)
			if err != nil {
				return err
			}
			if initial != decoded {
				edits := []edit{replace(block.get("text"), decoded)}
				changed, err := apply(data, edits)
				if err != nil {
					return err
				}
				q.raw = emitEvent(changed)
			}
		}
	case "content_block_delta":
		b := s.blocks[index]
		if b == nil {
			return Error("stream_order")
		}
		delta := n.get("delta")
		if delta == nil {
			return Error("stream_delta")
		}
		switch b.kind {
		case "tool_reference":
			// These native blocks carry complete content at start, not JSON deltas.
			return Error("stream_delta")
		case "tool_use":
			if delta.get("type").str() != "input_json_delta" {
				return Error("stream_delta")
			}
			part := delta.get("partial_json")
			if part == nil || part.kind != '"' {
				return Error("stream_json")
			}
			if b.raw {
				break
			}
			if len(b.input)+len(part.text) > maxTool {
				return Error("tool_limit")
			}
			b.input = append(b.input, part.text...)
			if len(b.deltas) > 0 {
				return nil
			}
			b.deltas = append(b.deltas, q)
			q.raw = nil
		case "text":
			if delta.get("type").str() != "text_delta" {
				return Error("stream_delta")
			}
			if text := delta.get("text"); text == nil || text.kind != '"' {
				return Error("stream_delta")
			}
			decoded, err := b.text.feed(delta.get("text").str())
			if err != nil {
				return err
			}

			changed, err := apply(data, []edit{replace(delta.get("text"), decoded)})
			if err != nil {
				return err
			}
			q.raw = emitEvent(changed)
		}
	case "content_block_stop":
		b := s.blocks[index]
		if b == nil {
			return Error("stream_order")
		}
		if b.kind == "tool_use" && !b.raw {
			// A zero-argument call streams no JSON (one empty partial_json, or no delta at all):
			// its input is the {} the start event already carried.
			if len(bytes.TrimSpace(b.input)) == 0 {
				b.input = []byte("{}")
			}
			args, err := parse(b.input)
			if err != nil || args.kind != '{' {
				return Error("stream_json")
			}
			startData, _ := eventData(b.start.raw)
			start, _ := parse(startData)
			name := start.get("content_block").get("name")
			original := s.m.reverse[name.str()]
			restored := []byte("{}")
			if b.marker != "" {
				original = b.marker
			} else {
				edits := []edit{}
				if err := s.m.schemas[original].arguments(args, true, &edits); err != nil {
					return err
				}
				if err := valueEdits(args, s.m.decodeValue, &edits); err != nil {
					return err
				}
				if restored, err = apply(b.input, edits); err != nil {
					return err
				}
			}
			changed, err := apply(startData, []edit{replace(name, original)})
			if err != nil {
				return err
			}
			s.update(b.start, emitEvent(changed))
			b.start.ready = true
			delta, _ := json.Marshal(map[string]any{"type": "content_block_delta", "index": index, "delta": map[string]string{"type": "input_json_delta", "partial_json": string(restored)}})
			if len(b.deltas) == 0 {
				s.enqueue(&queued{raw: emitEvent(delta), ready: true})
			} else {
				s.update(b.deltas[0], emitEvent(delta))
			}
		}
		if b.kind == "text" {
			decoded, err := b.text.finish()
			if err != nil {
				return err
			}
			if decoded != "" {
				s.enqueue(&queued{raw: deltaEvent(index, decoded), ready: true})
			}
		}
		delete(s.blocks, index)
	case "message_delta":
		if !s.started || len(s.blocks) > 0 {
			return Error("stream_order")
		}
		if stop := n.get("delta").get("stop_reason"); stop != nil && stop.kind != 'n' {
			s.terminal = true
		}
	case "message_stop":
		if !s.started || len(s.blocks) > 0 {
			return Error("stream_order")
		}
		s.ended = true
	case "error":
		// An upstream SSE error event (overloaded_error, api_error,
		// rate_limit_error, ...) is an upstream fault, not a shape violation:
		// surface it with its own type, message and status instead of a
		// contentalias 400 (which the harness reads as "lane cannot serve
		// this request shape"). Aliases in the message are restored.
		upstream := newUpstreamStreamError(s.m.RestoreErrorBody(data), n.get("error").get("type").str())
		upstream.event = bytes.Clone(raw)
		return upstream
	case "ping":
	default:
		return Error("stream_event")
	}
	s.enqueue(q)
	return nil
}
func (s *Stream) Finish() ([]byte, error) {
	if s.m == nil {
		return nil, nil
	}
	if s.failed {
		return nil, Error("stream_failed")
	}
	if !s.ended || len(s.blocks) > 0 || len(bytes.TrimSpace(s.buffer)) > 0 || len(s.queue) > 0 {
		return s.fail(Error("stream_eof"))
	}
	return nil, nil
}
