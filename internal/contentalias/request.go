package contentalias

import (
	"bytes"
	"encoding/json"
	"strings"
)

type RequestMap struct {
	st      state
	schemas map[string]*schema
	allowed map[string]bool
	reverse map[string]string
}

func Prepare(raw []byte, s *Session) ([]byte, *RequestMap, error) {
	if s == nil {
		return bytes.Clone(raw), nil, nil
	}
	n, err := parse(raw)
	if err != nil {
		return nil, nil, err
	}
	if n.kind != '{' {
		return nil, nil, Error("request")
	}
	// Optimistic: decode a snapshot of map.json WITHOUT the store lock (save
	// replaces it by atomic rename, so a read sees one whole map), do all the
	// per-request work on that private copy, and take the flock only to commit
	// a map that changed (t_1f4b978a). A request that adds nothing returns with
	// no lock at all: the map only grows, so every alias it used stays valid
	// and it is ordered at its snapshot. A commit whose snapshot went stale
	// (another request committed first) redoes the work on the new map.
	for attempt := 0; attempt < optimisticAttempts; attempt++ {
		if attempt > 0 {
			if n, err = parse(raw); err != nil {
				return nil, nil, err
			}
		}
		file, err := s.readMap()
		if err != nil {
			return nil, nil, err
		}
		st, payload, err := s.decodeMap(file)
		if err != nil {
			return nil, nil, err
		}
		wire, m, st, err := prepareOn(raw, n, st)
		if err != nil {
			return nil, nil, err
		}
		after, err := json.Marshal(st)
		if err != nil {
			return nil, nil, Error("store_write")
		}
		if bytes.Equal(after, payload) {
			m.st = st
			return wire, m, nil
		}
		committed, err := s.commitIfUnchanged(file, st)
		if err != nil {
			return nil, nil, err
		}
		if committed {
			m.st = st
			return wire, m, nil
		}
	}
	// Contended past optimisticAttempts: do it under the lock (always progresses).
	if n, err = parse(raw); err != nil {
		return nil, nil, err
	}
	lock, err := s.lock()
	if err != nil {
		return nil, nil, err
	}
	defer unlock(lock)
	st, err := s.load()
	if err != nil {
		return nil, nil, err
	}
	wire, m, st, err := prepareOn(raw, n, st)
	if err != nil {
		return nil, nil, err
	}
	if err := s.save(st); err != nil {
		return nil, nil, err
	}
	m.st = st
	return wire, m, nil
}

// optimisticAttempts bounds the lock-free tries before Prepare serializes.
const optimisticAttempts = 3

// prepareOn aliases raw (parsed as n) against st and returns the wire body, the
// request map and st with every allocation this request made. Pure: no store I/O.
func prepareOn(raw []byte, n *node, st state) ([]byte, *RequestMap, state, error) {
	m := &RequestMap{st: st, schemas: map[string]*schema{}, allowed: map[string]bool{}, reverse: map[string]string{}}
	edits := []edit{}
	if tools := n.get("tools"); tools != nil {
		if tools.kind != '[' {
			return nil, nil, st, Error("tools")
		}
		seen := map[string]bool{}
		for _, tool := range tools.items {
			name := tool.get("name").str()
			if name == "" || seen[name] {
				return nil, nil, st, Error("tool_duplicate")
			}
			seen[name] = true
		}
		// Reserve ALL declared names before allocation, including later opaque
		// provider tools; their wire names must never collide with custom aliases.
		for _, tool := range tools.items {
			name := tool.get("name").str()
			if tool.has("type") && tool.get("type").str() != "custom" {
				continue
			}
			if strings.HasPrefix(name, "dpx_v1_") {
				return nil, nil, st, Error("reserved_tool")
			}
			schemaNode := tool.get("input_schema")
			if schemaNode == nil {
				return nil, nil, st, Error("schema")
			}
			// A declared schema always re-pins the stored one (t_dbf31d31). The map
			// outlives client builds, so refusing a changed schema (the old
			// schema_changed) failed every tool-bearing request after any tool
			// edit until the map was wiped. Aliases are pure functions of
			// (binding, tool, path, key), so unchanged names keep their aliases,
			// and this request's arguments compile from the schema it declares.
			schemaRaw := bytes.Clone(raw[schemaNode.start:schemaNode.end])
			alias, err := st.allocate("t", name)
			if err != nil {
				return nil, nil, st, err
			}
			for other := range seen {
				if alias == other {
					return nil, nil, st, Error("tool_collision")
				}
			}
			st.Tools[name] = storedTool{alias, schemaRaw}
			m.allowed[name] = true
			m.reverse[alias] = name
			sc, err := compileSchema(schemaNode, &st, name, &edits)
			if err != nil {
				return nil, nil, st, err
			}
			m.schemas[name] = sc
			edits = append(edits, replace(tool.get("name"), alias))
			if err := textEdit(tool.get("description"), st.encodeText, &edits); err != nil {
				return nil, nil, st, err
			}
		}
	}
	for name, tool := range st.Tools {
		if m.schemas[name] != nil {
			continue
		}
		root, err := parse(tool.Schema)
		if err != nil {
			return nil, nil, st, Error("store_schema")
		}
		discard := []edit{}
		sc, err := compileSchema(root, &st, name, &discard)
		if err != nil {
			return nil, nil, st, err
		}
		m.schemas[name] = sc
	}
	if choice := n.get("tool_choice"); choice != nil && choice.get("type").str() == "tool" {
		name := choice.get("name")
		tool, ok := st.Tools[name.str()]
		if !ok || !m.allowed[name.str()] {
			return nil, nil, st, Error("tool_choice")
		}
		edits = append(edits, replace(name, tool.Alias))
	}
	encodeContent := func(content *node) error { return m.forwardContent(content, &st, &edits) }
	if err := encodeContent(n.get("system")); err != nil {
		return nil, nil, st, err
	}
	if messages := n.get("messages"); messages != nil {
		if messages.kind != '[' {
			return nil, nil, st, Error("messages")
		}
		for _, message := range messages.items {
			if err := encodeContent(message.get("content")); err != nil {
				return nil, nil, st, err
			}
		}
	}
	wire, err := apply(raw, edits)
	if err != nil {
		return nil, nil, st, err
	}
	// Source duplicate keys and schema/argument collisions were checked above;
	// edits replace only typed JSON strings. Validate the emitted grammar without
	// allocating a second multi-megabyte syntax tree that is immediately discarded.
	if !json.Valid(wire) {
		return nil, nil, st, Error("json")
	}
	return wire, m, st, nil
}
func (m *RequestMap) forwardContent(n *node, st *state, edits *[]edit) error {
	if n == nil {
		return nil
	}
	if n.kind == '"' {
		return textEdit(n, st.encodeText, edits)
	}
	if n.kind != '[' {
		return Error("content")
	}
	for _, block := range n.items {
		switch block.get("type").str() {
		case "text":
			if block.has("signature") {
				continue
			}
			if err := textEdit(block.get("text"), st.encodeText, edits); err != nil {
				return err
			}
		case "tool_reference", "tool_result", "tool_search_tool_result":
			if err := m.referenceEdits(block, st, false, edits); err != nil {
				return err
			}
			if err := resultTextEdits(block, st, edits); err != nil {
				return err
			}
		case "tool_use":
			name := block.get("name")
			if block.has("signature") {
				return Error("signed_tool_block")
			}
			alias, err := historyToolAlias(st, name.str())
			if err != nil {
				return err
			}
			*edits = append(*edits, replace(name, alias))
			if block.get("type").str() == "tool_use" {
				input := block.get("input")
				if input == nil || input.kind != '{' {
					return Error("tool_input")
				}
				if err := m.schemas[name.str()].arguments(input, false, edits); err != nil {
					return err
				}
				if err := valueEdits(input, st.encodeText, edits); err != nil {
					return err
				}
			}
			// Signed/unknown content is intentionally opaque.
		}
	}
	return nil
}
func (m *RequestMap) RestoreJSON(raw []byte) ([]byte, error) {
	if m == nil {
		return bytes.Clone(raw), nil
	}
	n, err := parse(raw)
	if err != nil {
		return nil, err
	}
	if n.kind != '{' {
		return nil, Error("response")
	}
	edits := []edit{}
	markers := 0
	content := n.get("content")
	if content != nil {
		if content.kind != '[' {
			return nil, Error("content")
		}
		for _, block := range content.items {
			if err := m.restoreBlock(block, &edits, &markers); err != nil {
				return nil, err
			}
		}
	}
	return apply(raw, edits)
}
func (m *RequestMap) restoreBlock(block *node, edits *[]edit, markers *int) error {
	switch block.get("type").str() {
	case "text":
		if block.has("signature") {
			return nil
		}
		return textEdit(block.get("text"), m.decodeText, edits)
	case "tool_reference", "tool_result", "tool_search_tool_result":
		return m.referenceEdits(block, nil, true, edits)
	case "tool_use":
		name := block.get("name")
		original, ok := m.reverse[name.str()]
		if block.has("signature") {
			return Error("signed_tool_block")
		}
		if !ok || !m.allowed[original] {
			if !aliasShapedToolName(name.str()) {
				return nil // t_3f4b6bd1: raw name passes through verbatim
			}
			*markers++
			*edits = append(*edits, replace(name, unknownToolMarker(*markers)))
			if input := block.get("input"); input != nil {
				*edits = append(*edits, edit{input.start, input.end, []byte("{}")})
			}
			return nil
		}
		*edits = append(*edits, replace(name, original))
		if block.get("type").str() == "tool_use" {
			input := block.get("input")
			if input == nil || input.kind != '{' {
				return Error("tool_input")
			}
			if err := m.schemas[original].arguments(input, true, edits); err != nil {
				return err
			}
			return valueEdits(input, m.decodeValue, edits)
		}
	}
	return nil
}

// resultTextEdits aliases the text a tool_result hands the model (t_cb095320):
// a string content, or the text of each immediate unsigned text item. A
// signed result and every non-text item (image, document, unknown) stay
// opaque. The model reads the aliases; whatever it copies into a tool_use
// value is restored by valueEdits before the caller executes it.
func resultTextEdits(block *node, st *state, edits *[]edit) error {
	if block.get("type").str() != "tool_result" || block.has("signature") {
		return nil
	}
	content := block.get("content")
	if content == nil {
		return nil
	}
	if content.kind == '"' {
		return textEdit(content, st.encodeText, edits)
	}
	if content.kind != '[' {
		return nil
	}
	for _, item := range content.items {
		if item.get("type").str() == "text" && !item.has("signature") {
			if err := textEdit(item.get("text"), st.encodeText, edits); err != nil {
				return err
			}
		}
	}
	return nil
}

// valueEdits maps every string VALUE of a tool_use input (keys are the
// schema's job): encode on the way out, decode on the way back, so a path or
// identifier the model saw aliased executes as the caller's original bytes.
func valueEdits(n *node, f func(string) (string, error), edits *[]edit) error {
	if n == nil {
		return nil
	}
	switch n.kind {
	case '"':
		return textEdit(n, f, edits)
	case '{':
		for _, field := range n.fields {
			if err := valueEdits(field.value, f, edits); err != nil {
				return err
			}
		}
	case '[':
		for _, item := range n.items {
			if err := valueEdits(item, f, edits); err != nil {
				return err
			}
		}
	}
	return nil
}

// historyToolAlias maps a tool name found in conversation history. A name in
// the store keeps its established alias. A name the store never saw (a
// hallucinated call the caller answered "tool does not exist", or a tool from a
// toolset the session has since dropped) gets the same deterministic symbol a
// declared tool would, but it is NOT registered as a callable tool: no schema,
// no RequestMap.allowed/reverse entry, so a response that names it is still
// refused. Before t_ca4ca1e2 this was a hard 400 history_tool, which no retry
// or seat rotation can clear: one hallucinated name poisoned the session.
func historyToolAlias(st *state, name string) (string, error) {
	if tool, ok := st.Tools[name]; ok {
		return tool.Alias, nil
	}
	if name == "" {
		return "", Error("history_tool")
	}
	if strings.HasPrefix(name, "dpx_v1_") {
		// A wire alias echoed back as a plain name stays a refusal.
		return "", Error("history_tool")
	}
	return st.allocate("t", name)
}
