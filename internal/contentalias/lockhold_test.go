package contentalias

// Performance gates for the t_129cf1ac class: slow work under the unit's
// exclusive store lock. Prepare holds the lock from load to save, so every
// request on a DPX unit queues behind the slowest Prepare in flight. One
// quadratic step (encodeText on a ~444 KB prompt, seconds per call) serialized
// whole units. TestEncodeTextIsLinearInTextSize covers that one function;
// these gates cover the class:
//
//   - TestPrepareLockHoldBudget: the commit's lock-hold budget on 50/200/450 KB
//     bodies, and warm Prepares take no hold.
//   - TestPreparePerformanceUnderParallelCallers: N=16 concurrent requests on
//     one unit state; queueing must stay proportional to the hold.
//   - TestPrepareScalesLinearly: 1x/4x/16x bodies on three axes (text, tools,
//     history) through Prepare, and through both decode paths (RestoreJSON,
//     the SSE Stream).
//
// Inputs are synthetic, generated in-test (synthetic_fixture_test.go).

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	// commitHoldBudget bounds one commit's store-lock hold: re-read map.json,
	// compare, save (marshal + fsync). No per-request work runs under the lock
	// (t_1f4b978a); fsync measured 1.6-15 ms on the loaded ci-box.
	commitHoldBudget = 40 * time.Millisecond
	// concurrentWorkers is how many requests race on one unit state.
	concurrentWorkers = 16
	// blockedWaitBudget bounds the time one solo Prepare spends blocked (sleep,
	// I/O, a blocked syscall): wall time minus the thread's CPU time minus its
	// runqueue wait, from /proc/thread-self/schedstat. Runqueue wait is what host
	// load adds, so this is load-immune; the best of the solo runs is taken.
	// Measured best-of-7 ~1 ms idle (ACE-AI), 1.6-15 ms on the ci-box at load
	// 200/256 (fsync of map.json). A 50 ms wait under the lock adds at least
	// 50 ms to every run.
	blockedWaitBudget = 40 * time.Millisecond
	// prepareCPURatio bounds the CPU one solo Prepare costs (all of it under the
	// store-free snapshot work) against a same-run reference: 8 json.Valid passes over the same
	// body, both the best thread-CPU time of 7 runs. A loaded or slower host
	// scales both alike. Measured 1.7-2.5 on ACE-AI and the loaded ci-box; the
	// pre-#24 unanchored encode scan and a linear symbol-collision scan measure
	// 11-21, a slow step outside the flock ~7.
	prepareCPURatio = 5.0
)

func fixture450(t testing.TB) []byte { return syntheticBody(t, fixtureSizes[len(fixtureSizes)-1]) }

// recordLockHolds installs lockHeldHook for the test and returns a reader of
// the holds observed so far.
func recordLockHolds(t *testing.T) func() []time.Duration {
	t.Helper()
	var mu sync.Mutex
	var holds []time.Duration
	lockHeldHook = func(d time.Duration) {
		mu.Lock()
		holds = append(holds, d)
		mu.Unlock()
	}
	t.Cleanup(func() { lockHeldHook = nil })
	return func() []time.Duration {
		mu.Lock()
		defer mu.Unlock()
		return append([]time.Duration(nil), holds...)
	}
}

func median(ds []time.Duration) time.Duration {
	s := append([]time.Duration(nil), ds...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	return s[len(s)/2]
}

func percentile(ds []time.Duration, p float64) time.Duration {
	s := append([]time.Duration(nil), ds...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	i := int(float64(len(s))*p+0.999999) - 1
	if i < 0 {
		i = 0
	}
	return s[i]
}

// The first Prepare allocates every tool alias and commits the map; later ones
// read the populated map, change nothing and take no lock. The one commit must
// stay inside commitHoldBudget at every size.
func TestPrepareLockHoldBudget(t *testing.T) {
	for _, size := range fixtureSizes {
		raw := syntheticBody(t, size)
		s := testSession(t)
		holds := recordLockHolds(t)
		const runs = 5
		for i := 0; i < runs; i++ {
			if _, _, err := Prepare(raw, s); err != nil {
				t.Fatal(err)
			}
		}
		got := holds()
		// The cold Prepare allocates and commits (one hold); warm Prepares add
		// nothing to the map and take no lock at all (t_1f4b978a).
		if len(got) != 1 {
			t.Fatalf("lock hook saw %d holds for %d Prepare calls on one body, want 1 (the cold commit): "+
				"a warm Prepare that changes nothing must not take the store lock (t_1f4b978a)", len(got), runs)
		}
		t.Logf("store-lock holds on %d B body: %v", len(raw), got)
		if first := got[0]; first >= commitHoldBudget {
			t.Fatalf("the cold Prepare's commit held the store lock %v on a %d B body (budget %v): the commit is "+
				"re-read + save only, per-request work belongs outside the lock (t_129cf1ac, t_1f4b978a)", first, len(raw), commitHoldBudget)
		}
	}
}

// threadSched reads the calling OS thread's on-CPU time and runqueue wait
// (/proc/thread-self/schedstat, Linux). ok is false where it is unavailable.
func threadSched() (cpu, runqueue time.Duration, ok bool) {
	b, err := os.ReadFile("/proc/thread-self/schedstat")
	if err != nil {
		return 0, 0, false
	}
	var c, r, slices int64
	if n, _ := fmt.Sscan(string(b), &c, &r, &slices); n < 2 {
		return 0, 0, false
	}
	return time.Duration(c), time.Duration(r), true
}

// Prepare does its work on a lock-free snapshot and takes the store lock only
// to commit a changed map (t_1f4b978a), so warm callers take no hold at all.
// Every bound is relative to this run's own measurements (a shared CI box
// stretches all of them alike; a fixed 800 ms wall-clock cap failed there on
// correct code, t_dae16eeb):
//
//   - no hold: N=16 warm callers and the solo runs record zero store-lock holds.
//     The parallel queue itself is gated by TestPrepareP90FlatAt30Goroutines.
//   - CPU: the best solo Prepare costs <= prepareCPURatio x the CPU of a
//     fixed reference workload on the same body. Fails superlinear or wasted
//     work under the lock, which inflates the hold and so hides from the queue
//     bound.
//   - blocked: the best solo Prepare spends < blockedWaitBudget blocked (wall
//     minus CPU minus runqueue wait). Fails a wall-clock wait under the lock
//     (sleep, I/O), which the queue bound cannot see because it is inside the
//     hold. TestPrepareLockHoldBudget bounds the hold itself.
func TestPreparePerformanceUnderParallelCallers(t *testing.T) {
	raw := fixture450(t)
	s := testSession(t)
	if _, _, err := Prepare(raw, s); err != nil { // warm the map
		t.Fatal(err)
	}
	holds := recordLockHolds(t)
	var solo []time.Duration
	blocked, prepCPU, haveSched := time.Duration(-1), time.Duration(-1), true
	runtime.LockOSThread() // keep the schedstat reading on the thread that ran Prepare
	for i := 0; i < 7; i++ {
		c0, r0, ok0 := threadSched()
		t0 := time.Now()
		if _, _, err := Prepare(raw, s); err != nil {
			runtime.UnlockOSThread()
			t.Fatal(err)
		}
		wall := time.Since(t0)
		c1, r1, ok1 := threadSched()
		solo = append(solo, wall)
		if !ok0 || !ok1 {
			haveSched = false
			continue
		}
		if w := wall - (c1 - c0) - (r1 - r0); blocked < 0 || w < blocked {
			blocked = w
		}
		if c := c1 - c0; prepCPU < 0 || c < prepCPU {
			prepCPU = c
		}
	}
	refCPU := time.Duration(-1)
	for i := 0; haveSched && i < 7; i++ {
		c0, _, _ := threadSched()
		for j := 0; j < 8; j++ {
			_ = json.Valid(raw)
		}
		c1, _, _ := threadSched()
		if c := c1 - c0; refCPU < 0 || c < refCPU {
			refCPU = c
		}
	}
	runtime.UnlockOSThread()
	soloHolds := holds()
	l1 := median(solo)

	lat := make([]time.Duration, concurrentWorkers)
	errs := make(chan error, concurrentWorkers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < concurrentWorkers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			t0 := time.Now()
			_, _, err := Prepare(raw, s)
			lat[i] = time.Since(t0)
			errs <- err
		}(i)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	p95 := percentile(lat, 0.95)
	parHolds := holds()[len(soloHolds):]
	t.Logf("N=1 latency %v, %d solo holds, best blocked %v, CPU %v (%.1fx reference %v); N=%d p95 %v (%.1fx N=1), %d holds",
		l1, len(soloHolds), blocked, prepCPU, float64(prepCPU)/float64(max(refCPU, 1)), refCPU, concurrentWorkers, p95, float64(p95)/float64(l1), len(parHolds))
	// Warm callers change nothing, so none of them may take the store lock; the
	// queue they could build is bounded by TestPrepareP90FlatAt30Goroutines.
	if len(soloHolds)+len(parHolds) != 0 {
		t.Fatalf("warm Prepares took %d store-lock holds (want 0): a request that changes nothing serializes the unit (t_1f4b978a)",
			len(soloHolds)+len(parHolds))
	}
	if !haveSched {
		t.Logf("no /proc/thread-self/schedstat on %s: CPU and blocked-wait checks skipped", runtime.GOOS)
		return
	}
	if refCPU > 0 && float64(prepCPU) > prepareCPURatio*float64(refCPU) {
		t.Fatalf("best of %d solo Prepares cost %v CPU, %.1fx the %v reference (8 json.Valid passes over the same body; "+
			"bound %.0fx): slow work under the store lock serializes the unit (t_129cf1ac)",
			len(solo), prepCPU, float64(prepCPU)/float64(refCPU), refCPU, prepareCPURatio)
	}
	if blocked >= blockedWaitBudget {
		t.Fatalf("best of %d solo Prepares spent %v blocked off the CPU (budget %v): a wall-clock wait "+
			"(sleep, I/O) is on the request path, and under the store lock it serializes the unit (t_129cf1ac)",
			len(solo), blocked, blockedWaitBudget)
	}
}

// scaledBody builds a body from a slice of the synthetic fixture (64 KB of each system
// text with a manifest word seeded every 400 bytes, the first 4 tools plus the
// ones the history calls, the first 12 messages) and grows ONE axis k-fold:
//
//   - axisText:    every system text repeats k times (per-string work: encode,
//     and the decode of the response built from it);
//   - axisTools:   the full tool catalog is cloned k times under unique names
//     (schema compile, symbol allocation, map load/save);
//   - axisHistory: the message slice repeats k times (block walk, edits, apply).
//
// Axes scale separately so a quadratic step in a small component is not hidden
// behind the linear cost of a large one.
type axis int

const (
	axisText axis = iota
	axisTools
	axisHistory
)

func (a axis) String() string { return [...]string{"text", "tools", "history"}[a] }

func scaledBody(t testing.TB, raw []byte, a axis, k int) []byte {
	t.Helper()
	var body map[string]json.RawMessage
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatal(err)
	}
	var system []map[string]any
	var tools []map[string]any
	var messages []json.RawMessage
	if err := json.Unmarshal(body["system"], &system); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(body["tools"], &tools); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(body["messages"], &messages); err != nil {
		t.Fatal(err)
	}
	word := DefaultManifest().Words[0]
	seed := func(text string, n int) string {
		if len(text) > n {
			cut := n
			for cut > 0 && !utf8RuneStart(text[cut]) {
				cut--
			}
			text = text[:cut]
		}
		var b strings.Builder
		for i := 0; i < len(text); {
			j := i + 400
			if j > len(text) {
				j = len(text)
			}
			for j < len(text) && !utf8RuneStart(text[j]) {
				j++
			}
			b.WriteString(text[i:j])
			b.WriteString(" " + word + "-x ")
			i = j
		}
		return b.String()
	}
	textK, toolsK, historyK := 1, 1, 1
	switch a {
	case axisText:
		textK = k
	case axisTools:
		toolsK = k
	case axisHistory:
		historyK = k
	}
	for _, block := range system {
		if text, ok := block["text"].(string); ok {
			block["text"] = strings.Repeat(seed(text, 64<<10), textK)
		}
	}
	history := messages[:12]
	used := map[string]bool{}
	for _, m := range history {
		var msg struct{ Content json.RawMessage }
		_ = json.Unmarshal(m, &msg)
		var blocks []map[string]any
		if json.Unmarshal(msg.Content, &blocks) == nil {
			for _, b := range blocks {
				if b["type"] == "tool_use" {
					used[b["name"].(string)] = true
				}
			}
		}
	}
	unitTools := tools
	if a != axisTools {
		unitTools = append([]map[string]any(nil), tools[:4]...)
		for _, tool := range tools[4:] {
			if used[tool["name"].(string)] {
				unitTools = append(unitTools, tool)
			}
		}
	}
	var outTools []map[string]any
	for i := 0; i < toolsK; i++ {
		for _, tool := range unitTools {
			c := map[string]any{}
			for key, v := range tool {
				c[key] = v
			}
			if i > 0 {
				c["name"] = fmt.Sprintf("%s_k%d", tool["name"], i)
			}
			outTools = append(outTools, c)
		}
	}
	var outMessages []json.RawMessage
	for i := 0; i < historyK; i++ {
		outMessages = append(outMessages, history...)
	}
	sys, _ := json.Marshal(system)
	tl, _ := json.Marshal(outTools)
	ms, _ := json.Marshal(outMessages)
	body["system"], body["tools"], body["messages"] = sys, tl, ms
	out, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func utf8RuneStart(b byte) bool { return b&0xC0 != 0x80 }

// responseFor turns the request's encoded system text (aliases included) into
// the two response shapes the decode path restores: a JSON message, and SSE
// events of 32-byte text deltas (upstream streams a few tokens per delta, and
// the executor feeds the stream one network read at a time).
func responseFor(t testing.TB, wire []byte) (jsonBody []byte, events [][]byte) {
	t.Helper()
	var req struct {
		System []struct{ Text string }
	}
	if err := json.Unmarshal(wire, &req); err != nil {
		t.Fatal(err)
	}
	var text strings.Builder
	for _, b := range req.System {
		text.WriteString(b.Text)
	}
	jsonBody, _ = json.Marshal(map[string]any{
		"type": "message", "role": "assistant",
		"content": []any{map[string]any{"type": "text", "text": text.String()}},
	})
	events = append(events,
		event(map[string]any{"type": "message_start", "message": map[string]any{"content": []any{}, "usage": map[string]int{"input_tokens": 7}}}),
		event(map[string]any{"type": "content_block_start", "index": 0, "content_block": map[string]any{"type": "text", "text": ""}}))
	s := text.String()
	for i := 0; i < len(s); {
		j := i + 32
		if j > len(s) {
			j = len(s)
		}
		for j < len(s) && !utf8RuneStart(s[j]) {
			j++
		}
		events = append(events, event(map[string]any{"type": "content_block_delta", "index": 0, "delta": map[string]string{"type": "text_delta", "text": s[i:j]}}))
		i = j
	}
	events = append(events,
		event(map[string]any{"type": "content_block_stop", "index": 0}),
		event(map[string]any{"type": "message_delta", "delta": map[string]string{"stop_reason": "end_turn"}}),
		event(map[string]any{"type": "message_stop"}))
	return jsonBody, events
}

func restoreStream(m *RequestMap, events [][]byte) ([]byte, error) {
	s := m.NewStream()
	var out []byte
	for _, e := range events {
		b, err := s.Feed(e)
		if err != nil {
			return nil, err
		}
		out = append(out, b...)
	}
	b, err := s.Finish()
	return append(out, b...), err
}

// Linear code grows ~16x from 1x to 16x, a quadratic step ~256x. The 64x bound
// (as in TestEncodeTextIsLinearInTextSize) absorbs timer noise and still fails
// a quadratic step by 4x. Each body gets a fresh unit so the map grows with it,
// as it does in production; each cost is the best of 3 runs.
func TestPrepareScalesLinearly(t *testing.T) {
	raw := fixture450(t)
	word := []byte(DefaultManifest().Words[0])
	best := func(d *time.Duration, v time.Duration, first bool) {
		if first || v < *d {
			*d = v
		}
	}
	type cost struct {
		bytes                            int
		prepare, restoreJSON, restoreSSE time.Duration
	}
	measure := func(a axis, k int) cost {
		body := scaledBody(t, raw, a, k)
		s := testSession(t)
		c := cost{bytes: len(body)}
		for i := 0; i < 3; i++ {
			if i > 0 && c.prepare+c.restoreJSON+c.restoreSSE > 2*time.Second {
				break // already far past any bound; spare the repeats
			}
			t0 := time.Now()
			wire, m, err := Prepare(body, s)
			best(&c.prepare, time.Since(t0), i == 0)
			if err != nil {
				t.Fatalf("%v k=%d: %v", a, k, err)
			}
			if a != axisText {
				continue
			}
			js, events := responseFor(t, wire)
			t1 := time.Now()
			outJSON, err := m.RestoreJSON(js)
			best(&c.restoreJSON, time.Since(t1), i == 0)
			if err != nil {
				t.Fatalf("k=%d RestoreJSON: %v", k, err)
			}
			t2 := time.Now()
			outSSE, err := restoreStream(m, events)
			best(&c.restoreSSE, time.Since(t2), i == 0)
			if err != nil {
				t.Fatalf("k=%d stream restore: %v", k, err)
			}
			if !bytes.Contains(outJSON, word) || !bytes.Contains(outSSE, word) {
				t.Fatalf("k=%d: decode did not restore the manifest word", k)
			}
		}
		return c
	}
	check := func(name string, c1, c4, c16 cost, d func(cost) time.Duration) {
		base := d(c1)
		if base <= 0 {
			base = time.Microsecond
		}
		ratio := float64(d(c16)) / float64(base)
		t.Logf("%-20s 1x %v  4x %v  16x %v  (16x/1x = %.1f; bytes %d/%d/%d)",
			name, d(c1), d(c4), d(c16), ratio, c1.bytes, c4.bytes, c16.bytes)
		if ratio > 64 {
			t.Errorf("%s scaled %.0fx for a 16x larger input (1x %v, 16x %v): a superlinear step is back on the path",
				name, ratio, d(c1), d(c16))
		}
	}
	for _, a := range []axis{axisText, axisTools, axisHistory} {
		c1, c4, c16 := measure(a, 1), measure(a, 4), measure(a, 16)
		check("Prepare/"+a.String(), c1, c4, c16, func(c cost) time.Duration { return c.prepare })
		if a == axisText {
			check("RestoreJSON/text", c1, c4, c16, func(c cost) time.Duration { return c.restoreJSON })
			check("RestoreStream/text", c1, c4, c16, func(c cost) time.Duration { return c.restoreSSE })
		}
	}
}
