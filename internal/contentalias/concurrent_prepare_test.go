package contentalias

// ALR local P6 (t_1f4b978a): the per-request work in Prepare (map decode, schema
// compile, text encode, edit apply, json.Valid) runs OUTSIDE the unit's store
// lock. Only a request that changes the map takes the flock, and only to commit.
// Live dpx-25 before the fix: 30 concurrent replayed CLI turns queued on the
// flock, prepare p90 1449 ms vs 261 ms at 5 (lockWait p90 1399 ms).

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"
)

var concurrentBinding = Binding{Principal: "sandbox", Session: "00000000-0000-4000-8000-000000000001"}

// unitStore is one DPX unit's map dir; each request opens its own Session, as
// the executor does (prepareDPXAlias calls Open per request).
func unitStore(t *testing.T) (string, Binding) {
	t.Helper()
	dir := privateDir(t)
	b := concurrentBinding
	b.Version = testVersion()
	if _, err := Create(dir, b, DefaultManifest()); err != nil {
		t.Fatal(err)
	}
	return dir, b
}

func openPrepare(dir string, b Binding, raw []byte) ([]byte, *RequestMap, error) {
	s, err := Open(dir, b, DefaultManifest())
	if err != nil {
		return nil, nil, err
	}
	return Prepare(raw, s)
}

func holdFlock(t *testing.T, dir string) (release func()) {
	t.Helper()
	f, err := os.OpenFile(filepath.Join(dir, "lock"), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	release = func() {
		once.Do(func() {
			syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
			f.Close()
		})
	}
	t.Cleanup(release)
	return release
}

// A warm request that adds nothing to the map must not wait for the store lock:
// another request holding it (a commit, or the pre-fix Prepare) is no reason to
// queue. A request that DOES change the map still waits, so the commit stays a
// serialized read-modify-write.
func TestWarmPrepareDoesNotWaitForStoreLock(t *testing.T) {
	raw := syntheticBody(t, fixtureSizes[0])
	dir, b := unitStore(t)
	if _, _, err := openPrepare(dir, b, raw); err != nil { // warm the map
		t.Fatal(err)
	}
	release := holdFlock(t, dir)
	const n = 30
	done := make(chan error, n)
	for i := 0; i < n; i++ {
		go func() { _, _, err := openPrepare(dir, b, raw); done <- err }()
	}
	deadline := time.After(3 * time.Second)
	for i := 0; i < n; i++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-deadline:
			release()
			t.Fatalf("%d of %d warm Prepares were still queued on the held store lock after 3 s: "+
				"per-request work runs under the lock (t_1f4b978a)", n-i, n)
		}
	}

	// A new tool changes the map: it must block until the holder releases.
	mut := []byte(`{"model":"m","tools":[{"name":"brand_new_tool","input_schema":{"type":"object","properties":{"x":{"type":"string"}}}}],"messages":[{"role":"user","content":"hi"}]}`)
	mutDone := make(chan error, 1)
	go func() { _, _, err := openPrepare(dir, b, mut); mutDone <- err }()
	select {
	case err := <-mutDone:
		t.Fatalf("a map-changing Prepare committed while another fd held the store lock (err %v)", err)
	case <-time.After(300 * time.Millisecond):
	}
	release()
	if err := <-mutDone; err != nil {
		t.Fatal(err)
	}
	s, err := Open(dir, b, DefaultManifest())
	if err != nil {
		t.Fatal(err)
	}
	st, err := s.load()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := st.Tools["brand_new_tool"]; !ok {
		t.Fatal("the map-changing Prepare did not persist its tool")
	}
}

func p90(ds []time.Duration) time.Duration { return percentile(ds, 0.90) }

// runConcurrent starts n goroutines on one barrier and returns each call's latency.
func runConcurrent(t *testing.T, n int, call func() error) []time.Duration {
	t.Helper()
	lat := make([]time.Duration, n)
	errs := make(chan error, n)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			t0 := time.Now()
			err := call()
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
	return lat
}

// 30 goroutines on one unit: the p90 of Open+Prepare stays within 2x of the
// p90 of the SAME work done by 30 goroutines with no store and no lock at all
// (decode an in-memory copy of the map, alias the body), in the same run. That
// reference absorbs what the host does to 30 CPU-bound goroutines (cores, SMT,
// load, memory bandwidth), so the bound sees only what the store adds: a
// shared-lock queue puts the 30th caller ~30 solo latencies back (measured
// 8-10x the reference before the fix, 1-1.3x after). 5 vs 30 is logged; it is
// the E2E criterion, read on the unit by claude-dpx studio/concurrency_harness.py.
func TestPrepareP90FlatAt30Goroutines(t *testing.T) {
	raw := syntheticBody(t, fixtureSizes[1])
	dir, b := unitStore(t)
	if _, _, err := openPrepare(dir, b, raw); err != nil {
		t.Fatal(err)
	}
	s, err := Open(dir, b, DefaultManifest())
	if err != nil {
		t.Fatal(err)
	}
	file, err := s.readMap()
	if err != nil {
		t.Fatal(err)
	}
	unit := func() error { _, _, err := openPrepare(dir, b, raw); return err }
	ideal := func() error {
		n, err := parse(raw)
		if err != nil {
			return err
		}
		// Open and Prepare each decode the map: the reference does too.
		if _, _, err := s.decodeMap(file); err != nil {
			return err
		}
		st, _, err := s.decodeMap(file)
		if err != nil {
			return err
		}
		_, _, _, err = prepareOn(raw, n, st)
		return err
	}
	best := func(n int, call func() error) time.Duration { // best p90 of 3: host noise only ever adds
		var out time.Duration = -1
		for r := 0; r < 3; r++ {
			if p := p90(runConcurrent(t, n, call)); out < 0 || p < out {
				out = p
			}
		}
		return out
	}
	u5, u30, i30 := best(5, unit), best(30, unit), best(30, ideal)
	t.Logf("GOMAXPROCS %d: unit p90 at 5 = %v, at 30 = %v (%.2fx); lock-free reference p90 at 30 = %v (unit/reference %.2fx, limit 2x)",
		runtime.GOMAXPROCS(0), u5, u30, float64(u30)/float64(u5), i30, float64(u30)/float64(i30))
	if u30 > 2*i30 {
		t.Fatalf("30-goroutine p90 %v is %.1fx the %v of the same work with no store (limit 2x): "+
			"Prepares queue on a shared lock (t_1f4b978a)", u30, float64(u30)/float64(i30), i30)
	}
}

// Concurrent map-changing requests (distinct new tools, all at once) must each
// persist and agree with the map: optimistic commits never drop a writer.
func TestConcurrentMapChangesAllPersist(t *testing.T) {
	dir, b := unitStore(t)
	const n = 30
	type got struct {
		name string
		wire []byte
		err  error
	}
	out := make(chan got, n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		go func(i int) {
			name := fmt.Sprintf("conc_tool_%02d", i)
			raw := []byte(fmt.Sprintf(`{"model":"m","tools":[{"name":%q,"input_schema":{"type":"object","properties":{"p%d":{"type":"string"}}}}],"messages":[{"role":"user","content":"hermes %d"}]}`, name, i, i))
			<-start
			wire, _, err := openPrepare(dir, b, raw)
			out <- got{name, wire, err}
		}(i)
	}
	close(start)
	s, err := Open(dir, b, DefaultManifest())
	if err != nil {
		t.Fatal(err)
	}
	results := make([]got, 0, n)
	for i := 0; i < n; i++ {
		r := <-out
		if r.err != nil {
			t.Fatal(r.err)
		}
		results = append(results, r)
	}
	st, err := s.load()
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range results {
		tool, ok := st.Tools[r.name]
		if !ok {
			t.Fatalf("tool %s lost: a concurrent commit overwrote it", r.name)
		}
		var w struct {
			Tools []struct{ Name string } `json:"tools"`
		}
		if err := json.Unmarshal(r.wire, &w); err != nil || len(w.Tools) != 1 {
			t.Fatalf("wire for %s: %v %s", r.name, err, r.wire)
		}
		if w.Tools[0].Name != tool.Alias {
			t.Fatalf("tool %s went out as %s but the map stores alias %s", r.name, w.Tools[0].Name, tool.Alias)
		}
	}
}
