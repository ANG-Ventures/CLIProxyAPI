package contentalias

// t_1f4b978a canary (claude-dpx-25, 2026-11-09): 30 concurrent identical
// requests on a freshly started unit read prepare p90 1504 ms, lockWait p90
// 1455 ms, on round 1 only (round 2: ~20 ms, 0 wait). The first request
// commits the new map and the other 29 must wait until it is published. The
// commit's save used (*os.File).Sync on the temp file and the directory, which
// on darwin is F_FULLFSYNC (a drive-cache flush): measured on the Studio 87-550
// ms per commit with the rename stalled behind it, vs 10-24 ms with fsync(2).
// Linux CI can't see it because there Sync already is fsync(2), so this pins
// the call instead: every sync in save goes through syncFd (fsync(2)).

import (
	"sync"
	"testing"
)

func TestSaveSyncsThroughFsyncNotFullFsync(t *testing.T) {
	raw := syntheticBody(t, fixtureSizes[0])
	s := testSession(t) // Create's own save is not counted
	var mu sync.Mutex
	calls := 0
	prev := syncFd
	syncFd = func(fd int) error { mu.Lock(); calls++; mu.Unlock(); return prev(fd) }
	t.Cleanup(func() { syncFd = prev })
	holds := recordLockHolds(t)
	if _, _, err := Prepare(raw, s); err != nil { // cold: allocates and commits once
		t.Fatal(err)
	}
	if got := len(holds()); got != 1 {
		t.Fatalf("cold Prepare took %d store-lock holds, want 1 commit", got)
	}
	// One commit = one save = the temp file's sync + the directory's sync.
	if calls != 2 {
		t.Fatalf("the commit made %d syncFd calls, want 2 (temp file + dir): save must sync with fsync(2), "+
			"not (*os.File).Sync, which is F_FULLFSYNC on darwin and held the store lock up to 1.45 s on the "+
			"Studio while 29 requests waited for the commit (t_1f4b978a)", calls)
	}
}
