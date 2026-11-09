package contentalias

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// t_997bc8a2: Prepare reports how long it queued for the unit's store lock, so
// the wirelog can split accept->upstream into lock wait vs work. A second fd
// holds the flock (another request on the unit); a Prepare that changes the map
// (a manifest word into a fresh map) must block until it is released and count
// that time. A Prepare that changes nothing takes no lock (t_1f4b978a,
// TestWarmPrepareDoesNotWaitForStoreLock). The bound is a lower bound only, so timer
// jitter can only make the measured wait longer, never fail the test.
func TestPrepareReportsStoreLockWait(t *testing.T) {
	dir := privateDir(t)
	b := Binding{Principal: "sandbox", Session: "00000000-0000-4000-8000-000000000001", Version: "v1"}
	if _, err := Create(dir, b, DefaultManifest()); err != nil {
		t.Fatal(err)
	}
	s, err := Open(dir, b, DefaultManifest())
	if err != nil {
		t.Fatal(err)
	}
	if s.LockWait() < 0 {
		t.Fatalf("negative lock wait %v", s.LockWait())
	}
	before := s.LockWait()

	holder, err := os.OpenFile(filepath.Join(dir, "lock"), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Close()
	if err := syscall.Flock(int(holder.Fd()), syscall.LOCK_EX); err != nil {
		t.Fatal(err)
	}
	const held = 150 * time.Millisecond
	done := make(chan error, 1)
	go func() {
		_, _, err := Prepare([]byte(`{"model":"m","messages":[{"role":"user","content":"hello hermes"}]}`), s)
		done <- err
	}()
	time.Sleep(held)
	if err := syscall.Flock(int(holder.Fd()), syscall.LOCK_UN); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got := s.LockWait() - before; got < held*2/3 {
		t.Fatalf("Prepare lock wait = %v, want >= %v (the flock was held %v)", got, held*2/3, held)
	}
}
