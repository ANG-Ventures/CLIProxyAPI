package contentalias

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

type Binding struct{ Principal, Session, Version string }
type Manifest struct{ Words []string }

func DefaultManifest() Manifest     { return Manifest{[]string{"hermes", "openclaw"}} }
func digest(s string) string        { h := sha256.Sum256([]byte(s)); return hex.EncodeToString(h[:]) }
func digestBytes(b []byte) [32]byte { return sha256.Sum256(b) }

var wordSeparators = strings.NewReplacer("_", "", "-", "")

func normalize(s string) string {
	return strings.ToLower(wordSeparators.Replace(s))
}
func (m Manifest) validate() error {
	if len(m.Words) == 0 {
		return Error("manifest")
	}
	for i, a := range m.Words {
		if len(a) < 2 || len(a) > 128 {
			return Error("manifest")
		}
		for j, b := range m.Words {
			if i != j && (strings.Contains(normalize(a), normalize(b)) || strings.Contains(normalize(b), normalize(a))) {
				return Error("vocabulary_overlap")
			}
		}
	}
	return nil
}

type entry struct{ Kind, Original string }
type storedTool struct {
	Alias  string
	Schema json.RawMessage
}
type state struct {
	Binding  Binding
	Manifest Manifest
	Symbols  map[string]entry
	Tools    map[string]storedTool
	// properties indexes v2/v3 property originals -> alias (not persisted).
	properties map[string]string
	// tools indexes v3 tool names -> alias (not persisted).
	tools map[string]string
}
type envelope struct {
	Payload  json.RawMessage
	Checksum string
}
type Session struct {
	dir      string
	binding  Binding
	manifest Manifest
	// mu guards lockWait only; it is never held across store work.
	mu sync.Mutex
	// lockWait sums the time this session spent waiting for the store flock
	// (t_997bc8a2). A Session is opened per request, so the sum is that
	// request's queueing behind other requests' commits on the unit.
	lockWait time.Duration
}

// LockWait reports how long this session waited for the store lock so far.
func (s *Session) LockWait() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lockWait
}

func session(dir string, b Binding, m Manifest) (*Session, error) {
	if b.Principal == "" || b.Session == "" || !validVersion(b.Version) {
		return nil, Error("binding")
	}
	if err := m.validate(); err != nil {
		return nil, err
	}
	info, err := os.Lstat(dir)
	if err != nil || !info.IsDir() || info.Mode().Perm() != 0700 {
		return nil, Error("store_permissions")
	}
	return &Session{dir: dir, binding: b, manifest: m}, nil
}
func Create(dir string, b Binding, m Manifest) (*Session, error) {
	s, err := session(dir, b, m)
	if err != nil {
		return nil, err
	}
	lock, err := s.lock()
	if err != nil {
		return nil, err
	}
	defer unlock(lock)
	if _, err := os.Lstat(filepath.Join(dir, "map.json")); !os.IsNotExist(err) {
		return nil, Error("store_exists")
	}
	st := state{Binding: b, Manifest: m, Symbols: map[string]entry{}, Tools: map[string]storedTool{}}
	if err := s.save(st); err != nil {
		return nil, err
	}
	return s, nil
}
func Open(dir string, b Binding, m Manifest) (*Session, error) {
	s, err := session(dir, b, m)
	if err != nil {
		return nil, err
	}
	// No flock: save replaces map.json by atomic rename, so this read sees one
	// whole map (t_1f4b978a).
	_, err = s.load()
	return s, err
}
func (s *Session) lock() (*heldLock, error) {
	fd, err := syscall.Open(filepath.Join(s.dir, "lock"), syscall.O_CREAT|syscall.O_RDWR|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return nil, Error("store_lock")
	}
	f := os.NewFile(uintptr(fd), "lock")
	waitStart := time.Now()
	err = syscall.Flock(fd, syscall.LOCK_EX)
	s.mu.Lock()
	s.lockWait += time.Since(waitStart)
	s.mu.Unlock()
	if err != nil {
		f.Close()
		return nil, Error("store_lock")
	}
	return &heldLock{f, time.Now()}, nil
}

// heldLock is the exclusive store flock plus the moment it was acquired.
// Everything between lock() and unlock() serializes every request on the unit
// (t_129cf1ac), so its duration is a performance invariant: see
// lockhold_test.go and CONTENTALIAS.md "Performance invariants".
type heldLock struct {
	f  *os.File
	at time.Time
}

// lockHeldHook observes each store-lock hold. Only tests set it; it is nil in
// production builds, where unlock pays one nil check.
var lockHeldHook func(held time.Duration)

// saveStallHook runs inside save just before the rename that publishes the
// map. Only tests set it, to reproduce a slow commit (the Studio measured
// 210-476 ms renames of map.json, t_1f4b978a); nil in production.
var saveStallHook func()

func unlock(h *heldLock) {
	if hook := lockHeldHook; hook != nil {
		hook(time.Since(h.at))
	}
	syscall.Flock(int(h.f.Fd()), syscall.LOCK_UN)
	h.f.Close()
}
func (s *Session) load() (state, error) {
	raw, err := s.readMap()
	if err != nil {
		return state{}, err
	}
	st, _, err := s.decodeMap(raw)
	return st, err
}

// readMap returns map.json's bytes. Safe without the store lock: save replaces
// the file by atomic rename, so the open fd is one complete map.
func (s *Session) readMap() ([]byte, error) {
	if _, err := session(s.dir, s.binding, s.manifest); err != nil {
		return nil, err
	}
	fd, err := syscall.Open(filepath.Join(s.dir, "map.json"), syscall.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, Error("store_missing")
	}
	f := os.NewFile(uintptr(fd), "map")
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 || info.Size() > 16<<20 {
		return nil, Error("store_permissions")
	}
	raw := make([]byte, info.Size())
	n, err := f.ReadAt(raw, 0)
	if err != nil || n != len(raw) {
		return nil, Error("store_read")
	}
	return raw, nil
}

// commitIfUnchanged saves st under the store lock iff map.json still holds
// snapshot (the bytes st was derived from). false = another request committed
// first; the caller redoes its work on the new map.
func (s *Session) commitIfUnchanged(snapshot []byte, st state) (bool, error) {
	lock, err := s.lock()
	if err != nil {
		return false, err
	}
	defer unlock(lock)
	cur, err := s.readMap()
	if err != nil {
		return false, err
	}
	if !bytes.Equal(cur, snapshot) {
		return false, nil
	}
	return true, s.save(st)
}

// decodeMap validates map.json's bytes and returns the state and its canonical
// payload (json.Marshal of the state, as save writes it).
func (s *Session) decodeMap(raw []byte) (state, []byte, error) {
	var st state
	var env envelope
	if _, err := parse(raw); err != nil {
		return st, nil, Error("store_corrupt")
	}
	if json.Unmarshal(raw, &env) != nil || digest(string(env.Payload)) != env.Checksum || json.Unmarshal(env.Payload, &st) != nil {
		return st, nil, Error("store_corrupt")
	}
	expected, _ := json.Marshal(s.manifest)
	actual, _ := json.Marshal(st.Manifest)
	if st.Binding != s.binding || string(expected) != string(actual) || st.Symbols == nil || st.Tools == nil {
		return st, nil, Error("store_version")
	}
	for alias, e := range st.Symbols {
		if wordPairTools(st.Binding) && e.Kind == "t" {
			if !validToolAlias(st.Binding, alias, e.Original) {
				return st, nil, Error("store_corrupt")
			}
			if st.tools == nil {
				st.tools = map[string]string{}
			}
			if _, dup := st.tools[e.Original]; dup {
				return st, nil, Error("store_corrupt")
			}
			st.tools[e.Original] = alias
			continue
		}
		if wordPairAliases(st.Binding) && e.Kind == "p" {
			if !validPropertyAlias(st.Binding, alias, e.Original) {
				return st, nil, Error("store_corrupt")
			}
			if st.properties == nil {
				st.properties = map[string]string{}
			}
			if _, dup := st.properties[e.Original]; dup {
				return st, nil, Error("store_corrupt")
			}
			st.properties[e.Original] = alias
			continue
		}
		if alias != symbol(st.Binding, e.Kind, e.Original) {
			return st, nil, Error("store_corrupt")
		}
	}
	canonicalState, _ := json.Marshal(st)
	if !bytes.Equal(canonicalState, env.Payload) {
		return st, nil, Error("store_corrupt")
	}
	for name, tool := range st.Tools {
		want := symbol(st.Binding, "t", name)
		if wordPairTools(st.Binding) {
			want = st.tools[name]
		}
		if want == "" || tool.Alias != want || st.Symbols[tool.Alias] != (entry{"t", name}) {
			return st, nil, Error("store_tool_relation")
		}
		if _, err := parse(tool.Schema); err != nil {
			return st, nil, Error("store_schema")
		}
	}
	return st, env.Payload, nil
}
func (s *Session) save(st state) error {
	payload, err := json.Marshal(st)
	if err != nil {
		return Error("store_write")
	}
	raw, err := json.Marshal(envelope{payload, digest(string(payload))})
	if err != nil || len(raw) > 16<<20 {
		return Error("store_limit")
	}
	// Every caller holds the exclusive store lock, so no other save is in flight: any
	// ".map-*" left here is residue of a writer killed between CreateTemp and rename
	// (defer below covers every error return, not SIGKILL). Sweep it before writing.
	if stale, globErr := filepath.Glob(filepath.Join(s.dir, ".map-*")); globErr == nil {
		for _, p := range stale {
			_ = os.Remove(p)
		}
	}
	f, err := os.CreateTemp(s.dir, ".map-")
	if err != nil {
		return Error("store_write")
	}
	name := f.Name()
	defer os.Remove(name)
	if _, err = f.Write(raw); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil || closeErr != nil {
		return Error("store_write")
	}
	if hook := saveStallHook; hook != nil {
		hook()
	}
	if err = os.Rename(name, filepath.Join(s.dir, "map.json")); err != nil {
		return Error("store_write")
	}
	dir, err := os.Open(s.dir)
	if err != nil {
		return Error("store_write")
	}
	defer dir.Close()
	if dir.Sync() != nil {
		return Error("store_write")
	}
	return nil
}
func symbol(b Binding, kind, original string) string {
	key, _ := json.Marshal([]string{b.Principal, b.Session, b.Version, kind, original})
	return "dpx_v1_" + kind + "_" + digest(string(key))[:24]
}
func (st *state) allocate(kind, original string) (string, error) {
	if kind == "p" && wordPairAliases(st.Binding) {
		return st.allocateProperty(original)
	}
	if kind == "t" && wordPairTools(st.Binding) {
		return st.allocateTool(original)
	}
	alias := symbol(st.Binding, kind, original)
	if old, ok := st.Symbols[alias]; ok && old != (entry{kind, original}) {
		return "", Error("symbol_collision")
	}
	st.Symbols[alias] = entry{kind, original}
	return alias, nil
}
