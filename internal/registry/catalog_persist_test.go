package registry

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
)

// A startup refresh that fails (no DNS yet) must retry on the short backoff,
// not wait a full ModelsRefreshInterval.
func TestCatalogStartupRefreshRetriesOnBackoff(t *testing.T) {
	oldBackoff := catalogRetryInitialBackoff
	catalogRetryInitialBackoff = 5 * time.Millisecond
	t.Cleanup(func() { catalogRetryInitialBackoff = oldBackoff })

	var fetches atomic.Int32
	published := make(chan string, 1)
	u := &catalogUpdater{
		fetch: func(context.Context, string) ([]byte, error) {
			if fetches.Add(1) <= 2 {
				return nil, errors.New("lookup registry: no such host")
			}
			return []byte("remote"), nil
		},
		publish: func(data []byte) ([]string, error) {
			published <- string(data)
			return nil, nil
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	u.configure(ctx, "https://catalog.invalid/models.json")

	select {
	case got := <-published:
		if got != "remote" {
			t.Fatalf("published %q", got)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("no publish after %d fetches; startup retry waits for ModelsRefreshInterval (%s)", fetches.Load(), ModelsRefreshInterval)
	}
	if n := fetches.Load(); n != 3 {
		t.Fatalf("fetches = %d, want 3", n)
	}
}

func catalogWithExtraCodexModel(t *testing.T, id string) []byte {
	t.Helper()
	var doc map[string]any
	if errUnmarshal := json.Unmarshal(embeddedModelsJSON, &doc); errUnmarshal != nil {
		t.Fatal(errUnmarshal)
	}
	for _, section := range []string{"codex-free", "codex-team", "codex-plus", "codex-pro"} {
		list, _ := doc[section].([]any)
		doc[section] = append(list, map[string]any{"id": id, "object": "model", "owned_by": "openai", "type": "openai"})
	}
	data, errMarshal := json.Marshal(doc)
	if errMarshal != nil {
		t.Fatal(errMarshal)
	}
	return data
}

func restoreEmbeddedCatalog(t *testing.T) {
	t.Helper()
	old := getModels()
	t.Cleanup(func() {
		modelsCatalogStore.mu.Lock()
		modelsCatalogStore.data = old
		modelsCatalogStore.mu.Unlock()
	})
	if errLoad := loadModelsFromBytes(embeddedModelsJSON, "embed"); errLoad != nil {
		t.Fatal(errLoad)
	}
}

func useCatalogStateDir(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "model-catalog")
	SetModelCatalogStateDir(dir)
	t.Cleanup(func() { SetModelCatalogStateDir("") })
	return dir
}

func codexProHas(id string) bool {
	for _, m := range GetCodexProModels() {
		if m != nil && m.ID == id {
			return true
		}
	}
	return false
}

// A restart with an unreachable source lists the last published remote
// catalog, not the build-time embed.
func TestCatalogRestartLoadsSavedCopyWhenSourceUnreachable(t *testing.T) {
	const newModel = "gpt-6.2-sol"
	restoreEmbeddedCatalog(t)
	useCatalogStateDir(t)
	if codexProHas(newModel) {
		t.Fatalf("embed already lists %s; test needs a model absent from the embed", newModel)
	}
	const source = "https://catalog.invalid/models.json"
	remote := catalogWithExtraCodexModel(t, newModel)

	// First process: the remote refresh succeeds and is saved.
	first := &catalogUpdater{
		name:    "catalog",
		fetch:   func(context.Context, string) ([]byte, error) { return remote, nil },
		publish: publishCatalogBytes,
	}
	if !first.refresh(context.Background(), source, first.generation) {
		t.Fatal("first refresh failed")
	}
	if !codexProHas(newModel) {
		t.Fatal("remote catalog was not published")
	}

	// Restart: the process comes back on the embed and DNS is down.
	if errLoad := loadModelsFromBytes(embeddedModelsJSON, "embed"); errLoad != nil {
		t.Fatal(errLoad)
	}
	second := &catalogUpdater{
		name:    "catalog",
		fetch:   func(context.Context, string) ([]byte, error) { return nil, errors.New("no such host") },
		publish: publishCatalogBytes,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	second.configure(ctx, source)
	if !codexProHas(newModel) {
		t.Fatalf("after restart with no network, %s missing: saved catalog not loaded", newModel)
	}
}

// A corrupt saved copy falls back to the embed and logs exactly one warning.
func TestCatalogCorruptSavedCopyFallsBackToEmbed(t *testing.T) {
	restoreEmbeddedCatalog(t)
	dir := useCatalogStateDir(t)
	if errMkdir := os.MkdirAll(dir, 0o700); errMkdir != nil {
		t.Fatal(errMkdir)
	}
	if errWrite := os.WriteFile(filepath.Join(dir, "catalog.catalog"), []byte(`{"source":"x","catalog":{"claude":[`), 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	embedded := getModels()
	hook := logtest.NewGlobal()
	t.Cleanup(func() { log.StandardLogger().ReplaceHooks(make(log.LevelHooks)) })

	u := &catalogUpdater{
		name:    "catalog",
		fetch:   func(context.Context, string) ([]byte, error) { return nil, errors.New("no such host") },
		publish: publishCatalogBytes,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	u.configure(ctx, "x")

	if getModels() != embedded {
		t.Fatal("corrupt saved copy replaced the embedded catalog")
	}
	warns := 0
	for _, entry := range hook.AllEntries() {
		if entry.Level == log.WarnLevel && strings.Contains(entry.Message, "saved copy") {
			warns++
		}
	}
	if warns != 1 {
		t.Fatalf("saved-copy warnings = %d, want 1", warns)
	}
}

// A local catalog file is never cached, so a local source at the cache path
// is not overwritten with a cache envelope.
func TestCatalogLocalSourceIsNotPersisted(t *testing.T) {
	restoreEmbeddedCatalog(t)
	dir := useCatalogStateDir(t)
	if errMkdir := os.MkdirAll(dir, 0o700); errMkdir != nil {
		t.Fatal(errMkdir)
	}
	source := filepath.Join(dir, "catalog.catalog")
	local := catalogWithExtraCodexModel(t, "gpt-6.2-sol")
	if errWrite := os.WriteFile(source, local, 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	u := &catalogUpdater{name: "catalog", fetch: readCatalogSource, publish: publishCatalogBytes}
	if !u.refresh(context.Background(), source, u.generation) {
		t.Fatal("local refresh failed")
	}
	got, errRead := os.ReadFile(source)
	if errRead != nil {
		t.Fatal(errRead)
	}
	if !bytes.Equal(got, local) {
		t.Fatalf("local catalog source %s was rewritten by the cache (%d -> %d bytes)", source, len(local), len(got))
	}
}

// The fleet runs with models.catalog unset, so the updater is configured with
// source "" (the built-in remote URLs). That default source must be saved and
// restored too, or a restart without DNS lists only the embed.
func TestCatalogDefaultSourceSavedAndRestored(t *testing.T) {
	const newModel = "gpt-6.2-sol"
	restoreEmbeddedCatalog(t)
	dir := useCatalogStateDir(t)
	if codexProHas(newModel) {
		t.Fatalf("embed already lists %s; test needs a model absent from the embed", newModel)
	}
	remote := catalogWithExtraCodexModel(t, newModel)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(remote)
	}))
	urls := []string{server.URL + "/models.json"}

	// First process: the default source publishes and is saved.
	first := &catalogUpdater{
		name:            "catalog",
		fetchWithSource: dynamicCatalogFetcherWithSource(embeddedModelsJSON, &urls, validateCatalogBytes),
		publish:         publishCatalogBytes,
	}
	ctx1, cancel1 := context.WithCancel(context.Background())
	first.configure(ctx1, "")
	saved := filepath.Join(dir, "catalog.catalog")
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, errStat := os.Stat(saved); errStat == nil {
			break
		}
		if time.Now().After(deadline) {
			cancel1()
			t.Fatalf("default-source refresh never wrote %s", saved)
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel1()
	server.Close()

	// Restart: back on the embed, the default URLs unreachable.
	if errLoad := loadModelsFromBytes(embeddedModelsJSON, "embed"); errLoad != nil {
		t.Fatal(errLoad)
	}
	second := &catalogUpdater{
		name:            "catalog",
		fetchWithSource: dynamicCatalogFetcherWithSource(embeddedModelsJSON, &urls, validateCatalogBytes),
		publish:         publishCatalogBytes,
	}
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	second.configure(ctx2, "")
	if !codexProHas(newModel) {
		t.Fatalf("after restart with source \"\" and no network, %s missing: default-source catalog not restored", newModel)
	}
}
