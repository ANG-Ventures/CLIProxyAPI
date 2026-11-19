package cliproxy

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/config"
)

// The PostgreSQL store lays its spool out as <spool>/config/config.yaml and
// <spool>/auths, and Bootstrap does os.RemoveAll(<spool>/auths). The saved
// model catalogs must survive that, so they must not live under the auth dir.
func TestModelCatalogStateDirSurvivesAuthSpoolReset(t *testing.T) {
	spool := t.TempDir()
	authDir := filepath.Join(spool, "auths")
	configPath := filepath.Join(spool, "config", "config.yaml")
	cfg := &config.Config{AuthDir: authDir}

	stateDir := modelCatalogStateDir(configPath, cfg)
	if stateDir == "" {
		t.Fatal("no state dir")
	}
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		t.Fatal(err)
	}
	saved := filepath.Join(stateDir, "catalog.catalog")
	if err := os.WriteFile(saved, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}

	// What syncAuthFromDatabase does on every Bootstrap.
	if err := os.RemoveAll(authDir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(saved); err != nil {
		t.Fatalf("saved catalog %s lost when the auth spool was reset: %v", saved, err)
	}
	if strings.HasPrefix(stateDir, authDir+string(filepath.Separator)) {
		t.Fatalf("state dir %s is inside auth dir %s", stateDir, authDir)
	}
}

func TestModelCatalogStateDirFallsBackToAuthDir(t *testing.T) {
	authDir := t.TempDir()
	got := modelCatalogStateDir("", &config.Config{AuthDir: authDir})
	if want := filepath.Join(authDir, "model-catalog"); got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// A read-only config dir (e.g. /etc/cliproxy mounted ro) must not win: every
// save would fail and every offline restart would fall back to the embed.
func TestModelCatalogStateDirSkipsReadOnlyConfigDir(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	configDir := t.TempDir()
	if err := os.Chmod(configDir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(configDir, 0o700) })
	authDir := t.TempDir()
	got := modelCatalogStateDir(filepath.Join(configDir, "config.yaml"), &config.Config{AuthDir: authDir})
	if want := filepath.Join(authDir, "model-catalog"); got != want {
		t.Fatalf("read-only config dir: got %q, want %q", got, want)
	}
}
