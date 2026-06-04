package statefulsensor

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"go.viam.com/rdk/logging"
	"go.viam.com/rdk/resource"
)

func newTestSensor(t *testing.T, filePath string) *statefulSensor {
	t.Helper()
	name := resource.NewName(resource.APINamespace("rdk").WithComponentType("sensor"), "test")
	s, err := New(context.Background(), nil, name, &Config{FilePath: filePath}, logging.NewTestLogger(t))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	return s.(*statefulSensor)
}

func TestCreatesFileWhenMissing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	newTestSensor(t, path)

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected state file to be created, got: %v", err)
	}
}

func TestSetAndReadings(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.json")
	s := newTestSensor(t, path)

	want := map[string]interface{}{"temperature": 72.5, "unit": "F"}
	if _, err := s.DoCommand(ctx, map[string]interface{}{"set": want}); err != nil {
		t.Fatalf("DoCommand set: %v", err)
	}

	got, err := s.Readings(ctx, nil)
	if err != nil {
		t.Fatalf("Readings: %v", err)
	}
	if got["temperature"] != 72.5 || got["unit"] != "F" {
		t.Fatalf("Readings = %+v, want %+v", got, want)
	}
}

func TestLoadsValueFromFileOnInit(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.json")

	// First sensor sets a value and persists it.
	s1 := newTestSensor(t, path)
	if _, err := s1.DoCommand(ctx, map[string]interface{}{"set": map[string]interface{}{"count": 3.0}}); err != nil {
		t.Fatalf("DoCommand set: %v", err)
	}
	_ = s1.Close(ctx)

	// A fresh sensor pointed at the same file should load the persisted value.
	s2 := newTestSensor(t, path)

	got, err := s2.Readings(ctx, nil)
	if err != nil {
		t.Fatalf("Readings: %v", err)
	}
	if got["count"] != 3.0 {
		t.Fatalf("Readings = %+v, want count=3", got)
	}
}

func TestDefaultPathUsesModuleDataDir(t *testing.T) {
	dataDir := t.TempDir()
	t.Setenv("VIAM_MODULE_DATA", dataDir)

	// No FilePath configured: the sensor should persist into VIAM_MODULE_DATA.
	name := resource.NewName(resource.APINamespace("rdk").WithComponentType("sensor"), "usage-sensor")
	s, err := New(context.Background(), nil, name, &Config{}, logging.NewTestLogger(t))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = s.Close(context.Background()) })

	want := filepath.Join(dataDir, "usage-sensor_state.json")
	if got := s.(*statefulSensor).filePath; got != want {
		t.Fatalf("filePath = %q, want %q", got, want)
	}
	if _, err := os.Stat(want); err != nil {
		t.Fatalf("expected state file in module data dir, got: %v", err)
	}
}

func TestDoCommandRejectsUnknownCommand(t *testing.T) {
	ctx := context.Background()
	s := newTestSensor(t, filepath.Join(t.TempDir(), "state.json"))

	if _, err := s.DoCommand(ctx, map[string]interface{}{"bogus": 1}); err == nil {
		t.Fatal("expected error for unknown command, got nil")
	}
}
