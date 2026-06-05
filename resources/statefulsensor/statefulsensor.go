// Package statefulsensor implements the viam:sensor-bundle:stateful-sensor model:
// a sensor that holds a value set via DoCommand, serves it through Readings, and
// persists it to a file on disk so it survives restarts.
package statefulsensor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	sensor "go.viam.com/rdk/components/sensor"
	"go.viam.com/rdk/logging"
	"go.viam.com/rdk/resource"
)

// Model is the stateful-sensor model triplet.
var Model = resource.NewModel("viam", "sensor-bundle", "stateful-sensor")

func init() {
	resource.RegisterComponent(sensor.API, Model,
		resource.Registration[sensor.Sensor, *Config]{
			Constructor: newStatefulSensor,
		},
	)
}

type Config struct {
	// FilePath is where the sensor's value is persisted on disk. If empty, a
	// default file named "<resource-name>_state.json" is created in the current
	// working directory.
	FilePath string `json:"file_path,omitempty"`
}

// Validate ensures all parts of the config are valid and important fields exist.
// Returns three values:
//  1. Required dependencies: other resources that must exist for this resource to work.
//  2. Optional dependencies: other resources that may exist but are not required.
//  3. An error if any Config fields are missing or invalid.
func (cfg *Config) Validate(path string) ([]string, []string, error) {
	// No required fields: file_path is optional and falls back to a default.
	return nil, nil, nil
}

type statefulSensor struct {
	resource.AlwaysRebuild
	resource.Named

	logger logging.Logger
	cfg    *Config

	filePath string

	mu    sync.RWMutex
	value map[string]interface{}

	cancelCtx  context.Context
	cancelFunc func()
}

func newStatefulSensor(ctx context.Context, deps resource.Dependencies, rawConf resource.Config, logger logging.Logger) (sensor.Sensor, error) {
	conf, err := resource.NativeConfig[*Config](rawConf)
	if err != nil {
		return nil, err
	}

	return New(ctx, deps, rawConf.ResourceName(), conf, logger)
}

// New builds a stateful-sensor and loads any previously persisted value from disk.
func New(ctx context.Context, deps resource.Dependencies, name resource.Name, conf *Config, logger logging.Logger) (sensor.Sensor, error) {
	cancelCtx, cancelFunc := context.WithCancel(context.Background())

	// When no file_path is configured, default to a file in the module's data
	// directory. Viam sets VIAM_MODULE_DATA to a per-module directory that is
	// created for us and guaranteed writable; persisting there keeps state
	// stable across restarts and redeploys. Fall back to the current working
	// directory (e.g. local runs and tests) when the env var is not set.
	filePath := conf.FilePath
	if filePath == "" {
		fileName := fmt.Sprintf("%s_state.json", name.Name)
		if dataDir := os.Getenv("VIAM_MODULE_DATA"); dataDir != "" {
			filePath = filepath.Join(dataDir, fileName)
		} else {
			filePath = fileName
		}
	}

	s := &statefulSensor{
		Named:      name.AsNamed(),
		logger:     logger,
		cfg:        conf,
		filePath:   filePath,
		value:      map[string]interface{}{},
		cancelCtx:  cancelCtx,
		cancelFunc: cancelFunc,
	}

	if err := s.loadFromFile(); err != nil {
		cancelFunc()
		return nil, err
	}

	return s, nil
}

// loadFromFile reads the persisted value from disk into memory. If the file does
// not exist, it is created with the current (empty) value.
func (s *statefulSensor) loadFromFile() error {
	data, err := os.ReadFile(s.filePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			s.logger.Infof("state file %q not found; creating it", s.filePath)
			return s.saveToFile()
		}
		return fmt.Errorf("failed to read state file %q: %w", s.filePath, err)
	}

	if len(data) == 0 {
		// Treat an empty file as an empty value.
		return nil
	}

	var value map[string]interface{}
	if err := json.Unmarshal(data, &value); err != nil {
		return fmt.Errorf("failed to parse state file %q: %w", s.filePath, err)
	}

	s.mu.Lock()
	s.value = value
	s.mu.Unlock()

	s.logger.Infof("loaded state from %q", s.filePath)
	return nil
}

// saveToFile persists the current in-memory value to disk atomically.
func (s *statefulSensor) saveToFile() error {
	s.mu.RLock()
	data, err := json.MarshalIndent(s.value, "", "  ")
	s.mu.RUnlock()
	if err != nil {
		return fmt.Errorf("failed to encode state: %w", err)
	}

	dir := filepath.Dir(s.filePath)
	tmp, err := os.CreateTemp(dir, ".state-*.json.tmp")
	if err != nil {
		return fmt.Errorf("failed to create temp state file in %q: %w", dir, err)
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }() // no-op if the rename succeeded

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("failed to write temp state file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("failed to close temp state file: %w", err)
	}

	if err := os.Rename(tmpName, s.filePath); err != nil {
		return fmt.Errorf("failed to persist state file %q: %w", s.filePath, err)
	}
	return nil
}

func (s *statefulSensor) Readings(ctx context.Context, extra map[string]interface{}) (map[string]interface{}, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Return a copy so callers cannot mutate our internal state.
	readings := make(map[string]interface{}, len(s.value))
	for k, v := range s.value {
		readings[k] = v
	}
	return readings, nil
}

// Status reports operational metadata for the sensor, served via the sensor
// service's GetStatus RPC. It is defined explicitly (rather than relying on the
// empty default promoted from the embedded resource.Named) so the model both
// exposes useful state and is guaranteed to have a non-nil receiver. Unlike
// Readings, which returns the stored value itself, Status reports where the
// value is persisted and how many keys it holds.
func (s *statefulSensor) Status(ctx context.Context) (map[string]interface{}, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return map[string]interface{}{
		"file_path": s.filePath,
		"num_keys":  len(s.value),
	}, nil
}

// DoCommand supports two commands:
//
//   - "set" replaces the entire value the sensor holds:
//     {"set": {"temperature": 72.5, "unit": "F"}}
//   - "merge" overlays the given keys onto the existing value, leaving keys not
//     mentioned untouched: {"merge": {"usage": 0}}
//
// In both cases the result is persisted to disk and returned by Readings.
func (s *statefulSensor) DoCommand(ctx context.Context, cmd map[string]interface{}) (map[string]interface{}, error) {
	if raw, ok := cmd["merge"]; ok {
		patch, ok := raw.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("%q must be an object mapping keys to values, got %T", "merge", raw)
		}

		s.mu.Lock()
		// Guard against a nil map (sensor never set, empty state file): writing to
		// a nil map panics, so initialize before overlaying the patch.
		if s.value == nil {
			s.value = make(map[string]interface{}, len(patch))
		}
		for k, v := range patch {
			s.value[k] = v
		}
		s.mu.Unlock()

		if err := s.saveToFile(); err != nil {
			return nil, err
		}

		s.logger.Infof("merged %+v", patch)
		return map[string]interface{}{"merge": "ok"}, nil
	}

	raw, ok := cmd["set"]
	if !ok {
		return nil, fmt.Errorf("unsupported command: expected a %q or %q key", "set", "merge")
	}

	value, ok := raw.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("%q must be an object mapping keys to values, got %T", "set", raw)
	}

	s.mu.Lock()
	s.value = value
	s.mu.Unlock()

	if err := s.saveToFile(); err != nil {
		return nil, err
	}

	s.logger.Infof("set value to %+v", value)
	return map[string]interface{}{"set": "ok"}, nil
}

func (s *statefulSensor) Close(context.Context) error {
	s.cancelFunc()
	return nil
}
