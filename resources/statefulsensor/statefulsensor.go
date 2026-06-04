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

	name   resource.Name
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

	filePath := conf.FilePath
	if filePath == "" {
		filePath = fmt.Sprintf("%s_state.json", name.Name)
	}

	s := &statefulSensor{
		name:       name,
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

func (s *statefulSensor) Name() resource.Name {
	return s.name
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

// DoCommand supports a "set" command that replaces the value the sensor holds.
//
// Example:
//
//	{"set": {"temperature": 72.5, "unit": "F"}}
//
// The provided object becomes the sensor's value, is persisted to disk, and is
// returned by Readings.
func (s *statefulSensor) DoCommand(ctx context.Context, cmd map[string]interface{}) (map[string]interface{}, error) {
	raw, ok := cmd["set"]
	if !ok {
		return nil, fmt.Errorf("unsupported command: expected a %q key", "set")
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
