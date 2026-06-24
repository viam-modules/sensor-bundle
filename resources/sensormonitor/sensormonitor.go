// Package sensormonitor implements the viam:sensor-bundle:sensor-monitor model: a
// sensor that watches another sensor's readings against numeric trigger rules and
// fires configurable DoCommand actions on other resources when a rule triggers or
// resolves.
package sensormonitor

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	sensor "go.viam.com/rdk/components/sensor"
	"go.viam.com/rdk/logging"
	"go.viam.com/rdk/resource"
)

// Model is the sensor-monitor model triplet. It watches the readings of a sensor
// and fires DoCommand actions on other resources when a numeric reading crosses a
// configured threshold.
var Model = resource.NewModel("viam", "sensor-bundle", "sensor-monitor")

func init() {
	resource.RegisterComponent(sensor.API, Model,
		resource.Registration[sensor.Sensor, *Config]{
			Constructor: newSensorMonitor,
		},
	)
}

// defaultPollInterval is used when poll_interval_seconds is not set.
const defaultPollInterval = 10 * time.Second

// Action is a DoCommand to fire on a resource when a rule changes state.
type Action struct {
	// Resource is the name of the resource to call.
	Resource string `json:"resource"`
	// Command is the DoCommand payload. String values may contain ${...}
	// references that are resolved before the command is sent — see resolveValue.
	Command map[string]interface{} `json:"command"`
	// Capture, when set, stores this action's response under this name so later
	// actions can reference its fields as ${name.field}.
	Capture string `json:"capture,omitempty"`
}

// Rule describes a single numeric trigger on one reading key.
type Rule struct {
	// Key is the reading key to watch, e.g. "temperature".
	Key string `json:"key"`
	// Operator is the comparison to apply between the reading value and Threshold.
	// Supported: ">", ">=", "<", "<=", "==", "!=" (aliases: gt, gte, lt, lte, eq, ne).
	Operator string `json:"operator"`
	// Threshold is the value the reading is compared against.
	Threshold float64 `json:"threshold"`
	// OnTrigger lists actions to fire when the rule transitions to triggered, and
	// again on each cooldown window while it stays triggered.
	OnTrigger []Action `json:"on_trigger,omitempty"`
	// OnResolve lists actions to fire when the rule clears (its reading returns to
	// the non-triggered side of the threshold).
	OnResolve []Action `json:"on_resolve,omitempty"`
}

// Config is the configuration for the sensor-monitor model.
type Config struct {
	// Sensor is the name of the sensor dependency whose readings are monitored.
	Sensor string `json:"sensor"`
	// Rules is the set of numeric trigger rules. At least one is required.
	Rules []Rule `json:"rules"`
	// PollIntervalSec is how often the sensor is polled, in seconds. Defaults to 10.
	PollIntervalSec float64 `json:"poll_interval_seconds,omitempty"`
	// CooldownSec is the minimum time between repeat on_trigger firings while a
	// rule stays triggered, in seconds. 0 (default) means fire only on the edge.
	CooldownSec float64 `json:"cooldown_seconds,omitempty"`
}

// Validate checks the config and returns the required dependency names.
func (cfg *Config) Validate(path string) ([]string, []string, error) {
	if cfg.Sensor == "" {
		return nil, nil, fmt.Errorf("%s: missing required field 'sensor'", path)
	}
	if len(cfg.Rules) == 0 {
		return nil, nil, fmt.Errorf("%s: at least one rule is required", path)
	}

	deps := []string{cfg.Sensor}
	seen := map[string]bool{cfg.Sensor: true}
	addDep := func(name string) {
		if !seen[name] {
			seen[name] = true
			deps = append(deps, name)
		}
	}

	for i, r := range cfg.Rules {
		if r.Key == "" {
			return nil, nil, fmt.Errorf("%s: rules[%d] missing required field 'key'", path, i)
		}
		if _, err := parseOperator(r.Operator); err != nil {
			return nil, nil, fmt.Errorf("%s: rules[%d] %w", path, i, err)
		}
		for j, a := range r.OnTrigger {
			if err := validateAction(a); err != nil {
				return nil, nil, fmt.Errorf("%s: rules[%d].on_trigger[%d] %w", path, i, j, err)
			}
			addDep(a.Resource)
		}
		for j, a := range r.OnResolve {
			if err := validateAction(a); err != nil {
				return nil, nil, fmt.Errorf("%s: rules[%d].on_resolve[%d] %w", path, i, j, err)
			}
			addDep(a.Resource)
		}
	}
	return deps, nil, nil
}

// validateAction checks a single action's required fields.
func validateAction(a Action) error {
	if a.Resource == "" {
		return fmt.Errorf("missing required field 'resource'")
	}
	if a.Command == nil {
		return fmt.Errorf("missing required field 'command'")
	}
	return nil
}

// ruleState tracks the runtime state of a single rule across polls.
type ruleState struct {
	triggered bool
	lastFired time.Time
	lastValue float64
	// vars holds the responses of actions that set "capture", keyed by capture
	// name, so later actions can reference values produced. Reset when the rule resolves.
	vars map[string]interface{}
}

type sensorMonitor struct {
	resource.Named
	resource.AlwaysRebuild

	logger logging.Logger
	cfg    *Config

	sensorDep sensor.Sensor
	// actionResources holds every resource named by a rule action, resolved once
	// at construction and keyed by name.
	actionResources map[string]resource.Resource

	pollInterval time.Duration
	cooldown     time.Duration

	cancelCtx  context.Context
	cancelFunc func()
	wg         sync.WaitGroup

	mu           sync.RWMutex
	lastReadings map[string]interface{}
	ruleStates   []ruleState
}

func newSensorMonitor(ctx context.Context, deps resource.Dependencies, rawConf resource.Config, logger logging.Logger) (sensor.Sensor, error) {
	conf, err := resource.NativeConfig[*Config](rawConf)
	if err != nil {
		return nil, err
	}
	return New(ctx, deps, rawConf.ResourceName(), conf, logger)
}

// New builds a sensor-monitor and starts its background polling loop.
func New(ctx context.Context, deps resource.Dependencies, name resource.Name, conf *Config, logger logging.Logger) (sensor.Sensor, error) {
	m, err := newMonitor(deps, name, conf, logger)
	if err != nil {
		return nil, err
	}

	m.wg.Add(1)
	go m.run()

	return m, nil
}

// newMonitor resolves dependencies and builds the monitor WITHOUT starting the
// background polling loop. New wraps this and starts the loop; tests use it to
// drive poll deterministically.
func newMonitor(deps resource.Dependencies, name resource.Name, conf *Config, logger logging.Logger) (*sensorMonitor, error) {
	sensorDep, err := sensor.FromProvider(deps, conf.Sensor)
	if err != nil {
		return nil, fmt.Errorf("failed to get sensor dependency %q: %w", conf.Sensor, err)
	}

	// Resolve every resource named by an action up front so firing is a simple map lookup + DoCommand.
	actionResources := map[string]resource.Resource{}
	resolveAction := func(a Action) error {
		if _, ok := actionResources[a.Resource]; ok {
			return nil
		}
		res, err := lookupResource(deps, a.Resource)
		if err != nil {
			return fmt.Errorf("action resource %q: %w", a.Resource, err)
		}
		actionResources[a.Resource] = res
		return nil
	}
	for _, r := range conf.Rules {
		for _, a := range r.OnTrigger {
			if err := resolveAction(a); err != nil {
				return nil, err
			}
		}
		for _, a := range r.OnResolve {
			if err := resolveAction(a); err != nil {
				return nil, err
			}
		}
	}

	pollInterval := defaultPollInterval
	if conf.PollIntervalSec > 0 {
		pollInterval = time.Duration(conf.PollIntervalSec * float64(time.Second))
	}
	cooldown := time.Duration(conf.CooldownSec * float64(time.Second))

	cancelCtx, cancelFunc := context.WithCancel(context.Background())

	return &sensorMonitor{
		Named:           name.AsNamed(),
		logger:          logger,
		cfg:             conf,
		sensorDep:       sensorDep,
		actionResources: actionResources,
		pollInterval:    pollInterval,
		cooldown:        cooldown,
		cancelCtx:       cancelCtx,
		cancelFunc:      cancelFunc,
		lastReadings:    map[string]interface{}{},
		ruleStates:      make([]ruleState, len(conf.Rules)),
	}, nil
}

// lookupResource finds a dependency by its short name regardless of API, so an
// action can target any resource type. Errors if no dependency — or more than
// one — matches the name.
func lookupResource(deps resource.Dependencies, shortName string) (resource.Resource, error) {
	var found resource.Resource
	for n, r := range deps {
		if n.Name != shortName {
			continue
		}
		if found != nil {
			return nil, fmt.Errorf("matches multiple dependencies")
		}
		found = r
	}
	if found == nil {
		return nil, fmt.Errorf("not found in dependencies")
	}
	return found, nil
}

// run is the background polling loop. It exits when the resource is closed.
func (m *sensorMonitor) run() {
	defer m.wg.Done()

	ticker := time.NewTicker(m.pollInterval)
	defer ticker.Stop()

	// Poll once immediately so we don't wait a full interval for the first check.
	m.poll(m.cancelCtx)

	for {
		select {
		case <-m.cancelCtx.Done():
			return
		case <-ticker.C:
			m.poll(m.cancelCtx)
		}
	}
}

// poll reads the sensor once, evaluates every rule, and fires the rule's actions.
func (m *sensorMonitor) poll(ctx context.Context) {
	readings, err := m.sensorDep.Readings(ctx, nil)
	if err != nil {
		m.logger.Warnf("failed to read sensor %q: %v", m.cfg.Sensor, err)
		return
	}

	m.mu.Lock()
	m.lastReadings = readings
	m.mu.Unlock()

	now := time.Now()
	for i := range m.cfg.Rules {
		rule := m.cfg.Rules[i]

		raw, ok := readings[rule.Key]
		if !ok {
			m.logger.Debugf("reading key %q not present; skipping rule %d", rule.Key, i)
			continue
		}
		value, ok := toFloat64(raw)
		if !ok {
			m.logger.Debugf("reading key %q is not numeric (%T); skipping rule %d", rule.Key, raw, i)
			continue
		}

		// parseOperator already succeeded during Validate, so ignore the error here.
		cmp, _ := parseOperator(rule.Operator)
		fired := cmp(value, rule.Threshold)

		fireTrigger := false
		fireResolve := false
		m.mu.Lock()
		st := &m.ruleStates[i]
		st.lastValue = value
		switch {
		case fired && !st.triggered:
			st.triggered = true
			st.lastFired = now
			fireTrigger = true
		case fired && st.triggered && m.cooldown > 0 && now.Sub(st.lastFired) >= m.cooldown:
			st.lastFired = now
			fireTrigger = true
		case !fired:
			fireResolve = st.triggered
			st.triggered = false
		}
		m.mu.Unlock()

		if fireTrigger {
			m.runActions(ctx, i, rule, rule.OnTrigger, value)
		}
		if fireResolve {
			m.runActions(ctx, i, rule, rule.OnResolve, value)
			// The episode is over; drop captured vars so the next one starts fresh.
			m.mu.Lock()
			m.ruleStates[i].vars = nil
			m.mu.Unlock()
		}
	}
}

// runActions fires each action's DoCommand on its resolved resource. Before each
// call it resolves ${...} references in the command against the rule/reading
// context and any captured responses; after a successful call it stores the
// response under the action's capture name (if set) for later actions to
// reference. Best-effort: an unresolved reference, missing resource, or DoCommand
// error logs a warning and skips that action without affecting monitoring.
func (m *sensorMonitor) runActions(ctx context.Context, ruleIdx int, rule Rule, actions []Action, value float64) {
	if len(actions) == 0 {
		return
	}

	// Substitution context: the rule/reading values plus any previously captured
	// responses for this rule.
	subCtx := map[string]interface{}{
		"value":     value,
		"key":       rule.Key,
		"threshold": rule.Threshold,
		"operator":  rule.Operator,
	}
	m.mu.RLock()
	for k, v := range m.ruleStates[ruleIdx].vars {
		subCtx[k] = v
	}
	m.mu.RUnlock()

	captured := map[string]interface{}{}
	for _, a := range actions {
		res, ok := m.actionResources[a.Resource]
		if !ok {
			// Every action resource is resolved at construction, so this is unexpected.
			m.logger.Warnf("action resource %q not resolved; skipping", a.Resource)
			continue
		}
		resolved, err := resolveValue(a.Command, subCtx)
		if err != nil {
			m.logger.Warnf("skipping action on %q: %v", a.Resource, err)
			continue
		}
		cmd, _ := resolved.(map[string]interface{})
		resp, err := res.DoCommand(ctx, cmd)
		if err != nil {
			m.logger.Warnf("action DoCommand on %q failed: %v", a.Resource, err)
			continue
		}
		if a.Capture != "" {
			// Available to later actions in this batch and (after the loop) persisted
			// for on_resolve.
			subCtx[a.Capture] = resp
			captured[a.Capture] = resp
		}
		m.logger.Infof("action fired on %q", a.Resource)
	}

	if len(captured) > 0 {
		m.mu.Lock()
		if m.ruleStates[ruleIdx].vars == nil {
			m.ruleStates[ruleIdx].vars = map[string]interface{}{}
		}
		for k, v := range captured {
			m.ruleStates[ruleIdx].vars[k] = v
		}
		m.mu.Unlock()
	}
}

// Readings returns the most recent sensor readings plus, per rule, a
// "<key>_triggered" boolean indicating whether the rule is currently firing.
func (m *sensorMonitor) Readings(ctx context.Context, extra map[string]interface{}) (map[string]interface{}, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := make(map[string]interface{}, len(m.lastReadings)+len(m.cfg.Rules))
	for k, v := range m.lastReadings {
		out[k] = v
	}
	for i := range m.cfg.Rules {
		out[m.cfg.Rules[i].Key+"_triggered"] = m.ruleStates[i].triggered
	}
	return out, nil
}

// DoCommand supports {"check": true} to force an immediate poll.
func (m *sensorMonitor) DoCommand(ctx context.Context, cmd map[string]interface{}) (map[string]interface{}, error) {
	if check, ok := cmd["check"]; ok {
		if b, ok := check.(bool); ok && b {
			m.poll(ctx)
			return map[string]interface{}{"check": "ok"}, nil
		}
	}
	return nil, fmt.Errorf("unsupported command: expected {%q: true}", "check")
}

func (m *sensorMonitor) Close(context.Context) error {
	m.cancelFunc()
	m.wg.Wait()
	return nil
}

// parseOperator maps an operator string to a comparison function. It accepts both
// symbolic forms (">", ">=", ...) and word aliases (gt, gte, ...).
func parseOperator(op string) (func(a, b float64) bool, error) {
	switch strings.ToLower(strings.TrimSpace(op)) {
	case ">", "gt":
		return func(a, b float64) bool { return a > b }, nil
	case ">=", "gte":
		return func(a, b float64) bool { return a >= b }, nil
	case "<", "lt":
		return func(a, b float64) bool { return a < b }, nil
	case "<=", "lte":
		return func(a, b float64) bool { return a <= b }, nil
	case "==", "eq":
		return func(a, b float64) bool { return a == b }, nil
	case "!=", "ne":
		return func(a, b float64) bool { return a != b }, nil
	default:
		return nil, fmt.Errorf("unknown operator %q", op)
	}
}

// toFloat64 coerces a JSON-decoded reading value to a float64.
func toFloat64(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	default:
		return 0, false
	}
}

// refExact matches a string that is exactly one reference, e.g. "${msg.ts}".
// refAny matches every reference embedded anywhere in a string.
var (
	refExact = regexp.MustCompile(`^\$\{([^}]+)\}$`)
	refAny   = regexp.MustCompile(`\$\{([^}]+)\}`)
)

// resolveValue walks a command value and substitutes ${name} / ${name.path}
// references against ctx. A value that is exactly one reference keeps the
// referenced value's type (so a captured map or number passes through intact); a
// reference embedded in a larger string is substituted textually. An unresolved
// reference returns an error so the caller can skip the action.
func resolveValue(v interface{}, ctx map[string]interface{}) (interface{}, error) {
	switch x := v.(type) {
	case string:
		if mm := refExact.FindStringSubmatch(x); mm != nil {
			val, ok := lookupPath(ctx, mm[1])
			if !ok {
				return nil, fmt.Errorf("unresolved reference ${%s}", mm[1])
			}
			return val, nil
		}
		var refErr error
		out := refAny.ReplaceAllStringFunc(x, func(match string) string {
			path := refAny.FindStringSubmatch(match)[1]
			val, ok := lookupPath(ctx, path)
			if !ok {
				refErr = fmt.Errorf("unresolved reference ${%s}", path)
				return match
			}
			return stringify(val)
		})
		if refErr != nil {
			return nil, refErr
		}
		return out, nil
	case map[string]interface{}:
		out := make(map[string]interface{}, len(x))
		for k, val := range x {
			rv, err := resolveValue(val, ctx)
			if err != nil {
				return nil, err
			}
			out[k] = rv
		}
		return out, nil
	case []interface{}:
		out := make([]interface{}, len(x))
		for i, e := range x {
			rv, err := resolveValue(e, ctx)
			if err != nil {
				return nil, err
			}
			out[i] = rv
		}
		return out, nil
	default:
		return v, nil
	}
}

// lookupPath walks a dot-separated path into a context map.
func lookupPath(ctx map[string]interface{}, path string) (interface{}, bool) {
	var cur interface{} = ctx
	for _, part := range strings.Split(path, ".") {
		m, ok := cur.(map[string]interface{})
		if !ok {
			return nil, false
		}
		cur, ok = m[part]
		if !ok {
			return nil, false
		}
	}
	return cur, true
}

// stringify renders a referenced value for embedding in a larger string,
// trimming trailing zeros from floats.
func stringify(v interface{}) string {
	if f, ok := v.(float64); ok {
		return strconv.FormatFloat(f, 'f', -1, 64)
	}
	return fmt.Sprint(v)
}
