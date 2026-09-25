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
	// Command is the DoCommand payload. String values may contain {{...}}
	// references that are resolved before the command is sent — see resolveValue.
	Command map[string]interface{} `json:"command"`
	// Capture, when set, stores this action's response under this name so later
	// actions can reference its fields as {{name.field}}.
	Capture string `json:"capture,omitempty"`
}

// Rule describes a single numeric trigger on one reading key.
type Rule struct {
	// Name optionally identifies the rule so it can be targeted by the "snooze"
	// DoCommand. Names, when set, must be unique across rules.
	Name string `json:"name,omitempty"`
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
	// SnoozeWindows are recurring windows during which this rule is not evaluated,
	// in addition to the monitor-wide Config.SnoozeWindows.
	SnoozeWindows []TimeWindow `json:"snooze_windows,omitempty"`
	// ActiveWindows, when set, restrict this rule to being evaluated only inside
	// one of these windows. They narrow, never widen, Config.ActiveWindows.
	ActiveWindows []TimeWindow `json:"active_windows,omitempty"`
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
	// SnoozeWindows are recurring windows during which no rule is evaluated, e.g.
	// a nightly maintenance slot. They take precedence over ActiveWindows.
	SnoozeWindows []TimeWindow `json:"snooze_windows,omitempty"`
	// ActiveWindows, when set, restrict every rule to being evaluated only inside
	// one of these windows, e.g. business hours.
	ActiveWindows []TimeWindow `json:"active_windows,omitempty"`
	// Timezone is the IANA time zone (e.g. "America/New_York") snooze and active
	// windows are interpreted in. Defaults to the machine's local time zone.
	Timezone string `json:"timezone,omitempty"`
}

// Validate checks the config and returns the required dependency names.
func (cfg *Config) Validate(path string) ([]string, []string, error) {
	if cfg.Sensor == "" {
		return nil, nil, fmt.Errorf("%s: missing required field 'sensor'", path)
	}
	if len(cfg.Rules) == 0 {
		return nil, nil, fmt.Errorf("%s: at least one rule is required", path)
	}
	if _, err := loadLocation(cfg.Timezone); err != nil {
		return nil, nil, fmt.Errorf("%s: %w", path, err)
	}
	if _, err := buildSchedules(cfg); err != nil {
		return nil, nil, fmt.Errorf("%s: %w", path, err)
	}

	deps := []string{cfg.Sensor}
	seen := map[string]bool{cfg.Sensor: true}
	addDep := func(name string) {
		if !seen[name] {
			seen[name] = true
			deps = append(deps, name)
		}
	}

	seenNames := map[string]bool{}
	for i, r := range cfg.Rules {
		if r.Key == "" {
			return nil, nil, fmt.Errorf("%s: rules[%d] missing required field 'key'", path, i)
		}
		if r.Name != "" {
			if seenNames[r.Name] {
				return nil, nil, fmt.Errorf("%s: rules[%d] duplicate rule name %q", path, i, r.Name)
			}
			seenNames[r.Name] = true
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
	// snoozeUntil suppresses evaluation of this rule until this time. The zero value
	// means not snoozed.
	snoozeUntil time.Time
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

	// loc is the time zone windows are evaluated in. schedules[i] gates rule i.
	loc       *time.Location
	schedules []schedule
	// now returns the current time; overridden in tests.
	now func() time.Time

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

	loc, err := loadLocation(conf.Timezone)
	if err != nil {
		return nil, err
	}
	schedules, err := buildSchedules(conf)
	if err != nil {
		return nil, err
	}

	cancelCtx, cancelFunc := context.WithCancel(context.Background())

	return &sensorMonitor{
		Named:           name.AsNamed(),
		logger:          logger,
		cfg:             conf,
		sensorDep:       sensorDep,
		actionResources: actionResources,
		pollInterval:    pollInterval,
		cooldown:        cooldown,
		loc:             loc,
		schedules:       schedules,
		now:             time.Now,
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

	now := m.now()
	for i := range m.cfg.Rules {
		rule := m.cfg.Rules[i]

		// Skip snoozed rules (by DoCommand or time window). Readings stay current
		// (updated above) but the rule is not evaluated, so no actions fire and its
		// state is frozen — a condition still breaching when the snooze ends fires
		// on the next poll.
		m.mu.RLock()
		snoozed := m.isSnoozed(i, now)
		m.mu.RUnlock()
		if snoozed {
			continue
		}

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

// isSnoozed reports whether rule i is suppressed at now, either by a "snooze"
// DoCommand or by its snooze/active windows. Callers must hold m.mu.
func (m *sensorMonitor) isSnoozed(i int, now time.Time) bool {
	return now.Before(m.ruleStates[i].snoozeUntil) || m.schedules[i].muted(now.In(m.loc))
}

// Readings returns the most recent sensor readings plus, per rule, a
// "<key>_triggered" boolean indicating whether the rule is currently firing and a
// "<key>_snoozed" boolean indicating whether the rule's evaluation is currently
// suppressed by a snooze DoCommand or its snooze/active windows.
func (m *sensorMonitor) Readings(ctx context.Context, extra map[string]interface{}) (map[string]interface{}, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	now := m.now()
	out := make(map[string]interface{}, len(m.lastReadings)+2*len(m.cfg.Rules))
	for k, v := range m.lastReadings {
		out[k] = v
	}
	for i := range m.cfg.Rules {
		out[m.cfg.Rules[i].Key+"_triggered"] = m.ruleStates[i].triggered
		out[m.cfg.Rules[i].Key+"_snoozed"] = m.isSnoozed(i, now)
	}
	return out, nil
}

// DoCommand supports:
//   - {"check": true} to force an immediate poll.
//   - {"snooze": {"rule_name": <name>, "duration": <duration>}} to suppress
//     evaluation of the named rule for the given duration, e.g. "30s", "90m",
//     "24h", "5d". Returns {"rule_name": <name>, "snoozed_until": <RFC3339 time>}.
func (m *sensorMonitor) DoCommand(ctx context.Context, cmd map[string]interface{}) (map[string]interface{}, error) {
	if check, ok := cmd["check"]; ok {
		if b, ok := check.(bool); ok && b {
			m.poll(ctx)
			return map[string]interface{}{"check": "ok"}, nil
		}
	}
	if raw, ok := cmd["snooze"]; ok {
		return m.snooze(raw)
	}
	return nil, fmt.Errorf("unsupported command: expected {%q: true} or {%q: {...}}", "check", "snooze")
}

// snooze handles the "snooze" DoCommand, suppressing evaluation of a single named
// rule for a duration. The payload is {"rule_name": <name>, "duration": <duration>}.
func (m *sensorMonitor) snooze(raw interface{}) (map[string]interface{}, error) {
	args, ok := raw.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("snooze: expected an object {\"rule_name\": <string>, \"duration\": <string>}, got %T", raw)
	}
	ruleName, ok := args["rule_name"].(string)
	if !ok || ruleName == "" {
		return nil, fmt.Errorf("snooze: missing or invalid \"rule_name\" (want a non-empty string)")
	}
	durStr, ok := args["duration"].(string)
	if !ok {
		return nil, fmt.Errorf("snooze: missing or invalid \"duration\" (want a string like \"30s\", \"90m\", \"24h\", \"5d\")")
	}
	d, err := parseSnoozeDuration(durStr)
	if err != nil {
		return nil, fmt.Errorf("snooze: %w", err)
	}

	idx := -1
	for i := range m.cfg.Rules {
		if m.cfg.Rules[i].Name == ruleName {
			idx = i
			break
		}
	}
	if idx < 0 {
		return nil, fmt.Errorf("snooze: no rule named %q", ruleName)
	}

	until := m.now().Add(d)
	m.mu.Lock()
	m.ruleStates[idx].snoozeUntil = until
	m.mu.Unlock()
	m.logger.Infof("snoozing rule %q until %s", ruleName, until.Format(time.RFC3339))
	return map[string]interface{}{"rule_name": ruleName, "snoozed_until": until.Format(time.RFC3339)}, nil
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

// snoozeDurationRe matches a duration like "30s", "90m", "24h", or "5d": a
// non-negative number followed by a unit of seconds, minutes, hours, or days.
var snoozeDurationRe = regexp.MustCompile(`^(\d+(?:\.\d+)?)\s*([smhd])$`)

// parseSnoozeDuration parses a snooze duration string. Unlike time.ParseDuration
// it accepts a "d" (day) unit — and only the units s, m, h, d — matching the
// coarse granularity a snooze needs, e.g. "30s", "90m", "24h", "5d".
func parseSnoozeDuration(s string) (time.Duration, error) {
	mm := snoozeDurationRe.FindStringSubmatch(strings.ToLower(strings.TrimSpace(s)))
	if mm == nil {
		return 0, fmt.Errorf("invalid duration %q: want a number followed by s, m, h, or d (e.g. \"30s\", \"90m\", \"24h\", \"5d\")", s)
	}
	n, err := strconv.ParseFloat(mm[1], 64)
	if err != nil {
		return 0, fmt.Errorf("invalid duration %q: %w", s, err)
	}
	var unit time.Duration
	switch mm[2] {
	case "s":
		unit = time.Second
	case "m":
		unit = time.Minute
	case "h":
		unit = time.Hour
	case "d":
		unit = 24 * time.Hour
	}
	return time.Duration(n * float64(unit)), nil
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

// refExact matches a string that is exactly one reference, e.g. "{{msg.ts}}".
// refAny matches every reference embedded anywhere in a string.
var (
	refExact = regexp.MustCompile(`^\{\{([^{}]+)\}\}$`)
	refAny   = regexp.MustCompile(`\{\{([^{}]+)\}\}`)
)

// resolveValue walks a command value and substitutes {{name}} / {{name.path}}
// references against ctx. A value that is exactly one reference keeps the
// referenced value's type (so a captured map or number passes through intact); a
// reference embedded in a larger string is substituted textually. An unresolved
// reference returns an error so the caller can skip the action.
func resolveValue(v interface{}, ctx map[string]interface{}) (interface{}, error) {
	switch x := v.(type) {
	case string:
		if mm := refExact.FindStringSubmatch(x); mm != nil {
			path := strings.TrimSpace(mm[1])
			val, ok := lookupPath(ctx, path)
			if !ok {
				return nil, fmt.Errorf("unresolved reference {{%s}}", path)
			}
			return val, nil
		}
		var refErr error
		out := refAny.ReplaceAllStringFunc(x, func(match string) string {
			path := strings.TrimSpace(refAny.FindStringSubmatch(match)[1])
			val, ok := lookupPath(ctx, path)
			if !ok {
				refErr = fmt.Errorf("unresolved reference {{%s}}", path)
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
