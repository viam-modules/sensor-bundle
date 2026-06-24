// Package sensormonitor implements the viam:sensor-bundle:sensor-monitor model: a
// sensor that watches another sensor's readings against numeric trigger rules and
// sends a notification via a generic service's DoCommand when a threshold is crossed.
package sensormonitor

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	sensor "go.viam.com/rdk/components/sensor"
	"go.viam.com/rdk/logging"
	"go.viam.com/rdk/resource"
	generic "go.viam.com/rdk/services/generic"
)

// Model is the sensor-monitor model triplet. It watches the readings of a sensor
// and fires notifications through a generic service when a numeric reading crosses
// a configured threshold.
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

// Rule describes a single numeric trigger on one reading key.
type Rule struct {
	// Key is the reading key to watch, e.g. "temperature".
	Key string `json:"key"`
	// Operator is the comparison to apply between the reading value and Threshold.
	// Supported: ">", ">=", "<", "<=", "==", "!=" (aliases: gt, gte, lt, lte, eq, ne).
	Operator string `json:"operator"`
	// Threshold is the value the reading is compared against.
	Threshold float64 `json:"threshold"`
	// Message is an optional notification template. It supports the placeholders
	// {key}, {value}, {threshold} and {operator}. If empty, a message is generated.
	Message string `json:"message,omitempty"`
	// ResolveReaction is the emoji name (without colons, e.g. "white_check_mark")
	// to add as a reaction to this rule's alert message when the rule clears (its
	// reading returns to the non-triggered side of the threshold). When unset, no
	// reaction is added. Requires the notifier to support a {"command": "react",
	// ...} DoCommand and to have returned a message "ts" on send (the Slack
	// bot-token path).
	ResolveReaction string `json:"resolve_reaction,omitempty"`
}

// Config is the configuration for the sensor-monitor model.
type Config struct {
	// Sensor is the name of the sensor dependency whose readings are monitored.
	Sensor string `json:"sensor"`
	// Notifier is the name of the generic service dependency that receives
	// notification DoCommands of the form {"command": "send", "text": <message>}.
	Notifier string `json:"notifier"`
	// Rules is the set of numeric trigger rules. At least one is required.
	Rules []Rule `json:"rules"`
	// PollIntervalSec is how often the sensor is polled, in seconds. Defaults to 10.
	PollIntervalSec float64 `json:"poll_interval_seconds,omitempty"`
	// CooldownSec is the minimum time between repeat notifications while a rule
	// stays triggered, in seconds. 0 (default) means do not re-notify until the
	// rule clears and fires again.
	CooldownSec float64 `json:"cooldown_seconds,omitempty"`
}

// Validate checks the config and returns the required dependency names.
func (cfg *Config) Validate(path string) ([]string, []string, error) {
	if cfg.Sensor == "" {
		return nil, nil, fmt.Errorf("%s: missing required field 'sensor'", path)
	}
	if cfg.Notifier == "" {
		return nil, nil, fmt.Errorf("%s: missing required field 'notifier'", path)
	}
	if len(cfg.Rules) == 0 {
		return nil, nil, fmt.Errorf("%s: at least one rule is required", path)
	}
	for i, r := range cfg.Rules {
		if r.Key == "" {
			return nil, nil, fmt.Errorf("%s: rules[%d] missing required field 'key'", path, i)
		}
		if _, err := parseOperator(r.Operator); err != nil {
			return nil, nil, fmt.Errorf("%s: rules[%d] %w", path, i, err)
		}
	}
	return []string{cfg.Sensor, cfg.Notifier}, nil, nil
}

// ruleState tracks the runtime state of a single rule across polls.
type ruleState struct {
	triggered    bool
	lastNotified time.Time
	lastValue    float64
	// msgChannel and msgTS identify the most recent alert message sent for this
	// rule, captured from the notifier's send response so the message can be
	// reacted to when the rule clears. Empty when no alert is outstanding or the
	// notifier did not return a ts (e.g. the Slack webhook path).
	msgChannel string
	msgTS      string
}

type sensorMonitor struct {
	resource.Named
	resource.AlwaysRebuild

	logger logging.Logger
	cfg    *Config

	sensorDep   sensor.Sensor
	notifierDep generic.Service

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
	notifierDep, err := generic.FromProvider(deps, conf.Notifier)
	if err != nil {
		return nil, fmt.Errorf("failed to get notifier dependency %q: %w", conf.Notifier, err)
	}

	pollInterval := defaultPollInterval
	if conf.PollIntervalSec > 0 {
		pollInterval = time.Duration(conf.PollIntervalSec * float64(time.Second))
	}
	cooldown := time.Duration(conf.CooldownSec * float64(time.Second))

	cancelCtx, cancelFunc := context.WithCancel(context.Background())

	return &sensorMonitor{
		Named:        name.AsNamed(),
		logger:       logger,
		cfg:          conf,
		sensorDep:    sensorDep,
		notifierDep:  notifierDep,
		pollInterval: pollInterval,
		cooldown:     cooldown,
		cancelCtx:    cancelCtx,
		cancelFunc:   cancelFunc,
		lastReadings: map[string]interface{}{},
		ruleStates:   make([]ruleState, len(conf.Rules)),
	}, nil
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

// poll reads the sensor once, evaluates every rule, and sends notifications.
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

		notify := false
		resolved := false
		var reactChannel, reactTS string
		m.mu.Lock()
		st := &m.ruleStates[i]
		st.lastValue = value
		switch {
		case fired && !st.triggered:
			st.triggered = true
			st.lastNotified = now
			notify = true
		case fired && st.triggered && m.cooldown > 0 && now.Sub(st.lastNotified) >= m.cooldown:
			st.lastNotified = now
			notify = true
		case !fired:
			// Edge from triggered to cleared: the rule just resolved. When a resolve
			// reaction is configured and we have a message to react to, capture it
			// now and clear the stored identity so we react exactly once.
			if st.triggered && st.msgTS != "" && rule.ResolveReaction != "" {
				resolved = true
				reactChannel = st.msgChannel
				reactTS = st.msgTS
			}
			st.triggered = false
			st.msgChannel = ""
			st.msgTS = ""
		}
		m.mu.Unlock()

		if notify {
			channel, ts := m.sendNotification(ctx, rule, value)
			// Record the message so it can be reacted to on resolve. A re-notify
			// (cooldown) overwrites the previous identity, so we react to the most
			// recent alert. Empty ts (webhook path, or send failure) leaves nothing
			// to react to.
			if ts != "" {
				m.mu.Lock()
				m.ruleStates[i].msgChannel = channel
				m.ruleStates[i].msgTS = ts
				m.mu.Unlock()
			}
		}
		if resolved {
			m.sendResolveReaction(ctx, rule, reactChannel, reactTS)
		}
	}
}

// sendNotification renders the rule's message and calls DoCommand on the
// notifier, returning the message's channel and ts when the notifier reports
// them (the Slack bot-token path). Both are empty on failure or when the
// notifier does not return them.
func (m *sensorMonitor) sendNotification(ctx context.Context, rule Rule, value float64) (channel, ts string) {
	msg := renderMessage(rule, value)
	cmd := map[string]interface{}{
		"command": "send",
		"text":    msg,
	}
	resp, err := m.notifierDep.DoCommand(ctx, cmd)
	if err != nil {
		m.logger.Errorf("failed to notify via %q: %v", m.cfg.Notifier, err)
		return "", ""
	}
	channel, _ = resp["channel"].(string)
	ts, _ = resp["ts"].(string)
	m.logger.Infof("notification sent: %s", msg)
	return channel, ts
}

// sendResolveReaction asks the notifier to add the rule's resolve_reaction emoji
// to a previously-sent alert message once the rule has cleared. Only called when
// the rule's resolve_reaction is set. Best-effort: a failure (e.g. a notifier
// that doesn't support "react", or a webhook notifier) is logged and never
// affects monitoring.
func (m *sensorMonitor) sendResolveReaction(ctx context.Context, rule Rule, channel, ts string) {
	cmd := map[string]interface{}{
		"command":   "react",
		"name":      rule.ResolveReaction,
		"channel":   channel,
		"timestamp": ts,
	}
	if _, err := m.notifierDep.DoCommand(ctx, cmd); err != nil {
		m.logger.Warnf("failed to react to resolved alert for %q via %q: %v", rule.Key, m.cfg.Notifier, err)
		return
	}
	m.logger.Infof("reacted :%s: to resolved alert for %q", rule.ResolveReaction, rule.Key)
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

// renderMessage builds the notification text for a fired rule.
func renderMessage(rule Rule, value float64) string {
	if rule.Message == "" {
		return fmt.Sprintf("%s is %s (%s %s)",
			rule.Key,
			formatFloat(value),
			rule.Operator,
			formatFloat(rule.Threshold),
		)
	}
	r := strings.NewReplacer(
		"{key}", rule.Key,
		"{value}", formatFloat(value),
		"{threshold}", formatFloat(rule.Threshold),
		"{operator}", rule.Operator,
	)
	return r.Replace(rule.Message)
}

// formatFloat renders a float without a trailing ".0" for whole numbers.
func formatFloat(f float64) string {
	return strconv.FormatFloat(f, 'f', -1, 64)
}
