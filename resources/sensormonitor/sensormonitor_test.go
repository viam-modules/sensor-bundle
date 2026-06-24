package sensormonitor

import (
	"context"
	"sync"
	"testing"
	"time"

	sensor "go.viam.com/rdk/components/sensor"
	"go.viam.com/rdk/logging"
	"go.viam.com/rdk/resource"
	generic "go.viam.com/rdk/services/generic"
)

// fakeSensor is a settable sensor.Sensor used to drive the monitor in tests.
type fakeSensor struct {
	resource.Named
	resource.AlwaysRebuild

	mu       sync.Mutex
	readings map[string]interface{}
}

func newFakeSensor(name string) *fakeSensor {
	return &fakeSensor{
		Named:    sensor.Named(name).AsNamed(),
		readings: map[string]interface{}{},
	}
}

func (f *fakeSensor) set(readings map[string]interface{}) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.readings = readings
}

func (f *fakeSensor) Readings(ctx context.Context, extra map[string]interface{}) (map[string]interface{}, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string]interface{}, len(f.readings))
	for k, v := range f.readings {
		out[k] = v
	}
	return out, nil
}

func (f *fakeSensor) Close(context.Context) error { return nil }

// fakeNotifier is a generic.Service that records the DoCommands it receives.
// On a "send" it returns a synthetic ts/channel (the Slack bot-token shape) so
// the monitor can capture a message identity to react to; an empty sendTS
// simulates the webhook path that returns no ts.
type fakeNotifier struct {
	resource.Named
	resource.AlwaysRebuild

	mu       sync.Mutex
	received []map[string]interface{}
	sendTS   string
	sendCh   string
}

func newFakeNotifier(name string) *fakeNotifier {
	return &fakeNotifier{Named: generic.Named(name).AsNamed(), sendTS: "100.1", sendCh: "C0ALERTS"}
}

func (f *fakeNotifier) DoCommand(ctx context.Context, cmd map[string]interface{}) (map[string]interface{}, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.received = append(f.received, cmd)
	if cmd["command"] == "send" && f.sendTS != "" {
		return map[string]interface{}{"ok": true, "ts": f.sendTS, "channel": f.sendCh}, nil
	}
	return map[string]interface{}{"ok": true}, nil
}

func (f *fakeNotifier) Close(context.Context) error { return nil }

// reactions returns the "react" DoCommands received so far.
func (f *fakeNotifier) reactions() []map[string]interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]map[string]interface{}, 0)
	for _, cmd := range f.received {
		if cmd["command"] == "react" {
			out = append(out, cmd)
		}
	}
	return out
}

// texts returns the notification message strings received so far.
func (f *fakeNotifier) texts() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, 0, len(f.received))
	for _, cmd := range f.received {
		if cmd["command"] != "send" {
			continue
		}
		if text, ok := cmd["text"].(string); ok {
			out = append(out, text)
		}
	}
	return out
}

// newTestMonitor builds a monitor wired to the given fakes WITHOUT starting the
// background polling loop, so tests drive poll() deterministically.
func newTestMonitor(t *testing.T, cfg *Config, src *fakeSensor, notifier *fakeNotifier) *sensorMonitor {
	t.Helper()
	deps := resource.Dependencies{
		sensor.Named(cfg.Sensor):    src,
		generic.Named(cfg.Notifier): notifier,
	}
	name := resource.NewName(sensor.API, "monitor")
	m, err := newMonitor(deps, name, cfg, logging.NewTestLogger(t))
	if err != nil {
		t.Fatalf("newMonitor: %v", err)
	}
	t.Cleanup(func() { _ = m.Close(context.Background()) })
	return m
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
		reqDeps []string
	}{
		{
			name:    "valid",
			cfg:     Config{Sensor: "s", Notifier: "n", Rules: []Rule{{Key: "t", Operator: ">", Threshold: 1}}},
			reqDeps: []string{"s", "n"},
		},
		{
			name:    "missing sensor",
			cfg:     Config{Notifier: "n", Rules: []Rule{{Key: "t", Operator: ">", Threshold: 1}}},
			wantErr: true,
		},
		{
			name:    "missing notifier",
			cfg:     Config{Sensor: "s", Rules: []Rule{{Key: "t", Operator: ">", Threshold: 1}}},
			wantErr: true,
		},
		{
			name:    "no rules",
			cfg:     Config{Sensor: "s", Notifier: "n"},
			wantErr: true,
		},
		{
			name:    "rule missing key",
			cfg:     Config{Sensor: "s", Notifier: "n", Rules: []Rule{{Operator: ">", Threshold: 1}}},
			wantErr: true,
		},
		{
			name:    "rule bad operator",
			cfg:     Config{Sensor: "s", Notifier: "n", Rules: []Rule{{Key: "t", Operator: "=>", Threshold: 1}}},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, opt, err := tt.cfg.Validate("path")
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if opt != nil {
				t.Fatalf("expected no optional deps, got %v", opt)
			}
			if len(req) != len(tt.reqDeps) {
				t.Fatalf("required deps = %v, want %v", req, tt.reqDeps)
			}
			for i := range req {
				if req[i] != tt.reqDeps[i] {
					t.Fatalf("required deps = %v, want %v", req, tt.reqDeps)
				}
			}
		})
	}
}

func TestParseOperator(t *testing.T) {
	tests := []struct {
		op      string
		a, b    float64
		want    bool
		wantErr bool
	}{
		{op: ">", a: 2, b: 1, want: true},
		{op: "gt", a: 1, b: 1, want: false},
		{op: ">=", a: 1, b: 1, want: true},
		{op: "<", a: 1, b: 2, want: true},
		{op: "lte", a: 2, b: 2, want: true},
		{op: "==", a: 2, b: 2, want: true},
		{op: "!=", a: 2, b: 3, want: true},
		{op: "??", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.op, func(t *testing.T) {
			cmp, err := parseOperator(tt.op)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got := cmp(tt.a, tt.b); got != tt.want {
				t.Fatalf("%v %s %v = %v, want %v", tt.a, tt.op, tt.b, got, tt.want)
			}
		})
	}
}

func TestToFloat64(t *testing.T) {
	cases := map[string]struct {
		in   interface{}
		want float64
		ok   bool
	}{
		"float64": {in: float64(1.5), want: 1.5, ok: true},
		"int":     {in: int(3), want: 3, ok: true},
		"int64":   {in: int64(4), want: 4, ok: true},
		"string":  {in: "nope", ok: false},
		"bool":    {in: true, ok: false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got, ok := toFloat64(tc.in)
			if ok != tc.ok || (ok && got != tc.want) {
				t.Fatalf("toFloat64(%v) = (%v, %v), want (%v, %v)", tc.in, got, ok, tc.want, tc.ok)
			}
		})
	}
}

func TestRenderMessage(t *testing.T) {
	value := 95.0
	def := renderMessage(Rule{Key: "temperature", Operator: ">", Threshold: 90}, value)
	if def != "temperature is 95 (> 90)" {
		t.Fatalf("default message = %q", def)
	}
	custom := renderMessage(Rule{Key: "temperature", Operator: ">", Threshold: 90, Message: "ALERT {key}={value} over {threshold}"}, value)
	if custom != "ALERT temperature=95 over 90" {
		t.Fatalf("custom message = %q", custom)
	}
}

func TestEdgeTriggeredNotifies(t *testing.T) {
	ctx := context.Background()
	src := newFakeSensor("src")
	src.set(map[string]interface{}{"temperature": 50.0})
	notifier := newFakeNotifier("notify")

	m := newTestMonitor(t, &Config{
		Sensor:   "src",
		Notifier: "notify",
		Rules:    []Rule{{Key: "temperature", Operator: ">", Threshold: 90}},
	}, src, notifier)

	// Below threshold: no notification.
	src.set(map[string]interface{}{"temperature": 50.0})
	m.poll(ctx)
	if got := len(notifier.texts()); got != 0 {
		t.Fatalf("expected 0 notifications below threshold, got %d", got)
	}

	// Cross above threshold: exactly one notification.
	src.set(map[string]interface{}{"temperature": 95.0})
	m.poll(ctx)
	if got := notifier.texts(); len(got) != 1 {
		t.Fatalf("expected 1 notification on edge, got %d: %v", len(got), got)
	}

	// Still above threshold, cooldown 0: no repeat.
	m.poll(ctx)
	m.poll(ctx)
	if got := len(notifier.texts()); got != 1 {
		t.Fatalf("expected no repeat while triggered, got %d", got)
	}

	// Clear then re-fire: a second notification.
	src.set(map[string]interface{}{"temperature": 50.0})
	m.poll(ctx)
	src.set(map[string]interface{}{"temperature": 99.0})
	m.poll(ctx)
	if got := len(notifier.texts()); got != 2 {
		t.Fatalf("expected 2 notifications after re-fire, got %d", got)
	}
}

func TestReactsOnResolve(t *testing.T) {
	ctx := context.Background()
	src := newFakeSensor("src")
	notifier := newFakeNotifier("notify")
	m := newTestMonitor(t, &Config{
		Sensor:   "src",
		Notifier: "notify",
		Rules:    []Rule{{Key: "usage", Operator: ">=", Threshold: 15}},
	}, src, notifier)

	// Cross the threshold: one alert, captured ts, no reaction yet.
	src.set(map[string]interface{}{"usage": 15.0})
	m.poll(ctx)
	if got := len(notifier.reactions()); got != 0 {
		t.Fatalf("expected no reaction while triggered, got %d", got)
	}

	// Still triggered: no reaction.
	m.poll(ctx)
	if got := len(notifier.reactions()); got != 0 {
		t.Fatalf("expected no reaction while still triggered, got %d", got)
	}

	// Counter reset below threshold (the streamdeck refill): react exactly once.
	src.set(map[string]interface{}{"usage": 0.0})
	m.poll(ctx)
	reactions := notifier.reactions()
	if len(reactions) != 1 {
		t.Fatalf("expected 1 reaction on resolve, got %d: %v", len(reactions), reactions)
	}
	r := reactions[0]
	if r["name"] != defaultResolveReaction || r["timestamp"] != "100.1" || r["channel"] != "C0ALERTS" {
		t.Fatalf("reaction payload not as expected: %v", r)
	}

	// Stays resolved: no further reactions (identity cleared).
	m.poll(ctx)
	if got := len(notifier.reactions()); got != 1 {
		t.Fatalf("expected no repeat reaction, got %d", got)
	}
}

func TestNoReactionWithoutTS(t *testing.T) {
	ctx := context.Background()
	src := newFakeSensor("src")
	notifier := newFakeNotifier("notify")
	notifier.sendTS = "" // simulate the webhook path: send returns no ts
	m := newTestMonitor(t, &Config{
		Sensor:   "src",
		Notifier: "notify",
		Rules:    []Rule{{Key: "usage", Operator: ">=", Threshold: 15}},
	}, src, notifier)

	src.set(map[string]interface{}{"usage": 20.0})
	m.poll(ctx)
	src.set(map[string]interface{}{"usage": 0.0})
	m.poll(ctx)

	if got := len(notifier.reactions()); got != 0 {
		t.Fatalf("expected no reaction when no ts was captured, got %d", got)
	}
}

func TestResolveReactionDisabled(t *testing.T) {
	ctx := context.Background()
	src := newFakeSensor("src")
	notifier := newFakeNotifier("notify")
	m := newTestMonitor(t, &Config{
		Sensor:          "src",
		Notifier:        "notify",
		ResolveReaction: "-",
		Rules:           []Rule{{Key: "usage", Operator: ">=", Threshold: 15}},
	}, src, notifier)

	src.set(map[string]interface{}{"usage": 20.0})
	m.poll(ctx)
	src.set(map[string]interface{}{"usage": 0.0})
	m.poll(ctx)

	if got := len(notifier.reactions()); got != 0 {
		t.Fatalf("expected no reaction when disabled, got %d", got)
	}
}

func TestReactsToLatestMessageAfterCooldownRenotify(t *testing.T) {
	ctx := context.Background()
	src := newFakeSensor("src")
	notifier := newFakeNotifier("notify")
	m := newTestMonitor(t, &Config{
		Sensor:      "src",
		Notifier:    "notify",
		CooldownSec: 60, // long cooldown; we backdate lastNotified to force a re-notify
		Rules:       []Rule{{Key: "usage", Operator: ">=", Threshold: 15}},
	}, src, notifier)

	src.set(map[string]interface{}{"usage": 20.0})
	m.poll(ctx) // first alert: ts 100.1

	// Backdate so the cooldown has deterministically elapsed, then re-notify.
	m.ruleStates[0].lastNotified = m.ruleStates[0].lastNotified.Add(-time.Hour)
	notifier.sendTS = "200.2" // a re-notify will return a new ts
	m.poll(ctx)               // cooldown elapsed: re-notify, ts 200.2

	src.set(map[string]interface{}{"usage": 0.0})
	m.poll(ctx) // resolve: should react to the latest message

	reactions := notifier.reactions()
	if len(reactions) != 1 || reactions[0]["timestamp"] != "200.2" {
		t.Fatalf("expected reaction to latest ts 200.2, got %v", reactions)
	}
}

func TestReadingsExposesTriggerState(t *testing.T) {
	ctx := context.Background()
	src := newFakeSensor("src")
	notifier := newFakeNotifier("notify")
	m := newTestMonitor(t, &Config{
		Sensor:   "src",
		Notifier: "notify",
		Rules:    []Rule{{Key: "humidity", Operator: "<", Threshold: 30}},
	}, src, notifier)

	src.set(map[string]interface{}{"humidity": 20.0})
	m.poll(ctx)

	got, err := m.Readings(ctx, nil)
	if err != nil {
		t.Fatalf("Readings: %v", err)
	}
	if got["humidity"] != 20.0 {
		t.Fatalf("expected humidity reading passed through, got %v", got["humidity"])
	}
	if triggered, ok := got["humidity_triggered"].(bool); !ok || !triggered {
		t.Fatalf("expected humidity_triggered=true, got %v", got["humidity_triggered"])
	}
}

func TestDoCommandCheckForcesPoll(t *testing.T) {
	ctx := context.Background()
	src := newFakeSensor("src")
	src.set(map[string]interface{}{"temperature": 95.0})
	notifier := newFakeNotifier("notify")
	m := newTestMonitor(t, &Config{
		Sensor:   "src",
		Notifier: "notify",
		Rules:    []Rule{{Key: "temperature", Operator: ">", Threshold: 90}},
	}, src, notifier)

	if _, err := m.DoCommand(ctx, map[string]interface{}{"check": true}); err != nil {
		t.Fatalf("DoCommand check: %v", err)
	}
	if got := len(notifier.texts()); got == 0 {
		t.Fatal("expected a notification after forced check")
	}

	if _, err := m.DoCommand(ctx, map[string]interface{}{"bogus": 1}); err == nil {
		t.Fatal("expected error for unknown command")
	}
}
