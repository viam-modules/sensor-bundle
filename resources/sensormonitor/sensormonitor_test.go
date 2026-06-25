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

// fakeTarget is a generic resource standing in for an action target. It records
// the DoCommands it receives and returns a configurable response, so tests can
// assert what the monitor fired and exercise the capture/reference mechanism.
type fakeTarget struct {
	resource.Named
	resource.AlwaysRebuild

	mu       sync.Mutex
	received []map[string]interface{}
	resp     map[string]interface{}
}

func newFakeTarget(name string) *fakeTarget {
	return &fakeTarget{Named: generic.Named(name).AsNamed()}
}

func (f *fakeTarget) DoCommand(ctx context.Context, cmd map[string]interface{}) (map[string]interface{}, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.received = append(f.received, cmd)
	if f.resp != nil {
		return f.resp, nil
	}
	return map[string]interface{}{}, nil
}

func (f *fakeTarget) Close(context.Context) error { return nil }

func (f *fakeTarget) commands() []map[string]interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]map[string]interface{}{}, f.received...)
}

// newTestMonitor builds a monitor wired to the given sensor and action targets
// WITHOUT starting the background polling loop, so tests drive poll()
// deterministically.
func newTestMonitor(t *testing.T, cfg *Config, src *fakeSensor, targets map[string]*fakeTarget) *sensorMonitor {
	t.Helper()
	deps := resource.Dependencies{
		sensor.Named(cfg.Sensor): src,
	}
	for name, tgt := range targets {
		deps[generic.Named(name)] = tgt
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
			cfg:     Config{Sensor: "s", Rules: []Rule{{Key: "t", Operator: ">", Threshold: 1}}},
			reqDeps: []string{"s"},
		},
		{
			name:    "missing sensor",
			cfg:     Config{Rules: []Rule{{Key: "t", Operator: ">", Threshold: 1}}},
			wantErr: true,
		},
		{
			name:    "no rules",
			cfg:     Config{Sensor: "s"},
			wantErr: true,
		},
		{
			name:    "rule missing key",
			cfg:     Config{Sensor: "s", Rules: []Rule{{Operator: ">", Threshold: 1}}},
			wantErr: true,
		},
		{
			name:    "rule bad operator",
			cfg:     Config{Sensor: "s", Rules: []Rule{{Key: "t", Operator: "=>", Threshold: 1}}},
			wantErr: true,
		},
		{
			name: "with actions",
			cfg: Config{Sensor: "s", Rules: []Rule{{Key: "t", Operator: ">", Threshold: 1,
				OnTrigger: []Action{{Resource: "r1", Command: map[string]interface{}{"x": 1}}},
				OnResolve: []Action{{Resource: "r2", Command: map[string]interface{}{"x": 0}}},
			}}},
			reqDeps: []string{"s", "r1", "r2"},
		},
		{
			name: "action missing resource",
			cfg: Config{Sensor: "s", Rules: []Rule{{Key: "t", Operator: ">", Threshold: 1,
				OnTrigger: []Action{{Command: map[string]interface{}{"x": 1}}},
			}}},
			wantErr: true,
		},
		{
			name: "action missing command",
			cfg: Config{Sensor: "s", Rules: []Rule{{Key: "t", Operator: ">", Threshold: 1,
				OnResolve: []Action{{Resource: "r1"}},
			}}},
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

func TestResolveValue(t *testing.T) {
	ctx := map[string]interface{}{
		"value":     95.0,
		"threshold": 90.0,
		"key":       "temperature",
		"msg":       map[string]interface{}{"ts": "100.1", "channel": "C1"},
	}

	t.Run("exact reference keeps type", func(t *testing.T) {
		got, err := resolveValue("{{value}}", ctx)
		if err != nil {
			t.Fatal(err)
		}
		if f, ok := got.(float64); !ok || f != 95.0 {
			t.Fatalf("expected float64 95, got %T %v", got, got)
		}
	})

	t.Run("exact reference into capture", func(t *testing.T) {
		got, err := resolveValue("{{msg.ts}}", ctx)
		if err != nil {
			t.Fatal(err)
		}
		if got != "100.1" {
			t.Fatalf("expected \"100.1\", got %v", got)
		}
	})

	t.Run("embedded references are stringified", func(t *testing.T) {
		got, err := resolveValue("temp {{value}} over {{threshold}}", ctx)
		if err != nil {
			t.Fatal(err)
		}
		if got != "temp 95 over 90" {
			t.Fatalf("got %q", got)
		}
	})

	t.Run("recurses into maps and slices", func(t *testing.T) {
		got, err := resolveValue(map[string]interface{}{
			"a": "{{value}}",
			"b": []interface{}{"{{key}}"},
		}, ctx)
		if err != nil {
			t.Fatal(err)
		}
		m := got.(map[string]interface{})
		if m["a"] != 95.0 {
			t.Fatalf("nested map not resolved: %v", m["a"])
		}
		if m["b"].([]interface{})[0] != "temperature" {
			t.Fatalf("nested slice not resolved: %v", m["b"])
		}
	})

	t.Run("unresolved reference errors", func(t *testing.T) {
		if _, err := resolveValue("{{nope.field}}", ctx); err == nil {
			t.Fatal("expected error for unresolved reference")
		}
	})
}

func TestActionsFireOnTriggerAndResolve(t *testing.T) {
	ctx := context.Background()
	src := newFakeSensor("src")
	relay := newFakeTarget("relay")
	m := newTestMonitor(t, &Config{
		Sensor: "src",
		Rules: []Rule{{Key: "usage", Operator: ">=", Threshold: 15,
			OnTrigger: []Action{{Resource: "relay", Command: map[string]interface{}{"set": true}}},
			OnResolve: []Action{{Resource: "relay", Command: map[string]interface{}{"set": false}}},
		}},
	}, src, map[string]*fakeTarget{"relay": relay})

	// Below threshold: nothing fired.
	src.set(map[string]interface{}{"usage": 5.0})
	m.poll(ctx)
	if got := len(relay.commands()); got != 0 {
		t.Fatalf("expected no actions below threshold, got %d", got)
	}

	// Cross above: on_trigger fires once.
	src.set(map[string]interface{}{"usage": 20.0})
	m.poll(ctx)
	cmds := relay.commands()
	if len(cmds) != 1 || cmds[0]["set"] != true {
		t.Fatalf("expected one on_trigger command {set:true}, got %v", cmds)
	}

	// Still triggered, cooldown 0: no refire.
	m.poll(ctx)
	if got := len(relay.commands()); got != 1 {
		t.Fatalf("expected no refire while triggered, got %d", got)
	}

	// Reading returns below threshold: on_resolve fires once.
	src.set(map[string]interface{}{"usage": 0.0})
	m.poll(ctx)
	cmds = relay.commands()
	if len(cmds) != 2 || cmds[1]["set"] != false {
		t.Fatalf("expected on_resolve command {set:false}, got %v", cmds)
	}

	// Stays resolved: no refire.
	m.poll(ctx)
	if got := len(relay.commands()); got != 2 {
		t.Fatalf("expected no refire while resolved, got %d", got)
	}
}

func TestOnTriggerRepeatsOnCooldown(t *testing.T) {
	ctx := context.Background()
	src := newFakeSensor("src")
	tgt := newFakeTarget("tgt")
	m := newTestMonitor(t, &Config{
		Sensor:      "src",
		CooldownSec: 60, // long; we backdate lastFired to force a repeat
		Rules: []Rule{{Key: "usage", Operator: ">=", Threshold: 15,
			OnTrigger: []Action{{Resource: "tgt", Command: map[string]interface{}{"ping": 1}}},
		}},
	}, src, map[string]*fakeTarget{"tgt": tgt})

	src.set(map[string]interface{}{"usage": 20.0})
	m.poll(ctx) // edge fire
	if got := len(tgt.commands()); got != 1 {
		t.Fatalf("expected 1 fire on edge, got %d", got)
	}

	// Backdate so the cooldown has elapsed; next poll re-fires.
	m.ruleStates[0].lastFired = m.ruleStates[0].lastFired.Add(-time.Hour)
	m.poll(ctx)
	if got := len(tgt.commands()); got != 2 {
		t.Fatalf("expected a cooldown repeat, got %d", got)
	}
}

func TestCaptureCarriesAcrossTriggerAndResolve(t *testing.T) {
	ctx := context.Background()
	src := newFakeSensor("src")
	// The notifier returns a message identity on send; the monitor must carry it
	// to the react command on resolve.
	notifier := newFakeTarget("notifier")
	notifier.resp = map[string]interface{}{"ok": true, "ts": "100.1", "channel": "C1"}
	m := newTestMonitor(t, &Config{
		Sensor: "src",
		Rules: []Rule{{Key: "usage", Operator: ">=", Threshold: 15,
			OnTrigger: []Action{{
				Resource: "notifier",
				Command:  map[string]interface{}{"command": "send", "text": "low ({{value}})"},
				Capture:  "msg",
			}},
			OnResolve: []Action{{
				Resource: "notifier",
				Command: map[string]interface{}{
					"command": "react", "name": "white_check_mark",
					"ts": "{{msg.ts}}", "channel": "{{msg.channel}}",
				},
			}},
		}},
	}, src, map[string]*fakeTarget{"notifier": notifier})

	src.set(map[string]interface{}{"usage": 20.0})
	m.poll(ctx)
	src.set(map[string]interface{}{"usage": 0.0})
	m.poll(ctx)

	cmds := notifier.commands()
	if len(cmds) != 2 {
		t.Fatalf("expected send then react, got %v", cmds)
	}
	if cmds[0]["command"] != "send" || cmds[0]["text"] != "low (20)" {
		t.Fatalf("send command not as expected: %v", cmds[0])
	}
	react := cmds[1]
	if react["command"] != "react" || react["name"] != "white_check_mark" ||
		react["ts"] != "100.1" || react["channel"] != "C1" {
		t.Fatalf("react command did not carry captured message: %v", react)
	}
}

func TestResolveActionSkippedWhenCaptureMissing(t *testing.T) {
	ctx := context.Background()
	src := newFakeSensor("src")
	notifier := newFakeTarget("notifier")
	// on_trigger does NOT capture, so the on_resolve reference can't resolve.
	m := newTestMonitor(t, &Config{
		Sensor: "src",
		Rules: []Rule{{Key: "usage", Operator: ">=", Threshold: 15,
			OnTrigger: []Action{{Resource: "notifier", Command: map[string]interface{}{"command": "send"}}},
			OnResolve: []Action{{Resource: "notifier", Command: map[string]interface{}{"command": "react", "ts": "{{msg.ts}}"}}},
		}},
	}, src, map[string]*fakeTarget{"notifier": notifier})

	src.set(map[string]interface{}{"usage": 20.0})
	m.poll(ctx)
	src.set(map[string]interface{}{"usage": 0.0})
	m.poll(ctx)

	cmds := notifier.commands()
	if len(cmds) != 1 || cmds[0]["command"] != "send" {
		t.Fatalf("expected only the send (react skipped, unresolved ref), got %v", cmds)
	}
}

func TestActionResourceMissingFromDeps(t *testing.T) {
	src := newFakeSensor("src")
	deps := resource.Dependencies{sensor.Named("src"): src}
	cfg := &Config{
		Sensor: "src",
		Rules: []Rule{{Key: "usage", Operator: ">=", Threshold: 15,
			OnTrigger: []Action{{Resource: "ghost", Command: map[string]interface{}{"x": 1}}},
		}},
	}
	if _, err := newMonitor(deps, resource.NewName(sensor.API, "monitor"), cfg, logging.NewTestLogger(t)); err == nil {
		t.Fatal("expected construction to fail when an action resource is not a dependency")
	}
}

func TestReadingsExposesTriggerState(t *testing.T) {
	ctx := context.Background()
	src := newFakeSensor("src")
	m := newTestMonitor(t, &Config{
		Sensor: "src",
		Rules:  []Rule{{Key: "humidity", Operator: "<", Threshold: 30}},
	}, src, nil)

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
	tgt := newFakeTarget("tgt")
	m := newTestMonitor(t, &Config{
		Sensor: "src",
		Rules: []Rule{{Key: "temperature", Operator: ">", Threshold: 90,
			OnTrigger: []Action{{Resource: "tgt", Command: map[string]interface{}{"go": 1}}},
		}},
	}, src, map[string]*fakeTarget{"tgt": tgt})

	if _, err := m.DoCommand(ctx, map[string]interface{}{"check": true}); err != nil {
		t.Fatalf("DoCommand check: %v", err)
	}
	if got := len(tgt.commands()); got == 0 {
		t.Fatal("expected an action to fire after forced check")
	}

	if _, err := m.DoCommand(ctx, map[string]interface{}{"bogus": 1}); err == nil {
		t.Fatal("expected error for unknown command")
	}
}
