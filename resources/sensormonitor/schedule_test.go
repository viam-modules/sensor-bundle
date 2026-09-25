package sensormonitor

import (
	"context"
	"testing"
	"time"
)

// Sep 21 2026 is a Monday.
func at(t *testing.T, loc *time.Location, day, hour, minute int) time.Time {
	t.Helper()
	return time.Date(2026, time.September, day, hour, minute, 0, 0, loc)
}

func TestParseWindowInvalid(t *testing.T) {
	cases := map[string]TimeWindow{
		"bad day":           {Days: []string{"funday"}},
		"bad range start":   {Days: []string{"xyz-fri"}},
		"bad range end":     {Days: []string{"mon-"}},
		"bad start":         {Start: "9am"},
		"bad minutes":       {Start: "09:60"},
		"single-digit mins": {Start: "09:5"},
		"hour out of range": {End: "25:00"},
		"24:30":             {End: "24:30"},
		"start 24:00":       {Start: "24:00"},
		"start equals end":  {Start: "09:00", End: "09:00"},
	}
	for name, tw := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := parseWindow(tw); err == nil {
				t.Fatal("expected error, got nil")
			}
		})
	}
}

func TestParseWindowDayRanges(t *testing.T) {
	tests := []struct {
		days []string
		want []time.Weekday
	}{
		{[]string{"mon-fri"}, []time.Weekday{time.Monday, time.Tuesday, time.Wednesday, time.Thursday, time.Friday}},
		{[]string{"Friday-Monday"}, []time.Weekday{time.Friday, time.Saturday, time.Sunday, time.Monday}},
		{[]string{"wed-wed"}, []time.Weekday{time.Wednesday}},
		{[]string{"mon", "wed-thu"}, []time.Weekday{time.Monday, time.Wednesday, time.Thursday}},
	}
	for _, tt := range tests {
		w, err := parseWindow(TimeWindow{Days: tt.days})
		if err != nil {
			t.Fatalf("%v: parseWindow: %v", tt.days, err)
		}
		var want [7]bool
		for _, d := range tt.want {
			want[d] = true
		}
		if w.days != want {
			t.Fatalf("%v: days = %v, want %v", tt.days, w.days, want)
		}
	}
}

func TestWindowContains(t *testing.T) {
	loc := time.UTC
	tests := []struct {
		name string
		tw   TimeWindow
		t    time.Time
		want bool
	}{
		{"whole day, every day", TimeWindow{}, at(t, loc, 23, 3, 0), true},
		{"whole day, listed day", TimeWindow{Days: []string{"Saturday", "sun"}}, at(t, loc, 26, 12, 0), true},
		{"whole day, unlisted day", TimeWindow{Days: []string{"sat", "sun"}}, at(t, loc, 25, 12, 0), false},
		{"same-day, at start", TimeWindow{Start: "12:00", End: "13:00"}, at(t, loc, 21, 12, 0), true},
		{"same-day, at end (exclusive)", TimeWindow{Start: "12:00", End: "13:00"}, at(t, loc, 21, 13, 0), false},
		{"end 24:00 covers 23:59", TimeWindow{Start: "18:00", End: "24:00"}, at(t, loc, 21, 23, 59), true},
		{"overnight, evening of listed day", TimeWindow{Days: []string{"fri"}, Start: "18:00", End: "09:00"}, at(t, loc, 25, 20, 0), true},
		{"overnight, next morning", TimeWindow{Days: []string{"fri"}, Start: "18:00", End: "09:00"}, at(t, loc, 26, 8, 59), true},
		{"overnight, next morning at end", TimeWindow{Days: []string{"fri"}, Start: "18:00", End: "09:00"}, at(t, loc, 26, 9, 0), false},
		{"overnight, morning of listed day", TimeWindow{Days: []string{"fri"}, Start: "18:00", End: "09:00"}, at(t, loc, 25, 8, 0), false},
		{"overnight wraps sun->mon", TimeWindow{Days: []string{"sun"}, Start: "22:00", End: "06:00"}, at(t, loc, 28, 5, 0), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w, err := parseWindow(tt.tw)
			if err != nil {
				t.Fatalf("parseWindow: %v", err)
			}
			if got := w.contains(tt.t); got != tt.want {
				t.Fatalf("contains(%s) = %v, want %v", tt.t.Format(time.RFC1123), got, tt.want)
			}
		})
	}
}

func TestScheduleMuted(t *testing.T) {
	loc := time.UTC
	businessHours := []TimeWindow{{Days: []string{"mon-fri"}, Start: "09:00", End: "17:00"}}
	cfg := &Config{
		ActiveWindows: businessHours,
		SnoozeWindows: []TimeWindow{{Days: []string{"wed"}, Start: "10:00", End: "11:00"}},
		Rules: []Rule{
			{Key: "a"},
			// Narrows the monitor's business hours to mornings.
			{Key: "b", ActiveWindows: []TimeWindow{{Start: "00:00", End: "12:00"}}},
			// Tries to widen to weekends, which the monitor's windows still exclude.
			{Key: "c", ActiveWindows: []TimeWindow{{Days: []string{"sat-sun"}}}},
		},
	}
	schedules, err := buildSchedules(cfg)
	if err != nil {
		t.Fatalf("buildSchedules: %v", err)
	}

	tests := []struct {
		name  string
		t     time.Time
		muted [3]bool
	}{
		{"monday business hours", at(t, loc, 21, 10, 0), [3]bool{false, false, true}},
		{"monday afternoon", at(t, loc, 21, 14, 0), [3]bool{false, true, true}},
		{"monday evening", at(t, loc, 21, 18, 0), [3]bool{true, true, true}},
		{"snooze beats active", at(t, loc, 23, 10, 30), [3]bool{true, true, true}},
		{"saturday", at(t, loc, 26, 10, 0), [3]bool{true, true, true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for i, s := range schedules {
				if got := s.muted(tt.t); got != tt.muted[i] {
					t.Fatalf("rule %q muted = %v, want %v", cfg.Rules[i].Key, got, tt.muted[i])
				}
			}
		})
	}
}

func TestValidateWindows(t *testing.T) {
	base := func() Config {
		return Config{Sensor: "s", Rules: []Rule{{Key: "t", Operator: ">", Threshold: 1}}}
	}

	ok := base()
	ok.Timezone = "America/New_York"
	ok.ActiveWindows = []TimeWindow{{Days: []string{"mon-fri"}, Start: "09:00", End: "17:00"}}
	ok.SnoozeWindows = []TimeWindow{{Days: []string{"sat", "sun"}}}
	ok.Rules[0].SnoozeWindows = []TimeWindow{{Start: "12:00", End: "13:00"}}
	ok.Rules[0].ActiveWindows = []TimeWindow{{End: "12:00"}}
	if _, _, err := ok.Validate("path"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	invalid := map[string]func(*Config){
		"timezone":       func(c *Config) { c.Timezone = "Mars/Olympus_Mons" },
		"monitor snooze": func(c *Config) { c.SnoozeWindows = []TimeWindow{{Start: "nope"}} },
		"monitor active": func(c *Config) { c.ActiveWindows = []TimeWindow{{Days: []string{"mon-xyz"}}} },
		"rule snooze":    func(c *Config) { c.Rules[0].SnoozeWindows = []TimeWindow{{Days: []string{"xyz"}}} },
		"rule active":    func(c *Config) { c.Rules[0].ActiveWindows = []TimeWindow{{End: "99:00"}} },
	}
	for name, mutate := range invalid {
		t.Run(name, func(t *testing.T) {
			c := base()
			mutate(&c)
			if _, _, err := c.Validate("path"); err == nil {
				t.Fatal("expected error, got nil")
			}
		})
	}
}

func TestWindowsBusinessHours(t *testing.T) {
	ctx := context.Background()
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatalf("LoadLocation: %v", err)
	}
	src := newFakeSensor("src")
	src.set(map[string]interface{}{"temperature": 95.0, "humidity": 20.0})
	hot := newFakeTarget("hot")
	dry := newFakeTarget("dry")
	m := newTestMonitor(t, &Config{
		Sensor:        "src",
		Timezone:      "America/New_York",
		ActiveWindows: []TimeWindow{{Days: []string{"mon-fri"}, Start: "09:00", End: "17:00"}},
		Rules: []Rule{
			{Key: "temperature", Operator: ">", Threshold: 90,
				OnTrigger: []Action{{Resource: "hot", Command: map[string]interface{}{"go": 1}}}},
			// This rule is also muted over lunch.
			{Key: "humidity", Operator: "<", Threshold: 30,
				SnoozeWindows: []TimeWindow{{Start: "12:00", End: "13:00"}},
				OnTrigger:     []Action{{Resource: "dry", Command: map[string]interface{}{"go": 1}}}},
		},
	}, src, map[string]*fakeTarget{"hot": hot, "dry": dry})

	var now time.Time
	m.now = func() time.Time { return now }

	// Saturday, Monday 08:59, and Tuesday 17:00 are all outside business hours.
	for _, ts := range []time.Time{at(t, loc, 26, 10, 0), at(t, loc, 21, 8, 59), at(t, loc, 22, 17, 0)} {
		now = ts
		m.poll(ctx)
		if len(hot.commands()) != 0 || len(dry.commands()) != 0 {
			t.Fatalf("expected no actions at %s", ts.Format(time.RFC1123))
		}
	}
	got, _ := m.Readings(ctx, nil)
	if snoozed, _ := got["temperature_snoozed"].(bool); !snoozed {
		t.Fatalf("expected temperature_snoozed=true outside business hours, got %v", got["temperature_snoozed"])
	}

	// Tuesday 12:30, lunch: temperature fires, humidity is muted by its own window.
	now = at(t, loc, 22, 12, 30)
	m.poll(ctx)
	if got := len(hot.commands()); got != 1 {
		t.Fatalf("expected temperature to fire during business hours, got %d", got)
	}
	if got := len(dry.commands()); got != 0 {
		t.Fatalf("expected humidity muted over lunch, got %d", got)
	}
	got, _ = m.Readings(ctx, nil)
	if snoozed, _ := got["temperature_snoozed"].(bool); snoozed {
		t.Fatalf("expected temperature_snoozed=false during business hours")
	}

	// Tuesday 13:00: the humidity condition, still breaching, fires now.
	now = at(t, loc, 22, 13, 0)
	m.poll(ctx)
	if got := len(dry.commands()); got != 1 {
		t.Fatalf("expected humidity to fire after lunch, got %d", got)
	}
}
