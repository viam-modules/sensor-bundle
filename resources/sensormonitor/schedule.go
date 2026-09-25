package sensormonitor

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	// Embed the IANA time zone database so "timezone" resolves even on machines
	// without system zoneinfo (e.g. minimal Linux images).
	_ "time/tzdata"
)

// TimeWindow is a recurring weekly time window, used both for snooze windows
// (rules are muted inside) and active windows (rules are muted outside).
type TimeWindow struct {
	// Days lists the days of the week the window starts on: single days
	// ("mon".."sun" or full names, case-insensitive) or inclusive ranges such as
	// "mon-fri" (ranges may wrap, e.g. "fri-mon"). Empty means every day.
	Days []string `json:"days,omitempty"`
	// Start is the local wall-clock time the window opens, as "HH:MM". Defaults to
	// "00:00".
	Start string `json:"start,omitempty"`
	// End is the local wall-clock time the window closes (exclusive), as "HH:MM".
	// Defaults to "24:00". An End earlier than Start makes the window run past
	// midnight into the following day.
	End string `json:"end,omitempty"`
}

const minutesPerDay = 24 * 60

// window is a parsed TimeWindow. start and end are minutes since midnight.
type window struct {
	days       [7]bool // indexed by time.Weekday
	start, end int
}

// contains reports whether t, already in the monitor's time zone, falls inside w.
func (w window) contains(t time.Time) bool {
	wd := t.Weekday()
	mins := t.Hour()*60 + t.Minute()
	if w.start < w.end {
		return w.days[wd] && mins >= w.start && mins < w.end
	}
	// Overnight: the window opens on a listed day and closes the next morning, so
	// the early-morning part belongs to the previous day's entry.
	prev := (wd + 6) % 7
	return (w.days[wd] && mins >= w.start) || (w.days[prev] && mins < w.end)
}

// inAnyWindow reports whether t falls inside any of the windows.
func inAnyWindow(windows []window, t time.Time) bool {
	for _, w := range windows {
		if w.contains(t) {
			return true
		}
	}
	return false
}

// schedule is the set of time windows that gate one rule.
type schedule struct {
	// snooze holds the monitor-wide and rule snooze windows; being inside any
	// of them mutes the rule.
	snooze []window
	// active holds one group per level (monitor, rule) that sets active_windows.
	// The rule is muted unless the time is inside some window of every group, so
	// a rule's active windows can only narrow the monitor's.
	active [][]window
}

// muted reports whether the schedule suppresses the rule at t, already in the
// monitor's time zone.
func (s schedule) muted(t time.Time) bool {
	if inAnyWindow(s.snooze, t) {
		return true
	}
	for _, group := range s.active {
		if !inAnyWindow(group, t) {
			return true
		}
	}
	return false
}

// buildSchedules parses every window in the config and returns one schedule per
// rule, in rule order.
func buildSchedules(cfg *Config) ([]schedule, error) {
	globalSnooze, err := parseWindows("snooze_windows", cfg.SnoozeWindows)
	if err != nil {
		return nil, err
	}
	globalActive, err := parseWindows("active_windows", cfg.ActiveWindows)
	if err != nil {
		return nil, err
	}

	out := make([]schedule, len(cfg.Rules))
	for i, r := range cfg.Rules {
		snooze, err := parseWindows("snooze_windows", r.SnoozeWindows)
		if err != nil {
			return nil, fmt.Errorf("rules[%d].%w", i, err)
		}
		active, err := parseWindows("active_windows", r.ActiveWindows)
		if err != nil {
			return nil, fmt.Errorf("rules[%d].%w", i, err)
		}

		s := schedule{snooze: append(append([]window{}, globalSnooze...), snooze...)}
		if len(globalActive) > 0 {
			s.active = append(s.active, globalActive)
		}
		if len(active) > 0 {
			s.active = append(s.active, active)
		}
		out[i] = s
	}
	return out, nil
}

// parseWindows parses a list of windows; field names the list in errors.
func parseWindows(field string, ws []TimeWindow) ([]window, error) {
	out := make([]window, 0, len(ws))
	for i, tw := range ws {
		w, err := parseWindow(tw)
		if err != nil {
			return nil, fmt.Errorf("%s[%d]: %w", field, i, err)
		}
		out = append(out, w)
	}
	return out, nil
}

func parseWindow(tw TimeWindow) (window, error) {
	var w window
	if len(tw.Days) == 0 {
		for i := range w.days {
			w.days[i] = true
		}
	}
	for _, d := range tw.Days {
		if err := addDays(&w.days, d); err != nil {
			return window{}, err
		}
	}

	var err error
	if w.start, err = parseClock(tw.Start, 0); err != nil {
		return window{}, fmt.Errorf("start: %w", err)
	}
	if w.end, err = parseClock(tw.End, minutesPerDay); err != nil {
		return window{}, fmt.Errorf("end: %w", err)
	}
	if w.start == minutesPerDay {
		return window{}, fmt.Errorf("start: \"24:00\" is only valid as an end time")
	}
	if w.start == w.end {
		return window{}, fmt.Errorf("start and end must differ (omit both for a whole-day window)")
	}
	return w, nil
}

// addDays marks the day or inclusive day range s (e.g. "mon", "mon-fri",
// "fri-mon") in days.
func addDays(days *[7]bool, s string) error {
	from, to, isRange := strings.Cut(s, "-")
	first, err := parseWeekday(from)
	if err != nil {
		return err
	}
	last := first
	if isRange {
		if last, err = parseWeekday(to); err != nil {
			return err
		}
	}
	for d := first; ; d = (d + 1) % 7 {
		days[d] = true
		if d == last {
			return nil
		}
	}
}

// parseClock parses "HH:MM" into minutes since midnight. "24:00" is accepted to
// mean end of day. An empty string yields def.
func parseClock(s string, def int) (int, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return def, nil
	}
	hh, mm, ok := strings.Cut(s, ":")
	if !ok || len(mm) != 2 {
		return 0, fmt.Errorf("invalid time %q: want \"HH:MM\"", s)
	}
	h, err1 := strconv.Atoi(hh)
	m, err2 := strconv.Atoi(mm)
	if err1 != nil || err2 != nil || h < 0 || m < 0 || m > 59 || h > 24 || (h == 24 && m != 0) {
		return 0, fmt.Errorf("invalid time %q: want \"HH:MM\" between 00:00 and 24:00", s)
	}
	return h*60 + m, nil
}

var weekdays = map[string]time.Weekday{
	"sun": time.Sunday, "sunday": time.Sunday,
	"mon": time.Monday, "monday": time.Monday,
	"tue": time.Tuesday, "tuesday": time.Tuesday,
	"wed": time.Wednesday, "wednesday": time.Wednesday,
	"thu": time.Thursday, "thursday": time.Thursday,
	"fri": time.Friday, "friday": time.Friday,
	"sat": time.Saturday, "saturday": time.Saturday,
}

func parseWeekday(s string) (time.Weekday, error) {
	wd, ok := weekdays[strings.ToLower(strings.TrimSpace(s))]
	if !ok {
		return 0, fmt.Errorf("invalid day %q: want mon, tue, wed, thu, fri, sat, or sun, or a range like \"mon-fri\"", s)
	}
	return wd, nil
}

// loadLocation resolves the configured time zone, defaulting to the machine's
// local zone.
func loadLocation(name string) (*time.Location, error) {
	if name == "" {
		return time.Local, nil
	}
	loc, err := time.LoadLocation(name)
	if err != nil {
		return nil, fmt.Errorf("invalid timezone %q: %w", name, err)
	}
	return loc, nil
}
