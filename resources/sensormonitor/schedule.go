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

// SnoozeWindow is a recurring weekly time window during which rules are not
// evaluated, e.g. outside business hours.
type SnoozeWindow struct {
	// Days lists the days of the week the window starts on ("mon".."sun" or full
	// names, case-insensitive). Empty means every day.
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

// window is a parsed SnoozeWindow. start and end are minutes since midnight.
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

// parseWindows parses a list of snooze windows.
func parseWindows(ws []SnoozeWindow) ([]window, error) {
	out := make([]window, 0, len(ws))
	for i, sw := range ws {
		w, err := parseWindow(sw)
		if err != nil {
			return nil, fmt.Errorf("snooze_windows[%d]: %w", i, err)
		}
		out = append(out, w)
	}
	return out, nil
}

func parseWindow(sw SnoozeWindow) (window, error) {
	var w window
	if len(sw.Days) == 0 {
		for i := range w.days {
			w.days[i] = true
		}
	}
	for _, d := range sw.Days {
		wd, err := parseWeekday(d)
		if err != nil {
			return window{}, err
		}
		w.days[wd] = true
	}

	var err error
	if w.start, err = parseClock(sw.Start, 0); err != nil {
		return window{}, fmt.Errorf("start: %w", err)
	}
	if w.end, err = parseClock(sw.End, minutesPerDay); err != nil {
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
		return 0, fmt.Errorf("invalid day %q: want mon, tue, wed, thu, fri, sat, or sun", s)
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
