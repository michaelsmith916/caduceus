package availability

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/caduceus/caduceus/internal/config"
)

const (
	StateAvailable   = "available"
	StateUnavailable = "unavailable"
	StateDraining    = "draining"
)

// ActivityProvider is deliberately local. Implementations must not return user
// names or application details; the evaluator exposes only an opaque result.
type ActivityProvider interface {
	IdleFor(context.Context) (time.Duration, bool, error)
	UserLoggedIn(context.Context) (bool, bool, error)
}

type Result struct {
	State         string `json:"state"`
	AcceptingWork bool   `json:"accepting_work"`
	Reason        string `json:"reason,omitempty"`
}

type Evaluator struct {
	cfg      config.AvailabilityConfig
	activity ActivityProvider
	now      func() time.Time

	mu              sync.RWMutex
	manualAvailable bool
	draining        bool
}

func New(cfg config.AvailabilityConfig, activity ActivityProvider, now func() time.Time) *Evaluator {
	if now == nil {
		now = time.Now
	}
	return &Evaluator{cfg: cfg, activity: activity, now: now, manualAvailable: cfg.ManualAvailable}
}

func (e *Evaluator) SetManualAvailable(available bool) {
	e.mu.Lock()
	e.manualAvailable = available
	e.mu.Unlock()
}

func (e *Evaluator) SetDraining(draining bool) {
	e.mu.Lock()
	e.draining = draining
	e.mu.Unlock()
}

func (e *Evaluator) Evaluate(ctx context.Context) Result {
	e.mu.RLock()
	manual, draining := e.manualAvailable, e.draining
	e.mu.RUnlock()
	if draining || e.cfg.Mode == config.AvailabilityDraining {
		return Result{State: StateDraining, Reason: "local_policy"}
	}
	switch e.cfg.Mode {
	case config.AvailabilityAlways:
		return available()
	case config.AvailabilityManual:
		if manual {
			return available()
		}
		return unavailable("manual")
	case config.AvailabilityUnavailable:
		return unavailable("local_policy")
	case config.AvailabilityIdle:
		if e.activity == nil {
			return unavailable("activity_unknown")
		}
		idle, supported, err := e.activity.IdleFor(ctx)
		if err != nil || !supported {
			return unavailable("activity_unknown")
		}
		if idle >= time.Duration(e.cfg.IdleAfterSeconds)*time.Second {
			return available()
		}
		return unavailable("locally_active")
	case config.AvailabilityLoggedOut:
		if e.activity == nil {
			return unavailable("login_state_unknown")
		}
		loggedIn, supported, err := e.activity.UserLoggedIn(ctx)
		if err != nil || !supported {
			return unavailable("login_state_unknown")
		}
		if !loggedIn {
			return available()
		}
		return unavailable("local_user_present")
	case config.AvailabilityScheduled:
		if inSchedule(e.now(), e.cfg) {
			return available()
		}
		return unavailable("outside_schedule")
	default:
		return unavailable("invalid_policy")
	}
}

func available() Result { return Result{State: StateAvailable, AcceptingWork: true} }

func unavailable(reason string) Result {
	return Result{State: StateUnavailable, AcceptingWork: false, Reason: reason}
}

func inSchedule(now time.Time, cfg config.AvailabilityConfig) bool {
	if cfg.Timezone != "" {
		if location, err := time.LoadLocation(cfg.Timezone); err == nil {
			now = now.In(location)
		}
	}
	minute := now.Hour()*60 + now.Minute()
	today := dayName(now.Weekday())
	yesterday := dayName((now.Weekday() + 6) % 7)
	for _, window := range cfg.Schedule {
		start, err1 := clockMinute(window.Start)
		end, err2 := clockMinute(window.End)
		if err1 != nil || err2 != nil || start == end {
			continue
		}
		if start < end && hasDay(window.Days, today) && minute >= start && minute < end {
			return true
		}
		if start > end && ((hasDay(window.Days, today) && minute >= start) || (hasDay(window.Days, yesterday) && minute < end)) {
			return true
		}
	}
	return false
}

func clockMinute(value string) (int, error) {
	parsed, err := time.Parse("15:04", value)
	if err != nil {
		return 0, err
	}
	return parsed.Hour()*60 + parsed.Minute(), nil
}

func hasDay(days []string, want string) bool {
	for _, day := range days {
		if strings.EqualFold(strings.TrimSpace(day), want) {
			return true
		}
	}
	return false
}

func dayName(day time.Weekday) string {
	names := [...]string{"sun", "mon", "tue", "wed", "thu", "fri", "sat"}
	if day < time.Sunday || day > time.Saturday {
		return ""
	}
	return names[day]
}

var ErrUnsupported = errors.New("local activity detection is unsupported")
