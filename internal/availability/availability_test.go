package availability

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/caduceus/caduceus/internal/config"
)

type fakeActivity struct {
	idle      time.Duration
	loggedIn  bool
	supported bool
	err       error
}

func (f fakeActivity) IdleFor(context.Context) (time.Duration, bool, error) {
	return f.idle, f.supported, f.err
}

func (f fakeActivity) UserLoggedIn(context.Context) (bool, bool, error) {
	return f.loggedIn, f.supported, f.err
}

func TestManualAvailabilityChanges(t *testing.T) {
	e := New(config.AvailabilityConfig{Mode: config.AvailabilityManual}, nil, nil)
	if got := e.Evaluate(context.Background()); got.AcceptingWork {
		t.Fatal("manual mode should start unavailable when configured false")
	}
	e.SetManualAvailable(true)
	if got := e.Evaluate(context.Background()); !got.AcceptingWork || got.State != StateAvailable {
		t.Fatalf("unexpected result %#v", got)
	}
	e.SetDraining(true)
	if got := e.Evaluate(context.Background()); got.State != StateDraining || got.AcceptingWork {
		t.Fatalf("unexpected drain result %#v", got)
	}
}

func TestIdleAndLoginDetectionFailClosed(t *testing.T) {
	e := New(config.AvailabilityConfig{Mode: config.AvailabilityIdle, IdleAfterSeconds: 60}, fakeActivity{idle: 2 * time.Minute, supported: true}, nil)
	if got := e.Evaluate(context.Background()); !got.AcceptingWork {
		t.Fatalf("idle machine should be available: %#v", got)
	}
	e = New(config.AvailabilityConfig{Mode: config.AvailabilityLoggedOut}, fakeActivity{supported: true, loggedIn: true}, nil)
	if got := e.Evaluate(context.Background()); got.AcceptingWork {
		t.Fatal("logged-in machine should be unavailable")
	}
	e = New(config.AvailabilityConfig{Mode: config.AvailabilityIdle, IdleAfterSeconds: 60}, fakeActivity{supported: true, err: errors.New("detector failed")}, nil)
	if got := e.Evaluate(context.Background()); got.AcceptingWork || got.Reason != "activity_unknown" {
		t.Fatalf("detector error must fail closed: %#v", got)
	}
}

func TestScheduleAndOvernightWindow(t *testing.T) {
	cfg := config.AvailabilityConfig{
		Mode:     config.AvailabilityScheduled,
		Timezone: "UTC",
		Schedule: []config.AvailabilityWindow{{Days: []string{"mon"}, Start: "22:00", End: "06:00"}},
	}
	for _, test := range []struct {
		name string
		now  time.Time
		want bool
	}{
		{name: "monday night", now: time.Date(2026, 8, 3, 23, 0, 0, 0, time.UTC), want: true},
		{name: "tuesday continuation", now: time.Date(2026, 8, 4, 5, 59, 0, 0, time.UTC), want: true},
		{name: "tuesday daytime", now: time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC), want: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			e := New(cfg, nil, func() time.Time { return test.now })
			if got := e.Evaluate(context.Background()).AcceptingWork; got != test.want {
				t.Fatalf("accepting = %v, want %v", got, test.want)
			}
		})
	}
}
