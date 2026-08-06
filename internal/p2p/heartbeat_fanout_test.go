package p2p

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestRunHeartbeatFanoutDoesNotSerializePeers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	unblockFirst := make(chan struct{})
	secondRan := make(chan struct{})
	jobs := []heartbeatFanoutJob{
		func(ctx context.Context) {
			select {
			case <-unblockFirst:
			case <-ctx.Done():
			}
		},
		func(context.Context) {
			close(secondRan)
			close(unblockFirst)
		},
	}

	runHeartbeatFanout(ctx, 2, jobs)
	select {
	case <-secondRan:
	default:
		t.Fatal("healthy peer heartbeat was serialized behind a blocked peer")
	}
}

func TestRunHeartbeatFanoutBoundsConcurrency(t *testing.T) {
	const (
		limit = 3
		count = 12
	)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	gate := make(chan struct{})
	started := make(chan struct{}, count)
	var running atomic.Int32
	var maximum atomic.Int32
	var completed atomic.Int32
	jobs := make([]heartbeatFanoutJob, 0, count)
	for range count {
		jobs = append(jobs, func(ctx context.Context) {
			current := running.Add(1)
			for {
				observed := maximum.Load()
				if current <= observed || maximum.CompareAndSwap(observed, current) {
					break
				}
			}
			started <- struct{}{}
			select {
			case <-gate:
			case <-ctx.Done():
			}
			running.Add(-1)
			completed.Add(1)
		})
	}

	done := make(chan struct{})
	go func() {
		runHeartbeatFanout(ctx, limit, jobs)
		close(done)
	}()
	for range limit {
		select {
		case <-started:
		case <-ctx.Done():
			t.Fatal("fanout did not start the configured number of workers")
		}
	}
	if got := maximum.Load(); got != limit {
		t.Fatalf("maximum concurrency = %d, want %d", got, limit)
	}
	close(gate)
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("fanout did not finish")
	}
	if got := completed.Load(); got != count {
		t.Fatalf("completed jobs = %d, want %d", got, count)
	}
}

func TestRunHeartbeatFanoutStopsDispatchingAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	firstStarted := make(chan struct{})
	var laterRuns atomic.Int32
	jobs := []heartbeatFanoutJob{
		func(ctx context.Context) {
			close(firstStarted)
			<-ctx.Done()
		},
	}
	for range 8 {
		jobs = append(jobs, func(context.Context) {
			laterRuns.Add(1)
		})
	}

	done := make(chan struct{})
	go func() {
		runHeartbeatFanout(ctx, 1, jobs)
		close(done)
	}()
	<-firstStarted
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("fanout did not stop after cancellation")
	}
	if got := laterRuns.Load(); got != 0 {
		t.Fatalf("jobs dispatched after cancellation = %d, want 0", got)
	}
}
