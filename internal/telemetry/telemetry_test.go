package telemetry

import (
	"context"
	"errors"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/caduceus/caduceus/internal/workers"
)

type sourceFunc func(context.Context) (Sample, error)

func (fn sourceFunc) Sample(ctx context.Context) (Sample, error) { return fn(ctx) }

func int64Pointer(value int64) *int64       { return &value }
func float64Pointer(value float64) *float64 { return &value }

func TestCollectorMapsResourceSnapshot(t *testing.T) {
	t.Parallel()
	collectedAt := time.Date(2026, time.August, 5, 12, 0, 0, 0, time.UTC)
	sourceCapabilities := []string{"tensor", "fp16"}
	collector := New(Options{
		Now: func() time.Time { return collectedAt },
		Source: sourceFunc(func(context.Context) (Sample, error) {
			return Sample{
				LogicalProcessors:       int64Pointer(8),
				LogicalProcessorsStatus: workers.MetricCurrent,
				CPUUtilization:          float64Pointer(0.25),
				CPUUtilizationStatus:    workers.MetricCurrent,
				TotalMemoryBytes:        int64Pointer(32 << 30),
				AvailableMemoryBytes:    int64Pointer(12 << 30),
				MemoryStatus:            workers.MetricCurrent,
				GPUStatus:               workers.MetricCurrent,
				GPUs: []workers.GPUResource{{
					Vendor:       "example",
					Model:        "accelerator",
					Capabilities: sourceCapabilities,
				}},
			}, nil
		}),
	})

	snapshot, err := collector.Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect() error = %v", err)
	}
	if snapshot.Status != workers.MetricCurrent || !snapshot.CollectedAt.Equal(collectedAt) {
		t.Fatalf("snapshot metadata = %#v", snapshot)
	}
	if got := snapshot.CPU.LogicalProcessors.Value; got == nil || *got != 8 {
		t.Fatalf("logical processors = %v", got)
	}
	if got := snapshot.CPU.AvailableCapacity.Value; got == nil || *got != 6 {
		t.Fatalf("available CPU capacity = %v, want 6", got)
	}
	if snapshot.CPU.AvailableCapacity.Unit != "cores" || snapshot.CPU.Utilization.Unit != "ratio" {
		t.Fatalf("CPU metric units = %#v", snapshot.CPU)
	}
	if got := snapshot.RAM.AvailableBytes.Value; got == nil || *got != 12<<30 {
		t.Fatalf("available memory = %v", got)
	}
	if snapshot.GPUStatus != workers.MetricCurrent || len(snapshot.GPUs) != 1 {
		t.Fatalf("GPU telemetry = status %q, devices %#v", snapshot.GPUStatus, snapshot.GPUs)
	}

	sourceCapabilities[0] = "mutated"
	if snapshot.GPUs[0].Capabilities[0] != "tensor" {
		t.Fatal("Collect() did not defensively copy GPU capabilities")
	}
}

func TestCollectorPreservesUnknownAndUnsupported(t *testing.T) {
	t.Parallel()
	collector := New(Options{Source: sourceFunc(func(context.Context) (Sample, error) {
		return Sample{
			LogicalProcessors:       int64Pointer(4),
			LogicalProcessorsStatus: workers.MetricCurrent,
			CPUUtilizationStatus:    workers.MetricUnsupported,
			TotalMemoryBytes:        int64Pointer(1024),
			AvailableMemoryBytes:    int64Pointer(512),
			MemoryStatus:            workers.MetricCurrent,
			GPUStatus:               workers.MetricUnsupported,
		}, nil
	})})

	snapshot, err := collector.Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect() error = %v", err)
	}
	if snapshot.CPU.Utilization.Status != workers.MetricUnsupported || snapshot.CPU.Utilization.Value != nil {
		t.Fatalf("CPU utilization = %#v", snapshot.CPU.Utilization)
	}
	if snapshot.CPU.AvailableCapacity.Status != workers.MetricUnsupported || snapshot.CPU.AvailableCapacity.Value != nil {
		t.Fatalf("available CPU = %#v", snapshot.CPU.AvailableCapacity)
	}
	if snapshot.GPUStatus != workers.MetricUnsupported || len(snapshot.GPUs) != 0 {
		t.Fatalf("GPU telemetry = %q, %#v", snapshot.GPUStatus, snapshot.GPUs)
	}
}

func TestCollectorRejectsInvalidMeasurements(t *testing.T) {
	t.Parallel()
	collector := New(Options{Source: sourceFunc(func(context.Context) (Sample, error) {
		return Sample{
			LogicalProcessors:       int64Pointer(-1),
			LogicalProcessorsStatus: workers.MetricCurrent,
			CPUUtilization:          float64Pointer(1.5),
			CPUUtilizationStatus:    workers.MetricCurrent,
			TotalMemoryBytes:        int64Pointer(100),
			AvailableMemoryBytes:    int64Pointer(101),
			MemoryStatus:            workers.MetricCurrent,
			GPUStatus:               workers.MetricCurrent,
			GPUs:                    []workers.GPUResource{{Model: "must-be-discarded-only-on-unsupported"}},
		}, nil
	})})

	snapshot, err := collector.Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect() error = %v", err)
	}
	if snapshot.Status != workers.MetricUnknown {
		t.Fatalf("snapshot status = %q, want unknown", snapshot.Status)
	}
	if snapshot.CPU.LogicalProcessors.Value != nil || snapshot.CPU.LogicalProcessors.Status != workers.MetricUnknown {
		t.Fatalf("logical processors = %#v", snapshot.CPU.LogicalProcessors)
	}
	if snapshot.CPU.Utilization.Value != nil || snapshot.CPU.Utilization.Status != workers.MetricUnknown {
		t.Fatalf("utilization = %#v", snapshot.CPU.Utilization)
	}
	if snapshot.RAM.AvailableBytes.Value != nil || snapshot.RAM.AvailableBytes.Status != workers.MetricUnknown {
		t.Fatalf("available memory = %#v", snapshot.RAM.AvailableBytes)
	}
}

func TestCollectorDropsValuesWithAbsentStatusAndPreservesStale(t *testing.T) {
	t.Parallel()
	collector := New(Options{Source: sourceFunc(func(context.Context) (Sample, error) {
		return Sample{
			LogicalProcessors:       int64Pointer(8),
			LogicalProcessorsStatus: workers.MetricStale,
			CPUUtilization:          float64Pointer(0.5),
			CPUUtilizationStatus:    workers.MetricUnknown,
			TotalMemoryBytes:        int64Pointer(100),
			AvailableMemoryBytes:    int64Pointer(50),
			MemoryStatus:            workers.MetricStale,
			GPUStatus:               workers.MetricUnsupported,
		}, nil
	})})

	snapshot, err := collector.Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect() error = %v", err)
	}
	if snapshot.CPU.Utilization.Value != nil || snapshot.CPU.Utilization.Status != workers.MetricUnknown {
		t.Fatalf("unknown utilization = %#v", snapshot.CPU.Utilization)
	}
	if snapshot.CPU.LogicalProcessors.Value == nil || snapshot.CPU.LogicalProcessors.Status != workers.MetricStale {
		t.Fatalf("stale logical processors = %#v", snapshot.CPU.LogicalProcessors)
	}
	if snapshot.Status != workers.MetricStale {
		t.Fatalf("aggregate status = %q, want stale", snapshot.Status)
	}
}

func TestCollectorReturnsDiagnosticSnapshotOnSourceError(t *testing.T) {
	t.Parallel()
	wantErr := errors.New("probe failed")
	collectedAt := time.Unix(10, 0)
	collector := New(Options{
		Now: func() time.Time { return collectedAt },
		Source: sourceFunc(func(context.Context) (Sample, error) {
			return Sample{}, wantErr
		}),
	})

	snapshot, err := collector.Collect(context.Background())
	if !errors.Is(err, wantErr) {
		t.Fatalf("Collect() error = %v, want wrapped %v", err, wantErr)
	}
	if snapshot.Status != workers.MetricUnknown || !snapshot.CollectedAt.Equal(collectedAt) {
		t.Fatalf("diagnostic snapshot = %#v", snapshot)
	}
	if snapshot.GPUStatus != workers.MetricUnknown {
		t.Fatalf("GPU status = %q, want unknown", snapshot.GPUStatus)
	}
}

func TestDefaultCollectorReportsGPUUnsupported(t *testing.T) {
	collector := New(Options{})
	snapshot, err := collector.Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect() error = %v", err)
	}
	if snapshot.GPUStatus != workers.MetricUnsupported {
		t.Fatalf("GPU status = %q, want unsupported", snapshot.GPUStatus)
	}
	if got := snapshot.CPU.LogicalProcessors.Value; got == nil || *got < 1 {
		t.Fatalf("logical processors = %v", got)
	}
	if snapshot.RAM.TotalBytes.Value == nil || snapshot.RAM.AvailableBytes.Value == nil {
		t.Fatalf("RAM telemetry = %#v", snapshot.RAM)
	}
}

func TestPerformanceTrackerEWMAAndStableSnapshot(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 5, 13, 0, 0, 0, time.UTC)
	tracker, err := NewPerformanceTracker(PerformanceOptions{Alpha: 0.25, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatalf("NewPerformanceTracker() error = %v", err)
	}

	first, err := tracker.ObserveRate(" model-z ", 10)
	if err != nil || first.TokensPerSecond == nil || *first.TokensPerSecond != 10 {
		t.Fatalf("first observation = %#v, %v", first, err)
	}
	now = now.Add(time.Second)
	second, err := tracker.ObserveRate("model-z", 30)
	if err != nil {
		t.Fatalf("second observation error = %v", err)
	}
	if second.TokensPerSecond == nil || *second.TokensPerSecond != 15 || second.SampleCount != 2 || !second.UpdatedAt.Equal(now) {
		t.Fatalf("second observation = %#v", second)
	}
	if _, err := tracker.ObserveRate("model-a", 4); err != nil {
		t.Fatalf("model-a observation error = %v", err)
	}
	snapshot := tracker.Snapshot()
	if len(snapshot) != 2 || snapshot[0].Model != "model-a" || snapshot[1].Model != "model-z" {
		t.Fatalf("Snapshot() order = %#v", snapshot)
	}

	*snapshot[1].TokensPerSecond = 999
	got, ok := tracker.Get("model-z")
	if !ok || got.TokensPerSecond == nil || *got.TokensPerSecond != 15 {
		t.Fatalf("Get() after caller mutation = %#v, %v", got, ok)
	}
}

func TestPerformanceTrackerObserveAndValidation(t *testing.T) {
	t.Parallel()
	tracker, err := NewPerformanceTracker(PerformanceOptions{Alpha: 1})
	if err != nil {
		t.Fatalf("NewPerformanceTracker() error = %v", err)
	}
	measurement, err := tracker.Observe("model", 100, 2*time.Second)
	if err != nil || measurement.TokensPerSecond == nil || *measurement.TokensPerSecond != 50 {
		t.Fatalf("Observe() = %#v, %v", measurement, err)
	}

	for name, call := range map[string]func() error{
		"empty model":     func() error { _, err := tracker.ObserveRate(" ", 1); return err },
		"negative rate":   func() error { _, err := tracker.ObserveRate("model", -1); return err },
		"NaN rate":        func() error { _, err := tracker.ObserveRate("model", math.NaN()); return err },
		"negative tokens": func() error { _, err := tracker.Observe("model", -1, time.Second); return err },
		"zero elapsed":    func() error { _, err := tracker.Observe("model", 1, 0); return err },
	} {
		t.Run(name, func(t *testing.T) {
			if err := call(); err == nil {
				t.Fatal("expected error")
			}
		})
	}
	if _, err := NewPerformanceTracker(PerformanceOptions{Alpha: 1.1}); err == nil {
		t.Fatal("expected invalid alpha error")
	}
}

func TestPerformanceTrackerConcurrentObservations(t *testing.T) {
	tracker, err := NewPerformanceTracker(PerformanceOptions{Alpha: 0.5})
	if err != nil {
		t.Fatalf("NewPerformanceTracker() error = %v", err)
	}

	const observations = 100
	var wait sync.WaitGroup
	wait.Add(observations)
	for i := 0; i < observations; i++ {
		go func(rate float64) {
			defer wait.Done()
			if _, err := tracker.ObserveRate("shared", rate); err != nil {
				t.Errorf("ObserveRate() error = %v", err)
			}
		}(float64(i))
	}
	wait.Wait()

	measurement, ok := tracker.Get("shared")
	if !ok || measurement.SampleCount != observations {
		t.Fatalf("concurrent measurement = %#v, %v", measurement, ok)
	}
	if measurement.TokensPerSecond == nil || math.IsNaN(*measurement.TokensPerSecond) || math.IsInf(*measurement.TokensPerSecond, 0) {
		t.Fatalf("invalid rolling rate = %#v", measurement)
	}
}

func TestCollectorSerializesConcurrentSourceAccess(t *testing.T) {
	var sourceCalls int
	collector := New(Options{Source: sourceFunc(func(context.Context) (Sample, error) {
		sourceCalls++
		return Sample{
			LogicalProcessors:       int64Pointer(1),
			LogicalProcessorsStatus: workers.MetricCurrent,
			TotalMemoryBytes:        int64Pointer(1),
			AvailableMemoryBytes:    int64Pointer(1),
			MemoryStatus:            workers.MetricCurrent,
			GPUStatus:               workers.MetricUnsupported,
		}, nil
	})})

	const observations = 50
	var wait sync.WaitGroup
	wait.Add(observations)
	for i := 0; i < observations; i++ {
		go func() {
			defer wait.Done()
			if _, err := collector.Collect(context.Background()); err != nil {
				t.Errorf("Collect() error = %v", err)
			}
		}()
	}
	wait.Wait()
	if sourceCalls != observations {
		t.Fatalf("source calls = %d, want %d", sourceCalls, observations)
	}
}
