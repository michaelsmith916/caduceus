// Package telemetry collects worker resource and model-performance telemetry.
// It deliberately represents missing measurements as unknown or unsupported;
// an absent measurement is never inferred to be zero.
package telemetry

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/caduceus/caduceus/internal/workers"
)

const (
	unitBytes = "bytes"
	unitCores = "cores"
	unitRatio = "ratio"
)

// Source is the narrow boundary between telemetry policy and platform probes.
// Implementations should use nil metric values for unavailable measurements
// and an explicit status describing why they are absent.
type Source interface {
	Sample(context.Context) (Sample, error)
}

// Sample is a platform-neutral, pre-normalized resource reading. The collector
// attaches its clock timestamp and converts it to the worker protocol model.
type Sample struct {
	LogicalProcessors       *int64
	LogicalProcessorsStatus workers.MetricStatus
	CPUUtilization          *float64
	CPUUtilizationStatus    workers.MetricStatus
	TotalMemoryBytes        *int64
	AvailableMemoryBytes    *int64
	MemoryStatus            workers.MetricStatus
	GPUStatus               workers.MetricStatus
	GPUs                    []workers.GPUResource
}

// Options makes collection deterministic in tests and lets embedders provide
// a richer GPU/platform probe without changing telemetry policy.
type Options struct {
	Source Source
	Now    func() time.Time
}

// Collector serializes access to its Source because CPU utilization probes
// commonly need a previous reading. It is safe for concurrent use.
type Collector struct {
	mu     sync.Mutex
	source Source
	now    func() time.Time
}

// New constructs a resource collector. The default source measures logical CPU
// count and RAM, samples CPU utilization where supported, and explicitly marks
// GPU telemetry unsupported because Caduceus has no vendor probe by default.
func New(options Options) *Collector {
	source := options.Source
	if source == nil {
		source = newSystemSource()
	}
	now := options.Now
	if now == nil {
		now = time.Now
	}
	return &Collector{source: source, now: now}
}

// NewCollector is an explicit alias for New.
func NewCollector(options Options) *Collector { return New(options) }

// Collect samples the source and returns the protocol-ready resource snapshot.
// A source error is returned alongside an unknown snapshot so callers can keep
// advertising honestly while retaining diagnostics.
func (c *Collector) Collect(ctx context.Context) (workers.ResourceSnapshot, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	collectedAt := c.now()
	sample, err := c.source.Sample(ctx)
	if err != nil {
		return unknownSnapshot(collectedAt), fmt.Errorf("collect worker resources: %w", err)
	}
	return snapshotFromSample(sample, collectedAt), nil
}

func snapshotFromSample(sample Sample, collectedAt time.Time) workers.ResourceSnapshot {
	logicalStatus := normalizeStatus(sample.LogicalProcessors, sample.LogicalProcessorsStatus)
	utilStatus := normalizeStatus(sample.CPUUtilization, sample.CPUUtilizationStatus)
	memoryTotalStatus := normalizeStatus(sample.TotalMemoryBytes, sample.MemoryStatus)
	memoryAvailableStatus := normalizeStatus(sample.AvailableMemoryBytes, sample.MemoryStatus)

	logical := validNonnegativeInt(sample.LogicalProcessors)
	utilization := validRatio(sample.CPUUtilization)
	totalMemory := validNonnegativeInt(sample.TotalMemoryBytes)
	availableMemory := validNonnegativeInt(sample.AvailableMemoryBytes)
	if logical == nil || !statusCarriesValue(logicalStatus) {
		logical = nil
		logicalStatus = absentStatus(logicalStatus)
	}
	if utilization == nil || !statusCarriesValue(utilStatus) {
		utilization = nil
		utilStatus = absentStatus(utilStatus)
	}
	if totalMemory == nil || !statusCarriesValue(memoryTotalStatus) {
		totalMemory = nil
		memoryTotalStatus = absentStatus(memoryTotalStatus)
	}
	if availableMemory == nil || !statusCarriesValue(memoryAvailableStatus) {
		availableMemory = nil
		memoryAvailableStatus = absentStatus(memoryAvailableStatus)
	}
	if totalMemory != nil && availableMemory != nil && *availableMemory > *totalMemory {
		availableMemory = nil
		memoryAvailableStatus = workers.MetricUnknown
	}

	availableCapacityStatus := utilStatus
	var availableCapacity *float64
	if logical != nil && utilization != nil {
		capacity := float64(*logical) * (1 - *utilization)
		availableCapacity = &capacity
		if logicalStatus != workers.MetricCurrent || utilStatus != workers.MetricCurrent {
			availableCapacityStatus = workers.MetricStale
		}
	} else {
		availableCapacityStatus = absentStatus(availableCapacityStatus)
	}

	gpuStatus := sample.GPUStatus
	if !gpuStatus.Valid() {
		gpuStatus = workers.MetricUnknown
	}
	gpus := cloneGPUs(sample.GPUs)
	if gpuStatus != workers.MetricCurrent && gpuStatus != workers.MetricStale {
		gpus = nil
	}

	status := aggregateStatus(logicalStatus, memoryTotalStatus, memoryAvailableStatus)

	return workers.ResourceSnapshot{
		Status:      status,
		CollectedAt: collectedAt,
		CPU: workers.CPUResources{
			LogicalProcessors: metricInt(logical, unitCores, logicalStatus, collectedAt),
			AvailableCapacity: metricFloat(availableCapacity, unitCores, availableCapacityStatus, collectedAt),
			Utilization:       metricFloat(utilization, unitRatio, utilStatus, collectedAt),
		},
		RAM: workers.MemoryResources{
			TotalBytes:     metricInt(totalMemory, unitBytes, memoryTotalStatus, collectedAt),
			AvailableBytes: metricInt(availableMemory, unitBytes, memoryAvailableStatus, collectedAt),
		},
		GPUStatus: gpuStatus,
		GPUs:      gpus,
	}
}

func unknownSnapshot(collectedAt time.Time) workers.ResourceSnapshot {
	return snapshotFromSample(Sample{
		LogicalProcessorsStatus: workers.MetricUnknown,
		CPUUtilizationStatus:    workers.MetricUnknown,
		MemoryStatus:            workers.MetricUnknown,
		GPUStatus:               workers.MetricUnknown,
	}, collectedAt)
}

func normalizeStatus[T any](value *T, status workers.MetricStatus) workers.MetricStatus {
	if !status.Valid() {
		if value != nil {
			return workers.MetricCurrent
		}
		return workers.MetricUnknown
	}
	return status
}

func absentStatus(status workers.MetricStatus) workers.MetricStatus {
	if status == workers.MetricUnsupported {
		return status
	}
	return workers.MetricUnknown
}

func statusCarriesValue(status workers.MetricStatus) bool {
	return status == workers.MetricCurrent || status == workers.MetricStale
}

func aggregateStatus(statuses ...workers.MetricStatus) workers.MetricStatus {
	result := workers.MetricCurrent
	for _, status := range statuses {
		switch status {
		case workers.MetricCurrent:
			continue
		case workers.MetricStale:
			if result == workers.MetricCurrent {
				result = workers.MetricStale
			}
		default:
			return workers.MetricUnknown
		}
	}
	return result
}

func validNonnegativeInt(value *int64) *int64 {
	if value == nil || *value < 0 {
		return nil
	}
	copy := *value
	return &copy
}

func validRatio(value *float64) *float64 {
	if value == nil || math.IsNaN(*value) || math.IsInf(*value, 0) || *value < 0 || *value > 1 {
		return nil
	}
	copy := *value
	return &copy
}

func metricInt(value *int64, unit string, status workers.MetricStatus, at time.Time) workers.Int64Metric {
	return workers.Int64Metric{Value: value, Unit: unit, Status: status, CollectedAt: at}
}

func metricFloat(value *float64, unit string, status workers.MetricStatus, at time.Time) workers.FloatMetric {
	return workers.FloatMetric{Value: value, Unit: unit, Status: status, CollectedAt: at}
}

func cloneGPUs(input []workers.GPUResource) []workers.GPUResource {
	if len(input) == 0 {
		return nil
	}
	output := make([]workers.GPUResource, len(input))
	copy(output, input)
	for i := range output {
		output[i].Capabilities = append([]string(nil), input[i].Capabilities...)
		output[i].TotalMemoryBytes.Value = cloneInt64(input[i].TotalMemoryBytes.Value)
		output[i].AvailableMemoryBytes.Value = cloneInt64(input[i].AvailableMemoryBytes.Value)
		output[i].Utilization.Value = cloneFloat64(input[i].Utilization.Value)
	}
	return output
}

func cloneInt64(value *int64) *int64 {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func cloneFloat64(value *float64) *float64 {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

// PerformanceOptions configures an exponentially weighted moving average.
// Alpha must be in (0,1]; zero selects DefaultEWMAAlpha.
type PerformanceOptions struct {
	Alpha float64
	Now   func() time.Time
}

const DefaultEWMAAlpha = 0.25

type performanceSample struct {
	rate      float64
	count     int
	updatedAt time.Time
}

// PerformanceTracker records rolling per-model throughput. It is safe for
// concurrent observations and returns defensive snapshots sorted by model.
type PerformanceTracker struct {
	mu      sync.RWMutex
	alpha   float64
	now     func() time.Time
	samples map[string]performanceSample
}

func NewPerformanceTracker(options PerformanceOptions) (*PerformanceTracker, error) {
	alpha := options.Alpha
	if alpha == 0 {
		alpha = DefaultEWMAAlpha
	}
	if math.IsNaN(alpha) || math.IsInf(alpha, 0) || alpha <= 0 || alpha > 1 {
		return nil, errors.New("telemetry EWMA alpha must be in (0,1]")
	}
	now := options.Now
	if now == nil {
		now = time.Now
	}
	return &PerformanceTracker{alpha: alpha, now: now, samples: make(map[string]performanceSample)}, nil
}

// Observe derives a token rate from a completed inference and records it.
func (p *PerformanceTracker) Observe(model string, tokens int64, elapsed time.Duration) (workers.ModelPerformance, error) {
	if tokens < 0 {
		return workers.ModelPerformance{}, errors.New("telemetry token count must be non-negative")
	}
	if elapsed <= 0 {
		return workers.ModelPerformance{}, errors.New("telemetry elapsed time must be positive")
	}
	return p.ObserveRate(model, float64(tokens)/elapsed.Seconds())
}

// ObserveRate records an already-derived tokens-per-second measurement.
func (p *PerformanceTracker) ObserveRate(model string, tokensPerSecond float64) (workers.ModelPerformance, error) {
	model = strings.TrimSpace(model)
	if model == "" {
		return workers.ModelPerformance{}, errors.New("telemetry model is required")
	}
	if math.IsNaN(tokensPerSecond) || math.IsInf(tokensPerSecond, 0) || tokensPerSecond < 0 {
		return workers.ModelPerformance{}, errors.New("telemetry tokens per second must be finite and non-negative")
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	measurement := p.samples[model]
	if measurement.count == 0 {
		measurement.rate = tokensPerSecond
	} else {
		measurement.rate = p.alpha*tokensPerSecond + (1-p.alpha)*measurement.rate
	}
	measurement.count++
	measurement.updatedAt = p.now()
	p.samples[model] = measurement
	return modelPerformance(model, measurement), nil
}

// Get returns a defensive copy of one model's rolling performance.
func (p *PerformanceTracker) Get(model string) (workers.ModelPerformance, bool) {
	model = strings.TrimSpace(model)
	p.mu.RLock()
	defer p.mu.RUnlock()
	measurement, ok := p.samples[model]
	if !ok {
		return workers.ModelPerformance{}, false
	}
	return modelPerformance(model, measurement), true
}

// Snapshot returns all measured models in stable lexical order.
func (p *PerformanceTracker) Snapshot() []workers.ModelPerformance {
	p.mu.RLock()
	defer p.mu.RUnlock()
	models := make([]string, 0, len(p.samples))
	for model := range p.samples {
		models = append(models, model)
	}
	sort.Strings(models)
	result := make([]workers.ModelPerformance, 0, len(models))
	for _, model := range models {
		result = append(result, modelPerformance(model, p.samples[model]))
	}
	return result
}

func modelPerformance(model string, sample performanceSample) workers.ModelPerformance {
	rate := sample.rate
	return workers.ModelPerformance{
		Model:           model,
		TokensPerSecond: &rate,
		SampleCount:     sample.count,
		UpdatedAt:       sample.updatedAt,
		Measured:        true,
	}
}
