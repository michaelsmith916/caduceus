package telemetry

import (
	"context"
	"math"
	"runtime"

	"github.com/caduceus/caduceus/internal/workers"
	"github.com/pbnjay/memory"
)

type systemSource struct {
	cpu cpuSampler
}

func newSystemSource() Source {
	return &systemSource{cpu: newCPUSampler()}
}

func (source *systemSource) Sample(ctx context.Context) (Sample, error) {
	if err := ctx.Err(); err != nil {
		return Sample{}, err
	}

	logicalProcessors := int64(runtime.NumCPU())
	utilization, utilizationStatus := source.cpu.sample()
	total := uint64ToInt64(memory.TotalMemory())
	available := uint64ToInt64(memory.FreeMemory())
	memoryStatus := workers.MetricCurrent
	if total == nil || available == nil || *total == 0 || *available > *total {
		total = nil
		available = nil
		memoryStatus = workers.MetricUnknown
	}

	return Sample{
		LogicalProcessors:       &logicalProcessors,
		LogicalProcessorsStatus: workers.MetricCurrent,
		CPUUtilization:          utilization,
		CPUUtilizationStatus:    utilizationStatus,
		TotalMemoryBytes:        total,
		AvailableMemoryBytes:    available,
		MemoryStatus:            memoryStatus,
		GPUStatus:               workers.MetricUnsupported,
	}, nil
}

func uint64ToInt64(value uint64) *int64 {
	if value > math.MaxInt64 {
		return nil
	}
	converted := int64(value)
	return &converted
}
