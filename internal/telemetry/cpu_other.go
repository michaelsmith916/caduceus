//go:build !linux && !windows

package telemetry

import "github.com/caduceus/caduceus/internal/workers"

type cpuSampler struct{}

func newCPUSampler() cpuSampler { return cpuSampler{} }

func (*cpuSampler) sample() (*float64, workers.MetricStatus) {
	return nil, workers.MetricUnsupported
}
