//go:build windows

package telemetry

import (
	"unsafe"

	"github.com/caduceus/caduceus/internal/workers"
	"golang.org/x/sys/windows"
)

var getSystemTimes = windows.NewLazySystemDLL("kernel32.dll").NewProc("GetSystemTimes")

type cpuTimes struct {
	total uint64
	idle  uint64
}

type cpuSampler struct {
	previous cpuTimes
	haveBase bool
}

func newCPUSampler() cpuSampler { return cpuSampler{} }

func (sampler *cpuSampler) sample() (*float64, workers.MetricStatus) {
	var idleTime, kernelTime, userTime windows.Filetime
	result, _, _ := getSystemTimes.Call(
		uintptr(unsafe.Pointer(&idleTime)),
		uintptr(unsafe.Pointer(&kernelTime)),
		uintptr(unsafe.Pointer(&userTime)),
	)
	if result == 0 {
		return nil, workers.MetricUnknown
	}
	current := cpuTimes{
		total: filetimeValue(kernelTime) + filetimeValue(userTime),
		idle:  filetimeValue(idleTime),
	}
	if !sampler.haveBase {
		sampler.previous = current
		sampler.haveBase = true
		return nil, workers.MetricUnknown
	}
	previous := sampler.previous
	sampler.previous = current
	if current.total <= previous.total || current.idle < previous.idle {
		return nil, workers.MetricUnknown
	}
	totalDelta := current.total - previous.total
	idleDelta := current.idle - previous.idle
	if idleDelta > totalDelta {
		return nil, workers.MetricUnknown
	}
	utilization := 1 - float64(idleDelta)/float64(totalDelta)
	return &utilization, workers.MetricCurrent
}

func filetimeValue(value windows.Filetime) uint64 {
	return uint64(value.HighDateTime)<<32 | uint64(value.LowDateTime)
}
