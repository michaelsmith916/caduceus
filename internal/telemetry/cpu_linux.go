//go:build linux

package telemetry

import (
	"os"
	"strconv"
	"strings"

	"github.com/caduceus/caduceus/internal/workers"
)

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
	current, ok := readCPUTimes()
	if !ok {
		return nil, workers.MetricUnknown
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

func readCPUTimes() (cpuTimes, bool) {
	contents, err := os.ReadFile("/proc/stat")
	if err != nil {
		return cpuTimes{}, false
	}
	line, _, _ := strings.Cut(string(contents), "\n")
	fields := strings.Fields(line)
	if len(fields) < 5 || fields[0] != "cpu" {
		return cpuTimes{}, false
	}
	var values []uint64
	for _, field := range fields[1:] {
		value, err := strconv.ParseUint(field, 10, 64)
		if err != nil {
			return cpuTimes{}, false
		}
		values = append(values, value)
	}
	var total uint64
	// guest and guest_nice are already included in user and nice. Linux
	// exposes them as fields 9 and 10, so summing them would double count.
	totalFields := len(values)
	if totalFields > 8 {
		totalFields = 8
	}
	for _, value := range values[:totalFields] {
		total += value
	}
	idle := values[3]
	if len(values) > 4 {
		idle += values[4]
	}
	return cpuTimes{total: total, idle: idle}, true
}
