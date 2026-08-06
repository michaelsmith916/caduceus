package main

import (
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/caduceus/caduceus/internal/registry"
	"github.com/caduceus/caduceus/internal/scheduler"
	"github.com/caduceus/caduceus/internal/tasks"
	"github.com/caduceus/caduceus/internal/workers"
)

// writeRouteTable is deliberately a presentation-only projection. All
// eligibility and scoring values come from the immutable scheduler decision;
// helpers here only render the task-relative match state for operators.
func writeRouteTable(out io.Writer, req tasks.Request, candidates []scheduler.CandidateScore) error {
	table := tabwriter.NewWriter(out, 0, 2, 2, ' ', 0)
	_, _ = fmt.Fprintln(table, "WORKER\tHEALTH/STATE\tELIGIBLE\tCAPS\tMODEL\tRESIDENT\tTRUST\tCPU FREE\tRAM FREE\tGPU FREE\tQUEUE\tRUNNING\tTOK/S\tRTT\tCOST\tLRU\tSCORE\tRANK\tSELECTED\tNOTES")
	for _, candidate := range candidates {
		worker := candidate.Worker.Worker
		rank := "N/A"
		if candidate.Rank > 0 {
			rank = strconv.Itoa(candidate.Rank)
		}
		_, _ = fmt.Fprintf(table, "%s\t%s\t%t\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%t\t%s\n",
			valueOrUnknown(candidate.WorkerID),
			tableHealth(candidate.Worker),
			candidate.Eligible,
			capabilityMatch(req, worker),
			modelAvailability(req, candidate),
			modelResidency(req, worker),
			trustMatch(req, candidate),
			cpuHeadroom(worker.Resources),
			ramHeadroom(worker.Resources),
			gpuHeadroom(worker.Resources),
			formatIntPointer(worker.QueueDepth),
			formatIntPointer(worker.RunningTasks),
			tableThroughput(req, candidate),
			formatOptionalMetric(worker.RTTMillis, metricAvailability(candidate.MissingMetrics, scheduler.ComponentRTT)),
			formatOptionalMetric(worker.CostWeight, metricAvailability(candidate.MissingMetrics, scheduler.ComponentCost)),
			formatLastAssigned(candidate.LastAssigned),
			formatCandidateScore(candidate),
			rank,
			candidate.Selected,
			tableNotes(candidate),
		)
	}
	return table.Flush()
}

func tableHealth(snapshot registry.WorkerSnapshot) string {
	state := valueOrUnknown(string(snapshot.Worker.State))
	switch {
	case snapshot.Stale:
		return state + "/stale"
	case snapshot.Suspect:
		return state + "/suspect"
	case snapshot.Healthy:
		return state + "/healthy"
	case snapshot.Alive:
		return state + "/alive"
	default:
		return state + "/unknown"
	}
}

func capabilityMatch(req tasks.Request, worker workers.Worker) string {
	if len(req.Constraints.RequiredCapabilities) == 0 {
		return "N/A (not required)"
	}
	for _, capability := range req.Constraints.RequiredCapabilities {
		if !worker.HasCapability(capability) {
			return "no"
		}
	}
	return "yes"
}

func modelAvailability(req tasks.Request, candidate scheduler.CandidateScore) string {
	model := requestedModel(req)
	if model == "" {
		return "N/A (not required)"
	}
	if hasReason(candidate.FilterReasons, scheduler.ReasonModelUnknown) {
		return "N/A (unknown)"
	}
	if hasReason(candidate.FilterReasons, scheduler.ReasonModelUnavailable) {
		return "no"
	}
	if candidate.Worker.Worker.HasModel(model) {
		return "yes"
	}
	return "N/A (unknown)"
}

func modelResidency(req tasks.Request, worker workers.Worker) string {
	model := requestedModel(req)
	if model == "" {
		return "N/A (not required)"
	}
	if worker.HasLoadedModel(model) {
		return "yes"
	}
	return "no"
}

func trustMatch(req tasks.Request, candidate scheduler.CandidateScore) string {
	required := strings.TrimSpace(req.Constraints.RequiredTrustLevel)
	if required == "" {
		required = strings.TrimSpace(req.TrustLevel)
	}
	if required == "" {
		return "N/A (not required)"
	}
	if hasReason(candidate.FilterReasons, scheduler.ReasonInsufficientTrust) {
		return "no"
	}
	return "yes"
}

func requestedModel(req tasks.Request) string {
	if model := strings.TrimSpace(req.Constraints.RequiredModel); model != "" {
		return model
	}
	return strings.TrimSpace(req.Task.Model)
}

func cpuHeadroom(resources *workers.ResourceSnapshot) string {
	if resources == nil {
		return "N/A (unknown)"
	}
	return formatFloatMetric(resources.CPU.AvailableCapacity)
}

func ramHeadroom(resources *workers.ResourceSnapshot) string {
	if resources == nil {
		return "N/A (unknown)"
	}
	return formatIntMetric(resources.RAM.AvailableBytes)
}

func gpuHeadroom(resources *workers.ResourceSnapshot) string {
	if resources == nil {
		return "N/A (unknown)"
	}
	if len(resources.GPUs) == 0 {
		return "N/A (" + metricStatus(resources.GPUStatus) + ")"
	}
	var total int64
	status := workers.MetricCurrent
	unit := "bytes"
	for _, gpu := range resources.GPUs {
		metric := gpu.AvailableMemoryBytes
		if metric.Value == nil {
			return "N/A (" + metricStatus(metric.Status) + ")"
		}
		total += *metric.Value
		if metric.Status != workers.MetricCurrent {
			status = metric.Status
		}
		if metric.Unit != "" {
			unit = metric.Unit
		}
	}
	return fmt.Sprintf("%d %s (%s)", total, unit, metricStatus(status))
}

func tableThroughput(req tasks.Request, candidate scheduler.CandidateScore) string {
	worker := candidate.Worker.Worker
	model := requestedModel(req)
	if model != "" {
		measurement, ok := worker.ModelPerformance(model)
		if !ok || !measurement.Measured {
			return "N/A (unknown)"
		}
		return formatOptionalMetric(measurement.TokensPerSecond, metricAvailability(candidate.MissingMetrics, scheduler.ComponentThroughput))
	}
	if len(worker.Performance) == 0 {
		return "N/A (unknown)"
	}
	measurements := append([]workers.ModelPerformance(nil), worker.Performance...)
	sort.Slice(measurements, func(i, j int) bool { return measurements[i].Model < measurements[j].Model })
	if !measurements[0].Measured {
		return "N/A (unknown)"
	}
	return formatOptionalMetric(measurements[0].TokensPerSecond, metricAvailability(candidate.MissingMetrics, scheduler.ComponentThroughput))
}

func formatLastAssigned(value *time.Time) string {
	if value == nil {
		return "N/A (unknown)"
	}
	return formatTime(*value)
}

func tableNotes(candidate scheduler.CandidateScore) string {
	notes := append([]string(nil), candidate.FilterReasons...)
	notes = append(notes, candidate.Warnings...)
	return formatStringSet(notes, "none")
}

func hasReason(reasons []string, reason string) bool {
	for _, candidate := range reasons {
		if candidate == reason {
			return true
		}
	}
	return false
}

func formatCandidateScore(candidate scheduler.CandidateScore) string {
	if !candidate.Eligible {
		return "N/A (filtered)"
	}
	return formatFloat(candidate.TotalScore)
}
