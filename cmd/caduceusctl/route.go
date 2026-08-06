package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/caduceus/caduceus/internal/control"
	"github.com/caduceus/caduceus/internal/scheduler"
	"github.com/caduceus/caduceus/internal/tasks"
	"github.com/caduceus/caduceus/internal/workers"
)

const maxRouteRequestBytes = 1 << 20

var routeComponentOrder = []string{
	scheduler.ComponentThroughput,
	scheduler.ComponentLoad,
	scheduler.ComponentQueueDepth,
	scheduler.ComponentModelResidency,
	scheduler.ComponentRTT,
	scheduler.ComponentCost,
	scheduler.ComponentResourceHeadroom,
}

func routeCommand(ctx context.Context, opts cliOptions, args []string) error {
	fs := flag.NewFlagSet("route", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	explain := fs.Bool("explain", false, "explain routing without executing the task")
	output := fs.String("output", "human", "output format: human or json")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if !*explain {
		return errors.New("route requires --explain")
	}
	if fs.NArg() != 1 {
		return errors.New("route --explain requires exactly one task.json path or -")
	}
	format := strings.ToLower(strings.TrimSpace(*output))
	if opts.json {
		format = "json"
	}
	if format != "human" && format != "json" {
		return fmt.Errorf("unsupported route output %q (want human or json)", *output)
	}

	req, err := readRouteRequest(fs.Arg(0), os.Stdin)
	if err != nil {
		return err
	}
	return withClient(ctx, opts, func(c *control.Client) error {
		resp, err := c.ExplainRoute(ctx, req)
		if err != nil {
			return err
		}
		if format == "json" {
			return printResponse(resp, nil, true)
		}
		if !resp.OK {
			return printResponse(resp, nil, false)
		}
		decision, err := decodeRoutingDecision(resp.Data)
		if err != nil {
			return fmt.Errorf("decode route explanation: %w", err)
		}
		return writeRouteExplanationForRequest(os.Stdout, req, decision)
	})
}

func readRouteRequest(path string, stdin io.Reader) (tasks.Request, error) {
	var reader io.Reader
	var closer io.Closer
	if path == "-" {
		reader = stdin
	} else {
		file, err := os.Open(path)
		if err != nil {
			return tasks.Request{}, fmt.Errorf("open task request: %w", err)
		}
		reader, closer = file, file
	}
	if closer != nil {
		defer closer.Close()
	}

	data, err := io.ReadAll(io.LimitReader(reader, maxRouteRequestBytes+1))
	if err != nil {
		return tasks.Request{}, fmt.Errorf("read task request: %w", err)
	}
	if len(data) > maxRouteRequestBytes {
		return tasks.Request{}, fmt.Errorf("task request exceeds %d bytes", maxRouteRequestBytes)
	}
	var req tasks.Request
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		return tasks.Request{}, fmt.Errorf("decode task request: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return tasks.Request{}, errors.New("decode task request: multiple JSON values are not allowed")
		}
		return tasks.Request{}, fmt.Errorf("decode task request: %w", err)
	}
	if err := tasks.ValidateRequest(req); err != nil {
		return tasks.Request{}, fmt.Errorf("validate task request: %w", err)
	}
	return req, nil
}

func decodeRoutingDecision(value any) (scheduler.RoutingDecision, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return scheduler.RoutingDecision{}, err
	}
	var decision scheduler.RoutingDecision
	if err := json.Unmarshal(data, &decision); err != nil {
		return scheduler.RoutingDecision{}, err
	}
	return decision, nil
}

// writeRouteExplanation is a CLI presentation adapter. It canonicalizes all
// unordered scheduler data and never changes the scheduling model or decision.
func writeRouteExplanation(out io.Writer, decision scheduler.RoutingDecision) error {
	return writeRouteExplanationForRequest(out, tasks.Request{}, decision)
}

func writeRouteExplanationForRequest(out io.Writer, req tasks.Request, decision scheduler.RoutingDecision) error {
	w := bufio.NewWriter(out)
	line := func(format string, args ...any) {
		_, _ = fmt.Fprintf(w, format+"\n", args...)
	}

	line("ROUTE EXPLANATION")
	line("task: %s", valueOrUnknown(decision.TaskID))
	line("selection: %s", valueOrNA(decision.SelectedWorkerID, "none"))
	line("required_model: %s", valueOrNA(requestedModel(req), "not required"))
	line("required_capabilities: %s", formatStringSet(req.Constraints.RequiredCapabilities, "N/A (not required)"))
	line("required_trust: %s", valueOrNA(firstNonEmpty(req.Constraints.RequiredTrustLevel, req.TrustLevel), "not required"))
	line("snapshot: version=%d timestamp=%s", decision.RegistryVersion, formatTime(decision.RegistryTimestamp))
	line("weights: %s", formatWeights(decision.AppliedWeights))
	line("tie-break: %s", valueOrNA(decision.TieBreak, "none"))
	line("warnings: %s", formatStringSet(decision.Warnings, "none"))
	line("explanation: %s", valueOrNA(decision.Explanation, "none"))
	line("candidates: %d", len(decision.Candidates))
	candidates := canonicalCandidates(decision.Candidates)
	line("")
	if err := writeRouteTable(w, req, candidates); err != nil {
		return err
	}
	line("")
	line("CANDIDATE DETAILS")

	for _, candidate := range candidates {
		writeRouteCandidate(line, candidate)
	}
	return w.Flush()
}

func writeRouteCandidate(line func(string, ...any), candidate scheduler.CandidateScore) {
	worker := candidate.Worker.Worker
	rank := "N/A"
	if candidate.Rank > 0 {
		rank = strconv.Itoa(candidate.Rank)
	}
	selection := "not-selected"
	if candidate.Selected {
		selection = "SELECTED"
	}
	line("")
	line("[%s] worker=%s %s eligible=%t total_score=%s", rank, valueOrUnknown(candidate.WorkerID), selection, candidate.Eligible, formatCandidateScore(candidate))
	line("  identity: name=%s peer=%s session=%s protocol=%s trust=%s group=%s", valueOrUnknown(worker.Name), valueOrUnknown(worker.PeerID), valueOrUnknown(worker.SessionID), valueOrUnknown(worker.ProtocolVersion), valueOrUnknown(worker.TrustLevel), valueOrUnknown(worker.GroupHash))
	line("  health: state=%s alive=%t healthy=%t suspect=%t stale=%t schedulable=%t accepting=%s last_seen_age=%s", valueOrUnknown(string(worker.State)), candidate.Worker.Alive, candidate.Worker.Healthy, candidate.Worker.Suspect, candidate.Worker.Stale, candidate.Worker.Schedulable, formatBoolPointer(worker.AcceptingWork), candidate.Worker.LastSeenAge)
	line("  capabilities: llm=%t streaming=%t artifacts=%t models=%s loaded_models=%s tools=%s", worker.Capabilities.LLM, worker.Capabilities.Streaming, worker.Capabilities.Artifacts, formatStringSet(worker.Capabilities.Models, "N/A (none advertised)"), formatStringSet(worker.LoadedModels, "N/A (none advertised)"), formatStringSet(worker.Capabilities.Tools, "N/A (none advertised)"))
	line("  admission: running=%s/%s queue=%s/%s", formatIntPointer(worker.RunningTasks), formatEffectiveConcurrency(worker), formatIntPointer(worker.QueueDepth), formatIntPointer(worker.MaxQueueDepth))
	writeResources(line, worker.Resources)
	writePerformance(line, worker.Performance, candidate.MissingMetrics)
	line("  network: rtt_ms=%s updated=%s", formatOptionalMetric(worker.RTTMillis, metricAvailability(candidate.MissingMetrics, scheduler.ComponentRTT)), formatTime(worker.RTTUpdatedAt))
	line("  economics: cost_weight=%s", formatOptionalMetric(worker.CostWeight, metricAvailability(candidate.MissingMetrics, scheduler.ComponentCost)))
	if candidate.LastAssigned == nil {
		line("  lru: last_assigned=N/A (unknown)")
	} else {
		line("  lru: last_assigned=%s", formatTime(*candidate.LastAssigned))
	}
	line("  filter_reasons: %s", formatStringSet(candidate.FilterReasons, "none"))
	line("  missing_metrics: %s", formatStringSet(candidate.MissingMetrics, "none"))
	line("  warnings: %s", formatStringSet(candidate.Warnings, "none"))
	line("  tie-break: %s", valueOrNA(candidate.TieBreak, "none"))
	line("  components:")
	for _, name := range canonicalComponentNames(candidate.Components) {
		component, ok := candidate.Components[name]
		if !ok {
			line("    %s: N/A (unknown)", name)
			continue
		}
		line("    %s: available=%t score=%s weight=%s detail=%s", name, component.Available, formatFloat(component.Score), formatFloat(component.Weight), valueOrNA(component.Detail, "none"))
	}
}

func writeResources(line func(string, ...any), resources *workers.ResourceSnapshot) {
	if resources == nil {
		line("  resources: status=unknown collected=N/A (unknown)")
		line("    cpu: logical=N/A (unknown) available=N/A (unknown) utilization=N/A (unknown)")
		line("    ram: total=N/A (unknown) available=N/A (unknown)")
		line("    gpu: status=unknown devices=N/A (unknown)")
		return
	}
	line("  resources: status=%s collected=%s", metricStatus(resources.Status), formatTime(resources.CollectedAt))
	line("    cpu: logical=%s available=%s utilization=%s", formatIntMetric(resources.CPU.LogicalProcessors), formatFloatMetric(resources.CPU.AvailableCapacity), formatFloatMetric(resources.CPU.Utilization))
	line("    ram: total=%s available=%s", formatIntMetric(resources.RAM.TotalBytes), formatIntMetric(resources.RAM.AvailableBytes))
	if len(resources.GPUs) == 0 {
		line("    gpu: status=%s devices=N/A (none advertised)", metricStatus(resources.GPUStatus))
		return
	}
	line("    gpu: status=%s devices=%d", metricStatus(resources.GPUStatus), len(resources.GPUs))
	gpus := append([]workers.GPUResource(nil), resources.GPUs...)
	sort.Slice(gpus, func(i, j int) bool {
		left := gpus[i].DeviceID + "\x00" + gpus[i].Vendor + "\x00" + gpus[i].Model
		right := gpus[j].DeviceID + "\x00" + gpus[j].Vendor + "\x00" + gpus[j].Model
		return left < right
	})
	for _, gpu := range gpus {
		line("      device=%s vendor=%s model=%s total=%s available=%s utilization=%s runtime=%s capabilities=%s", valueOrUnknown(gpu.DeviceID), valueOrUnknown(gpu.Vendor), valueOrUnknown(gpu.Model), formatIntMetric(gpu.TotalMemoryBytes), formatIntMetric(gpu.AvailableMemoryBytes), formatFloatMetric(gpu.Utilization), valueOrUnknown(gpu.Runtime), formatStringSet(gpu.Capabilities, "N/A (none advertised)"))
	}
}

func writePerformance(line func(string, ...any), performance []workers.ModelPerformance, missing []string) {
	if len(performance) == 0 {
		line("  performance: tokens_per_second=N/A (unknown) samples=N/A updated=N/A (unknown)")
		return
	}
	items := append([]workers.ModelPerformance(nil), performance...)
	sort.Slice(items, func(i, j int) bool { return items[i].Model < items[j].Model })
	availability := metricAvailability(missing, scheduler.ComponentThroughput)
	for _, item := range items {
		value := formatOptionalMetric(item.TokensPerSecond, availability)
		if !item.Measured {
			value = "N/A (unknown; not measured)"
		}
		line("  performance: model=%s tokens_per_second=%s measured=%t samples=%d updated=%s", valueOrUnknown(item.Model), value, item.Measured, item.SampleCount, formatTime(item.UpdatedAt))
	}
}

func canonicalCandidates(input []scheduler.CandidateScore) []scheduler.CandidateScore {
	out := append([]scheduler.CandidateScore(nil), input...)
	sort.SliceStable(out, func(i, j int) bool {
		left, right := out[i], out[j]
		if left.Eligible != right.Eligible {
			return left.Eligible
		}
		leftRank, rightRank := left.Rank, right.Rank
		if leftRank == 0 {
			leftRank = int(^uint(0) >> 1)
		}
		if rightRank == 0 {
			rightRank = int(^uint(0) >> 1)
		}
		if leftRank != rightRank {
			return leftRank < rightRank
		}
		return left.WorkerID < right.WorkerID
	})
	return out
}

func canonicalComponentNames(components map[string]scheduler.ComponentScore) []string {
	seen := make(map[string]bool, len(routeComponentOrder))
	out := append([]string(nil), routeComponentOrder...)
	for _, name := range routeComponentOrder {
		seen[name] = true
	}
	extra := make([]string, 0)
	for name := range components {
		if !seen[name] {
			extra = append(extra, name)
		}
	}
	sort.Strings(extra)
	return append(out, extra...)
}

func formatWeights(weights scheduler.Weights) string {
	return fmt.Sprintf("throughput=%s load=%s queue_depth=%s model_residency=%s rtt=%s cost=%s resource_headroom=%s",
		formatFloat(weights.Throughput), formatFloat(weights.Load), formatFloat(weights.QueueDepth),
		formatFloat(weights.ModelResidency), formatFloat(weights.RTT), formatFloat(weights.Cost),
		formatFloat(weights.ResourceHeadroom))
}

func formatStringSet(values []string, empty string) string {
	if len(values) == 0 {
		return empty
	}
	copy := append([]string(nil), values...)
	sort.Strings(copy)
	return strings.Join(copy, ",")
}

func formatEffectiveConcurrency(worker workers.Worker) string {
	if value, ok := worker.EffectiveMaxConcurrentTasks(); ok {
		return strconv.Itoa(value)
	}
	return "N/A (unknown)"
}

func formatIntPointer(value *int) string {
	if value == nil {
		return "N/A (unknown)"
	}
	return strconv.Itoa(*value)
}

func formatBoolPointer(value *bool) string {
	if value == nil {
		return "N/A (unknown)"
	}
	return strconv.FormatBool(*value)
}

func formatIntMetric(metric workers.Int64Metric) string {
	status := metricStatus(metric.Status)
	if metric.Value == nil {
		return "N/A (" + status + ")"
	}
	return fmt.Sprintf("%d %s (%s)", *metric.Value, valueOrNA(metric.Unit, "units"), status)
}

func formatFloatMetric(metric workers.FloatMetric) string {
	status := metricStatus(metric.Status)
	if metric.Value == nil {
		return "N/A (" + status + ")"
	}
	return fmt.Sprintf("%s %s (%s)", formatFloat(*metric.Value), valueOrNA(metric.Unit, "units"), status)
}

func formatOptionalMetric(value *float64, availability string) string {
	if value == nil {
		return "N/A (unknown)"
	}
	return formatFloat(*value) + " (" + availability + ")"
}

func metricAvailability(missing []string, name string) string {
	for _, item := range missing {
		if item == name {
			return "unknown/stale"
		}
	}
	return "current"
}

func metricStatus(status workers.MetricStatus) string {
	if status == "" {
		return string(workers.MetricUnknown)
	}
	return string(status)
}

func formatFloat(value float64) string {
	return strconv.FormatFloat(value, 'f', 6, 64)
}

func formatTime(value time.Time) string {
	if value.IsZero() {
		return "N/A (unknown)"
	}
	return value.UTC().Format(time.RFC3339Nano)
}

func valueOrUnknown(value string) string {
	return valueOrNA(value, "unknown")
}

func valueOrNA(value, reason string) string {
	if strings.TrimSpace(value) == "" {
		return "N/A (" + reason + ")"
	}
	return value
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
