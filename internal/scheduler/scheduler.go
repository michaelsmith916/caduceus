package scheduler

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/caduceus/caduceus/internal/registry"
	"github.com/caduceus/caduceus/internal/tasks"
	"github.com/caduceus/caduceus/internal/workers"
)

const (
	ComponentThroughput       = "throughput"
	ComponentLoad             = "load"
	ComponentQueueDepth       = "queue_depth"
	ComponentModelResidency   = "model_residency"
	ComponentRTT              = "rtt"
	ComponentCost             = "cost"
	ComponentResourceHeadroom = "resource_headroom"

	ReasonNotRequestedWorker  = "not_requested_worker"
	ReasonWorkerStale         = "worker_stale"
	ReasonWorkerOffline       = "worker_offline"
	ReasonWorkerNotAllowed    = "worker_not_allowed"
	ReasonWorkerDraining      = "worker_draining"
	ReasonWorkerUnavailable   = "worker_unavailable"
	ReasonWorkerNotAccepting  = "worker_not_accepting"
	ReasonInsufficientTrust   = "insufficient_trust"
	ReasonCapabilityMissing   = "capability_missing"
	ReasonModelUnknown        = "model_unknown"
	ReasonModelUnavailable    = "model_unavailable"
	ReasonAtCapacity          = "at_capacity"
	ReasonQueueFull           = "queue_full"
	ReasonCPUUnknown          = "cpu_unknown"
	ReasonInsufficientCPU     = "insufficient_cpu"
	ReasonRAMUnknown          = "ram_unknown"
	ReasonInsufficientRAM     = "insufficient_ram"
	ReasonGPUUnknown          = "gpu_unknown"
	ReasonGPURequired         = "gpu_required"
	ReasonGPUMismatch         = "gpu_mismatch"
	ReasonRuntimeMissing      = "runtime_missing"
	ReasonTaskWorkerAllowlist = "task_worker_not_allowed"
	ReasonTaskGroupAllowlist  = "task_group_not_allowed"
)

var componentOrder = [...]string{
	ComponentThroughput,
	ComponentLoad,
	ComponentQueueDepth,
	ComponentModelResidency,
	ComponentRTT,
	ComponentCost,
	ComponentResourceHeadroom,
}

type Weights struct {
	Throughput       float64 `json:"throughput" yaml:"throughput"`
	Load             float64 `json:"load" yaml:"load"`
	QueueDepth       float64 `json:"queue_depth" yaml:"queue_depth"`
	ModelResidency   float64 `json:"model_residency" yaml:"model_residency"`
	RTT              float64 `json:"rtt" yaml:"rtt"`
	Cost             float64 `json:"cost" yaml:"cost"`
	ResourceHeadroom float64 `json:"resource_headroom" yaml:"resource_headroom"`
}

func (w Weights) Map() map[string]float64 {
	return map[string]float64{
		ComponentThroughput:       w.Throughput,
		ComponentLoad:             w.Load,
		ComponentQueueDepth:       w.QueueDepth,
		ComponentModelResidency:   w.ModelResidency,
		ComponentRTT:              w.RTT,
		ComponentCost:             w.Cost,
		ComponentResourceHeadroom: w.ResourceHeadroom,
	}
}

type Config struct {
	Weights              Weights       `json:"weights" yaml:"weights"`
	MinimumMetricSamples int           `json:"minimum_metric_samples" yaml:"minimum_metric_samples"`
	MetricMaxAge         time.Duration `json:"metric_max_age" yaml:"metric_max_age"`
	ScoreEpsilon         float64       `json:"score_epsilon" yaml:"score_epsilon"`
}

func DefaultConfig() Config {
	return Config{
		Weights: Weights{
			Throughput:       1,
			Load:             1,
			QueueDepth:       1,
			ModelResidency:   1,
			RTT:              0.5,
			Cost:             0.5,
			ResourceHeadroom: 0.5,
		},
		MinimumMetricSamples: 3,
		MetricMaxAge:         15 * time.Minute,
		ScoreEpsilon:         1e-9,
	}
}

type Scheduler struct {
	config Config
}

type AssignmentHistory map[string]time.Time

type ComponentScore struct {
	Score     float64 `json:"score"`
	Weight    float64 `json:"weight"`
	Available bool    `json:"available"`
	Detail    string  `json:"detail,omitempty"`
}

type CandidateScore struct {
	WorkerID       string                    `json:"worker_id"`
	Worker         registry.WorkerSnapshot   `json:"worker"`
	Eligible       bool                      `json:"eligible"`
	FilterReasons  []string                  `json:"filter_reasons,omitempty"`
	MissingMetrics []string                  `json:"missing_metrics,omitempty"`
	Warnings       []string                  `json:"warnings,omitempty"`
	Components     map[string]ComponentScore `json:"components,omitempty"`
	TotalScore     float64                   `json:"total_score,omitempty"`
	Rank           int                       `json:"rank,omitempty"`
	Selected       bool                      `json:"selected,omitempty"`
	LastAssigned   *time.Time                `json:"last_assigned,omitempty"`
	TieBreak       string                    `json:"tie_break,omitempty"`
}

type RoutingDecision struct {
	TaskID            string           `json:"task_id,omitempty"`
	SelectedWorkerID  string           `json:"selected_worker_id,omitempty"`
	RegistryTimestamp time.Time        `json:"registry_timestamp"`
	RegistryVersion   uint64           `json:"registry_version"`
	Candidates        []CandidateScore `json:"candidates"`
	AppliedWeights    Weights          `json:"applied_weights"`
	Warnings          []string         `json:"warnings,omitempty"`
	TieBreak          string           `json:"tie_break,omitempty"`
	Explanation       string           `json:"explanation"`
}

type candidateWork struct {
	candidate  CandidateScore
	throughput *float64
	rtt        *float64
}

func New(config Config) (*Scheduler, error) {
	if config == (Config{}) {
		config = DefaultConfig()
	} else {
		if config.MetricMaxAge == 0 {
			config.MetricMaxAge = DefaultConfig().MetricMaxAge
		}
		if config.ScoreEpsilon == 0 {
			config.ScoreEpsilon = DefaultConfig().ScoreEpsilon
		}
	}
	if err := validateConfig(config); err != nil {
		return nil, err
	}
	return &Scheduler{config: config}, nil
}

func (s *Scheduler) Config() Config {
	return s.config
}

func (s *Scheduler) Rank(request tasks.Request, snapshot registry.Snapshot, history AssignmentHistory) ([]CandidateScore, error) {
	decision, err := s.evaluate(request, snapshot, history)
	if err != nil {
		return nil, err
	}
	return decision.Candidates, nil
}

func (s *Scheduler) Evaluate(request tasks.Request, snapshot registry.Snapshot, history AssignmentHistory) (RoutingDecision, error) {
	return s.evaluate(request, snapshot, history)
}

func (s *Scheduler) evaluate(request tasks.Request, snapshot registry.Snapshot, history AssignmentHistory) (RoutingDecision, error) {
	if s == nil {
		return RoutingDecision{}, errors.New("scheduler is nil")
	}
	if err := tasks.ValidateRequest(request); err != nil {
		return RoutingDecision{}, fmt.Errorf("validate task: %w", err)
	}

	work := make([]candidateWork, 0, len(snapshot.Workers))
	for _, workerSnapshot := range snapshot.Workers {
		candidate := CandidateScore{
			WorkerID:   workerSnapshot.Worker.WorkerID,
			Worker:     workerSnapshot,
			Eligible:   true,
			Components: make(map[string]ComponentScore, 7),
		}
		if assigned, ok := history[workerSnapshot.Worker.WorkerID]; ok && !assigned.IsZero() {
			copy := assigned
			candidate.LastAssigned = &copy
		}
		item := candidateWork{candidate: candidate}
		s.filter(&item.candidate, request, snapshot.Timestamp)
		if item.candidate.Eligible {
			s.scoreLocalComponents(&item, request, snapshot.Timestamp)
		}
		work = append(work, item)
	}

	s.scoreNormalizedComponents(work)
	for i := range work {
		if work[i].candidate.Eligible {
			work[i].candidate.TotalScore = s.total(work[i].candidate.Components)
		}
		work[i].candidate.FilterReasons = uniqueSorted(work[i].candidate.FilterReasons)
		work[i].candidate.MissingMetrics = uniqueSorted(work[i].candidate.MissingMetrics)
		work[i].candidate.Warnings = uniqueSorted(work[i].candidate.Warnings)
	}

	sort.Slice(work, func(i, j int) bool {
		left, right := work[i].candidate, work[j].candidate
		if left.Eligible != right.Eligible {
			return left.Eligible
		}
		if left.Eligible && left.TotalScore != right.TotalScore {
			return left.TotalScore > right.TotalScore
		}
		return left.WorkerID < right.WorkerID
	})
	for start := 0; start < len(work) && work[start].candidate.Eligible; {
		end := start + 1
		for end < len(work) && work[end].candidate.Eligible &&
			math.Abs(work[start].candidate.TotalScore-work[end].candidate.TotalScore) <= s.config.ScoreEpsilon {
			end++
		}
		sort.SliceStable(work[start:end], func(i, j int) bool {
			left, right := work[start+i].candidate, work[start+j].candidate
			if lessRecentlyAssigned(left.LastAssigned, right.LastAssigned) {
				return true
			}
			if lessRecentlyAssigned(right.LastAssigned, left.LastAssigned) {
				return false
			}
			return left.WorkerID < right.WorkerID
		})
		start = end
	}

	decision := RoutingDecision{
		TaskID:            request.TaskID,
		RegistryTimestamp: snapshot.Timestamp,
		RegistryVersion:   snapshot.Version,
		AppliedWeights:    s.config.Weights,
		Candidates:        make([]CandidateScore, len(work)),
	}
	rank := 0
	for i := range work {
		candidate := work[i].candidate
		if candidate.Eligible {
			rank++
			candidate.Rank = rank
		}
		decision.Candidates[i] = candidate
	}
	if rank == 0 {
		decision.Explanation = "No worker satisfied all hard scheduling requirements."
		if requested := explicitWorkerID(request.WorkerID); requested != "" && !snapshotContains(snapshot, requested) {
			decision.Warnings = append(decision.Warnings, "requested worker was not present in the registry snapshot")
		}
		return decision, nil
	}

	decision.Candidates[0].Selected = true
	decision.SelectedWorkerID = decision.Candidates[0].Worker.Worker.WorkerID
	decision.Explanation = selectionExplanation(decision.Candidates[0], request)
	if len(decision.Candidates) > 1 && decision.Candidates[1].Eligible &&
		math.Abs(decision.Candidates[0].TotalScore-decision.Candidates[1].TotalScore) <= s.config.ScoreEpsilon {
		if !sameAssignmentTime(decision.Candidates[0].LastAssigned, decision.Candidates[1].LastAssigned) {
			decision.TieBreak = "least_recently_assigned"
		} else {
			decision.TieBreak = "worker_id"
		}
		decision.Candidates[0].TieBreak = decision.TieBreak
	}
	for _, candidate := range decision.Candidates {
		for _, metric := range candidate.MissingMetrics {
			decision.Warnings = append(decision.Warnings, candidate.Worker.Worker.WorkerID+": "+metric+" unavailable or stale")
		}
	}
	decision.Warnings = uniqueSorted(decision.Warnings)
	return decision, nil
}

func (s *Scheduler) filter(candidate *CandidateScore, request tasks.Request, now time.Time) {
	workerSnapshot := candidate.Worker
	worker := workerSnapshot.Worker
	addReason := func(reason string) {
		candidate.FilterReasons = append(candidate.FilterReasons, reason)
		candidate.Eligible = false
	}

	if requested := explicitWorkerID(request.WorkerID); requested != "" && worker.WorkerID != requested {
		addReason(ReasonNotRequestedWorker)
	}
	if !workerSnapshot.Alive {
		addReason(ReasonWorkerOffline)
	} else if !workerSnapshot.Healthy || workerSnapshot.Stale || workerSnapshot.Suspect {
		addReason(ReasonWorkerStale)
	}
	if !worker.Allowed {
		addReason(ReasonWorkerNotAllowed)
	}
	switch worker.State {
	case "", workers.StateAvailable, workers.StateBusy:
		if worker.AcceptingWork != nil && !*worker.AcceptingWork {
			addReason(ReasonWorkerNotAccepting)
		}
	case workers.StateDraining:
		addReason(ReasonWorkerDraining)
	case workers.StateOffline:
		addReason(ReasonWorkerOffline)
	default:
		addReason(ReasonWorkerUnavailable)
	}

	if len(request.Constraints.AllowedWorkerIDs) > 0 && !containsExact(request.Constraints.AllowedWorkerIDs, worker.WorkerID) {
		addReason(ReasonTaskWorkerAllowlist)
	}
	if len(request.Constraints.AllowedGroups) > 0 && !workerInGroup(worker, request.Constraints.AllowedGroups) {
		addReason(ReasonTaskGroupAllowlist)
	}

	requiredTrust := strings.TrimSpace(request.Constraints.RequiredTrustLevel)
	if requiredTrust == "" {
		requiredTrust = strings.TrimSpace(request.TrustLevel)
	}
	if requiredTrust != "" && worker.TrustLevel != requiredTrust {
		addReason(ReasonInsufficientTrust + ":" + requiredTrust)
	}

	requiredCapabilities := append([]string(nil), request.Constraints.RequiredCapabilities...)
	if request.Kind == "" || request.Kind == tasks.KindPrompt {
		requiredCapabilities = append(requiredCapabilities, "llm")
	}
	if request.Task.Stream {
		requiredCapabilities = append(requiredCapabilities, "streaming")
	}
	for _, capability := range uniqueSorted(requiredCapabilities) {
		if !worker.HasCapability(capability) {
			addReason(ReasonCapabilityMissing + ":" + capability)
		}
	}

	model := requiredModel(request)
	if model != "" {
		if len(worker.Capabilities.Models) == 0 {
			addReason(ReasonModelUnknown + ":" + model)
		} else if !worker.HasModel(model) {
			addReason(ReasonModelUnavailable + ":" + model)
		}
	}

	maximum, maximumKnown := worker.EffectiveMaxConcurrentTasks()
	if maximumKnown && maximum <= 0 {
		addReason(ReasonAtCapacity)
	} else if maximumKnown && worker.RunningTasks != nil && *worker.RunningTasks >= maximum {
		if worker.MaxQueueDepth != nil && *worker.MaxQueueDepth > 0 {
			if worker.QueueDepth == nil {
				candidate.MissingMetrics = append(candidate.MissingMetrics, "queue_depth")
			} else if *worker.QueueDepth >= *worker.MaxQueueDepth {
				addReason(ReasonQueueFull)
			}
		} else {
			addReason(ReasonAtCapacity)
		}
	} else {
		if !maximumKnown {
			candidate.MissingMetrics = append(candidate.MissingMetrics, "max_concurrent_tasks")
		}
		if worker.RunningTasks == nil {
			candidate.MissingMetrics = append(candidate.MissingMetrics, "running_tasks")
		}
	}

	s.filterResources(candidate, request, now)
}

func (s *Scheduler) filterResources(candidate *CandidateScore, request tasks.Request, now time.Time) {
	constraints := request.Constraints
	worker := candidate.Worker.Worker
	fresh := resourcesFresh(worker.Resources, now, s.config.MetricMaxAge)
	addReason := func(reason string) {
		candidate.FilterReasons = append(candidate.FilterReasons, reason)
		candidate.Eligible = false
	}

	if constraints.MinCPU > 0 {
		value, ok := currentFloat(worker.Resources, func(resources *workers.ResourceSnapshot) workers.FloatMetric {
			return resources.CPU.AvailableCapacity
		}, fresh, now, s.config.MetricMaxAge)
		if !ok {
			addReason(ReasonCPUUnknown)
		} else if value < constraints.MinCPU {
			addReason(ReasonInsufficientCPU)
		}
	}
	if constraints.MinRAMBytes > 0 {
		value, ok := currentInt(worker.Resources, func(resources *workers.ResourceSnapshot) workers.Int64Metric {
			return resources.RAM.AvailableBytes
		}, fresh, now, s.config.MetricMaxAge)
		if !ok {
			addReason(ReasonRAMUnknown)
		} else if uint64(value) < constraints.MinRAMBytes {
			addReason(ReasonInsufficientRAM)
		}
	}

	needsGPU := constraints.GPURequired || constraints.GPUVendor != "" ||
		constraints.MinGPUMemoryBytes > 0 || len(constraints.RequiredGPUCapabilities) > 0
	if needsGPU {
		if !fresh || worker.Resources.GPUStatus != workers.MetricCurrent {
			addReason(ReasonGPUUnknown)
		} else if len(worker.Resources.GPUs) == 0 {
			addReason(ReasonGPURequired)
		} else if !matchingGPU(worker.Resources, constraints, now, s.config.MetricMaxAge) {
			addReason(ReasonGPUMismatch)
		}
	}
	if runtime := strings.TrimSpace(constraints.RequiredRuntime); runtime != "" && !hasRuntime(worker, runtime) {
		addReason(ReasonRuntimeMissing + ":" + runtime)
	}
}

func (s *Scheduler) scoreLocalComponents(item *candidateWork, request tasks.Request, now time.Time) {
	worker := item.candidate.Worker.Worker
	model := scoreModel(request)
	weights := s.config.Weights

	if measurement, ok := worker.ModelPerformance(model); ok &&
		measurement.Measured &&
		measurement.SampleCount >= s.config.MinimumMetricSamples &&
		workers.ValidPositiveFloat(measurement.TokensPerSecond) &&
		metricTimeFresh(measurement.UpdatedAt, now, s.config.MetricMaxAge) {
		value := *measurement.TokensPerSecond
		item.throughput = &value
	} else {
		item.candidate.MissingMetrics = append(item.candidate.MissingMetrics, ComponentThroughput)
		if ok && measurement.SampleCount < s.config.MinimumMetricSamples {
			item.candidate.Warnings = append(item.candidate.Warnings, "throughput sample count below minimum")
		}
	}
	item.candidate.Components[ComponentThroughput] = neutralComponent(weights.Throughput, item.throughput != nil, "normalized measured tokens per second")

	if worker.RunningTasks != nil {
		if maximum, ok := worker.EffectiveMaxConcurrentTasks(); ok && maximum > 0 {
			score := clamp01(1 - float64(*worker.RunningTasks)/float64(maximum))
			item.candidate.Components[ComponentLoad] = ComponentScore{
				Score: score, Weight: weights.Load, Available: true,
				Detail: fmt.Sprintf("%d running of %d", *worker.RunningTasks, maximum),
			}
		} else {
			item.candidate.Components[ComponentLoad] = neutralComponent(weights.Load, false, "maximum concurrency unknown")
			item.candidate.MissingMetrics = append(item.candidate.MissingMetrics, ComponentLoad)
		}
	} else {
		item.candidate.Components[ComponentLoad] = neutralComponent(weights.Load, false, "running task count unknown")
		item.candidate.MissingMetrics = append(item.candidate.MissingMetrics, ComponentLoad)
	}

	if worker.QueueDepth != nil {
		score := 1 / (1 + float64(*worker.QueueDepth))
		detail := fmt.Sprintf("queue depth %d", *worker.QueueDepth)
		if worker.MaxQueueDepth != nil && *worker.MaxQueueDepth > 0 {
			score = clamp01(1 - float64(*worker.QueueDepth)/float64(*worker.MaxQueueDepth))
			detail = fmt.Sprintf("queue depth %d of %d", *worker.QueueDepth, *worker.MaxQueueDepth)
		}
		item.candidate.Components[ComponentQueueDepth] = ComponentScore{
			Score: score, Weight: weights.QueueDepth, Available: true, Detail: detail,
		}
	} else {
		item.candidate.Components[ComponentQueueDepth] = neutralComponent(weights.QueueDepth, false, "queue depth unknown")
		item.candidate.MissingMetrics = append(item.candidate.MissingMetrics, ComponentQueueDepth)
	}

	if model == "" {
		item.candidate.Components[ComponentModelResidency] = neutralComponent(weights.ModelResidency, false, "no requested or preferred model")
	} else if worker.HasLoadedModel(model) {
		item.candidate.Components[ComponentModelResidency] = ComponentScore{
			Score: 1, Weight: weights.ModelResidency, Available: true, Detail: "requested model is resident",
		}
	} else {
		item.candidate.Components[ComponentModelResidency] = ComponentScore{
			Score: 0, Weight: weights.ModelResidency, Available: true, Detail: "model is available but not resident",
		}
	}

	if worker.RTTMillis != nil && *worker.RTTMillis >= 0 &&
		!math.IsNaN(*worker.RTTMillis) && !math.IsInf(*worker.RTTMillis, 0) &&
		metricTimeFresh(worker.RTTUpdatedAt, now, s.config.MetricMaxAge) {
		value := *worker.RTTMillis
		item.rtt = &value
	} else {
		item.candidate.MissingMetrics = append(item.candidate.MissingMetrics, ComponentRTT)
	}
	item.candidate.Components[ComponentRTT] = neutralComponent(weights.RTT, item.rtt != nil, "normalized coordinator-measured RTT")

	if workers.ValidPositiveFloat(worker.CostWeight) {
		score := 1 / (1 + *worker.CostWeight)
		item.candidate.Components[ComponentCost] = ComponentScore{
			Score: score, Weight: weights.Cost, Available: true,
			Detail: fmt.Sprintf("declared cost weight %.4g", *worker.CostWeight),
		}
	} else {
		item.candidate.Components[ComponentCost] = neutralComponent(weights.Cost, false, "cost weight unknown or invalid")
		item.candidate.MissingMetrics = append(item.candidate.MissingMetrics, ComponentCost)
	}

	if score, detail, ok := s.resourceHeadroom(worker, now); ok {
		item.candidate.Components[ComponentResourceHeadroom] = ComponentScore{
			Score: score, Weight: weights.ResourceHeadroom, Available: true, Detail: detail,
		}
	} else {
		item.candidate.Components[ComponentResourceHeadroom] = neutralComponent(weights.ResourceHeadroom, false, "resource headroom unavailable or stale")
		item.candidate.MissingMetrics = append(item.candidate.MissingMetrics, ComponentResourceHeadroom)
	}
}

func (s *Scheduler) scoreNormalizedComponents(work []candidateWork) {
	var throughputValues, rttValues []float64
	for _, item := range work {
		if !item.candidate.Eligible {
			continue
		}
		if item.throughput != nil {
			throughputValues = append(throughputValues, *item.throughput)
		}
		if item.rtt != nil {
			rttValues = append(rttValues, *item.rtt)
		}
	}
	throughputMin, throughputMax := bounds(throughputValues)
	rttMin, rttMax := bounds(rttValues)
	for i := range work {
		if !work[i].candidate.Eligible {
			continue
		}
		if work[i].throughput != nil {
			component := work[i].candidate.Components[ComponentThroughput]
			component.Score = normalizeHigher(*work[i].throughput, throughputMin, throughputMax)
			component.Available = true
			component.Detail = fmt.Sprintf("%.4g measured tokens/sec", *work[i].throughput)
			work[i].candidate.Components[ComponentThroughput] = component
		}
		if work[i].rtt != nil {
			component := work[i].candidate.Components[ComponentRTT]
			component.Score = normalizeLower(*work[i].rtt, rttMin, rttMax)
			component.Available = true
			component.Detail = fmt.Sprintf("%.4g ms coordinator-measured RTT", *work[i].rtt)
			work[i].candidate.Components[ComponentRTT] = component
		}
	}
}

func (s *Scheduler) resourceHeadroom(worker workers.Worker, now time.Time) (float64, string, bool) {
	if !resourcesFresh(worker.Resources, now, s.config.MetricMaxAge) {
		return 0, "", false
	}
	var scores []float64
	if available, ok := currentFloat(worker.Resources, func(resources *workers.ResourceSnapshot) workers.FloatMetric {
		return resources.CPU.AvailableCapacity
	}, true, now, s.config.MetricMaxAge); ok {
		if total, totalOK := currentInt(worker.Resources, func(resources *workers.ResourceSnapshot) workers.Int64Metric {
			return resources.CPU.LogicalProcessors
		}, true, now, s.config.MetricMaxAge); totalOK && total > 0 {
			scores = append(scores, clamp01(available/float64(total)))
		}
	}
	if available, ok := currentInt(worker.Resources, func(resources *workers.ResourceSnapshot) workers.Int64Metric {
		return resources.RAM.AvailableBytes
	}, true, now, s.config.MetricMaxAge); ok {
		if total, totalOK := currentInt(worker.Resources, func(resources *workers.ResourceSnapshot) workers.Int64Metric {
			return resources.RAM.TotalBytes
		}, true, now, s.config.MetricMaxAge); totalOK && total > 0 {
			scores = append(scores, clamp01(float64(available)/float64(total)))
		}
	}
	bestGPU := -1.0
	if worker.Resources.GPUStatus == workers.MetricCurrent {
		for _, gpu := range worker.Resources.GPUs {
			available, availableOK := currentIntMetric(gpu.AvailableMemoryBytes, worker.Resources.CollectedAt, now, s.config.MetricMaxAge)
			total, totalOK := currentIntMetric(gpu.TotalMemoryBytes, worker.Resources.CollectedAt, now, s.config.MetricMaxAge)
			if availableOK && totalOK && total > 0 {
				bestGPU = math.Max(bestGPU, clamp01(float64(available)/float64(total)))
			}
		}
		if bestGPU >= 0 {
			scores = append(scores, bestGPU)
		}
	}
	if len(scores) == 0 {
		return 0, "", false
	}
	var sum float64
	for _, score := range scores {
		sum += score
	}
	return sum / float64(len(scores)), fmt.Sprintf("average of %d reported headroom ratios", len(scores)), true
}

func (s *Scheduler) total(components map[string]ComponentScore) float64 {
	var weighted, totalWeight float64
	for _, name := range componentOrder {
		component := components[name]
		weighted += component.Score * component.Weight
		totalWeight += component.Weight
	}
	if totalWeight == 0 {
		return 0
	}
	return clamp01(weighted / totalWeight)
}

func validateConfig(config Config) error {
	if config.MinimumMetricSamples < 0 {
		return errors.New("minimum metric samples must be non-negative")
	}
	if config.MetricMaxAge < 0 {
		return errors.New("metric max age must be non-negative")
	}
	if config.ScoreEpsilon <= 0 || math.IsNaN(config.ScoreEpsilon) || math.IsInf(config.ScoreEpsilon, 0) {
		return errors.New("score epsilon must be finite and positive")
	}
	var total float64
	weights := config.Weights.Map()
	for _, name := range componentOrder {
		weight := weights[name]
		if weight < 0 || math.IsNaN(weight) || math.IsInf(weight, 0) {
			return fmt.Errorf("%s weight must be finite and non-negative", name)
		}
		total += weight
	}
	if total == 0 {
		return errors.New("at least one scheduler weight must be positive")
	}
	return nil
}

func requiredModel(request tasks.Request) string {
	if model := strings.TrimSpace(request.Constraints.RequiredModel); model != "" {
		return model
	}
	return strings.TrimSpace(request.Task.Model)
}

func scoreModel(request tasks.Request) string {
	if model := requiredModel(request); model != "" {
		return model
	}
	for _, model := range request.Constraints.PreferredModels {
		if model = strings.TrimSpace(model); model != "" {
			return model
		}
	}
	return ""
}

func explicitWorkerID(workerID string) string {
	workerID = strings.TrimSpace(workerID)
	if workerID == "" || strings.EqualFold(workerID, "auto") {
		return ""
	}
	return workerID
}

func workerInGroup(worker workers.Worker, allowed []string) bool {
	for _, group := range allowed {
		group = strings.TrimSpace(group)
		if group == "" {
			continue
		}
		if worker.GroupHash == group {
			return true
		}
	}
	return false
}

func matchingGPU(resources *workers.ResourceSnapshot, constraints tasks.Constraints, now time.Time, maxAge time.Duration) bool {
	for _, gpu := range resources.GPUs {
		if constraints.GPUVendor != "" && !strings.EqualFold(strings.TrimSpace(gpu.Vendor), strings.TrimSpace(constraints.GPUVendor)) {
			continue
		}
		if constraints.MinGPUMemoryBytes > 0 {
			available, ok := currentIntMetric(gpu.AvailableMemoryBytes, resources.CollectedAt, now, maxAge)
			if !ok || uint64(available) < constraints.MinGPUMemoryBytes {
				continue
			}
		}
		matches := true
		for _, required := range constraints.RequiredGPUCapabilities {
			if !containsFold(gpu.Capabilities, required) {
				matches = false
				break
			}
		}
		if matches {
			return true
		}
	}
	return false
}

func hasRuntime(worker workers.Worker, runtime string) bool {
	if worker.Resources != nil {
		for _, gpu := range worker.Resources.GPUs {
			if strings.EqualFold(strings.TrimSpace(gpu.Runtime), runtime) {
				return true
			}
		}
	}
	return containsFold(worker.Capabilities.Tools, runtime)
}

func resourcesFresh(resources *workers.ResourceSnapshot, now time.Time, maxAge time.Duration) bool {
	return resources != nil &&
		resources.Status == workers.MetricCurrent &&
		metricTimeFresh(resources.CollectedAt, now, maxAge)
}

func currentInt(resources *workers.ResourceSnapshot, selectMetric func(*workers.ResourceSnapshot) workers.Int64Metric, resourcesAreFresh bool, now time.Time, maxAge time.Duration) (int64, bool) {
	if resources == nil || !resourcesAreFresh {
		return 0, false
	}
	return currentIntMetric(selectMetric(resources), resources.CollectedAt, now, maxAge)
}

func currentFloat(resources *workers.ResourceSnapshot, selectMetric func(*workers.ResourceSnapshot) workers.FloatMetric, resourcesAreFresh bool, now time.Time, maxAge time.Duration) (float64, bool) {
	if resources == nil || !resourcesAreFresh {
		return 0, false
	}
	metric := selectMetric(resources)
	if metric.Status != workers.MetricCurrent || metric.Value == nil ||
		*metric.Value < 0 || math.IsNaN(*metric.Value) || math.IsInf(*metric.Value, 0) {
		return 0, false
	}
	collectedAt := metric.CollectedAt
	if collectedAt.IsZero() {
		collectedAt = resources.CollectedAt
	}
	if !metricTimeFresh(collectedAt, now, maxAge) {
		return 0, false
	}
	return *metric.Value, true
}

func currentIntMetric(metric workers.Int64Metric, inheritedTime, now time.Time, maxAge time.Duration) (int64, bool) {
	if metric.Status != workers.MetricCurrent || metric.Value == nil || *metric.Value < 0 {
		return 0, false
	}
	collectedAt := metric.CollectedAt
	if collectedAt.IsZero() {
		collectedAt = inheritedTime
	}
	if !metricTimeFresh(collectedAt, now, maxAge) {
		return 0, false
	}
	return *metric.Value, true
}

func metricTimeFresh(collectedAt, now time.Time, maxAge time.Duration) bool {
	if collectedAt.IsZero() {
		return false
	}
	if collectedAt.After(now) {
		return collectedAt.Sub(now) <= 5*time.Minute
	}
	return maxAge == 0 || now.Sub(collectedAt) <= maxAge
}

func neutralComponent(weight float64, available bool, detail string) ComponentScore {
	return ComponentScore{Score: 0.5, Weight: weight, Available: available, Detail: detail}
}

func bounds(values []float64) (float64, float64) {
	if len(values) == 0 {
		return 0, 0
	}
	minimum, maximum := values[0], values[0]
	for _, value := range values[1:] {
		minimum = math.Min(minimum, value)
		maximum = math.Max(maximum, value)
	}
	return minimum, maximum
}

func normalizeHigher(value, minimum, maximum float64) float64 {
	if maximum <= minimum {
		return 0.5
	}
	return clamp01((value - minimum) / (maximum - minimum))
}

func normalizeLower(value, minimum, maximum float64) float64 {
	return 1 - normalizeHigher(value, minimum, maximum)
}

func clamp01(value float64) float64 {
	return math.Max(0, math.Min(1, value))
}

func lessRecentlyAssigned(left, right *time.Time) bool {
	if left == nil || left.IsZero() {
		return right != nil && !right.IsZero()
	}
	if right == nil || right.IsZero() {
		return false
	}
	return left.Before(*right)
}

func sameAssignmentTime(left, right *time.Time) bool {
	if left == nil || left.IsZero() {
		return right == nil || right.IsZero()
	}
	return right != nil && !right.IsZero() && left.Equal(*right)
}

func snapshotContains(snapshot registry.Snapshot, workerID string) bool {
	for _, worker := range snapshot.Workers {
		if worker.Worker.WorkerID == workerID {
			return true
		}
	}
	return false
}

func selectionExplanation(candidate CandidateScore, request tasks.Request) string {
	reasons := []string{"highest weighted score"}
	if model := scoreModel(request); model != "" && candidate.Worker.Worker.HasLoadedModel(model) {
		reasons = append(reasons, "requested model is resident")
	}
	if component, ok := candidate.Components[ComponentThroughput]; ok && component.Available && component.Score > 0.5 {
		reasons = append(reasons, "measured throughput is strong relative to eligible workers")
	}
	return "Selected " + candidate.Worker.Worker.WorkerID + ": " + strings.Join(reasons, "; ") + "."
}

func uniqueSorted(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			set[value] = struct{}{}
		}
	}
	out := make([]string, 0, len(set))
	for value := range set {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func containsExact(values []string, target string) bool {
	for _, value := range values {
		if strings.TrimSpace(value) == strings.TrimSpace(target) {
			return true
		}
	}
	return false
}

func containsFold(values []string, target string) bool {
	for _, value := range values {
		if strings.EqualFold(strings.TrimSpace(value), strings.TrimSpace(target)) {
			return true
		}
	}
	return false
}
