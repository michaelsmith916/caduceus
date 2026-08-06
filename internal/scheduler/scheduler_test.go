package scheduler

import (
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/caduceus/caduceus/internal/registry"
	"github.com/caduceus/caduceus/internal/tasks"
	"github.com/caduceus/caduceus/internal/workers"
)

func intPointer(value int) *int           { return &value }
func int64Pointer(value int64) *int64     { return &value }
func floatPointer(value float64) *float64 { return &value }
func boolPointer(value bool) *bool        { return &value }

func testScheduler(t *testing.T, weights Weights) *Scheduler {
	t.Helper()
	scheduler, err := New(Config{
		Weights:              weights,
		MinimumMetricSamples: 3,
		MetricMaxAge:         time.Hour,
		ScoreEpsilon:         1e-9,
	})
	if err != nil {
		t.Fatal(err)
	}
	return scheduler
}

func baseRequest() tasks.Request {
	return tasks.Request{
		TaskID:     "task-1",
		Kind:       tasks.KindPrompt,
		TrustLevel: "trusted-lan",
		Task: tasks.PromptTask{
			Prompt: "hello",
			Model:  "model-a",
		},
		Constraints: tasks.Constraints{
			RequiredCapabilities: []string{"llm"},
		},
	}
}

func baseWorker(id string, now time.Time) workers.Worker {
	totalRAM := int64(16 << 30)
	availableRAM := int64(12 << 30)
	logicalCPU := int64(8)
	availableCPU := 6.0
	totalGPU := int64(24 << 30)
	availableGPU := int64(20 << 30)
	throughput := 40.0
	return workers.Worker{
		WorkerID:           id,
		PeerID:             "peer-" + id,
		SessionID:          "session-" + id,
		Allowed:            true,
		TrustLevel:         "trusted-lan",
		State:              workers.StateAvailable,
		AcceptingWork:      boolPointer(true),
		RunningTasks:       intPointer(0),
		MaxConcurrentTasks: intPointer(2),
		QueueDepth:         intPointer(0),
		MaxQueueDepth:      intPointer(4),
		LoadedModels:       []string{"model-a"},
		CostWeight:         floatPointer(1),
		RTTMillis:          floatPointer(5),
		RTTUpdatedAt:       now,
		Capabilities: workers.Capabilities{
			LLM:                true,
			Streaming:          true,
			Artifacts:          true,
			Models:             []string{"model-a", "model-b"},
			MaxConcurrentTasks: 2,
			Tools:              []string{"cuda"},
		},
		Resources: &workers.ResourceSnapshot{
			Status:      workers.MetricCurrent,
			GPUStatus:   workers.MetricCurrent,
			CollectedAt: now,
			CPU: workers.CPUResources{
				LogicalProcessors: workers.Int64Metric{Value: &logicalCPU, Status: workers.MetricCurrent},
				AvailableCapacity: workers.FloatMetric{Value: &availableCPU, Status: workers.MetricCurrent},
			},
			RAM: workers.MemoryResources{
				TotalBytes:     workers.Int64Metric{Value: &totalRAM, Status: workers.MetricCurrent},
				AvailableBytes: workers.Int64Metric{Value: &availableRAM, Status: workers.MetricCurrent},
			},
			GPUs: []workers.GPUResource{{
				Vendor:       "NVIDIA",
				Model:        "RTX",
				Runtime:      "cuda",
				Capabilities: []string{"cuda", "fp16"},
				TotalMemoryBytes: workers.Int64Metric{
					Value: &totalGPU, Status: workers.MetricCurrent,
				},
				AvailableMemoryBytes: workers.Int64Metric{
					Value: &availableGPU, Status: workers.MetricCurrent,
				},
			}},
		},
		Performance: []workers.ModelPerformance{{
			Model:           "model-a",
			TokensPerSecond: &throughput,
			SampleCount:     10,
			UpdatedAt:       now,
			Measured:        true,
		}},
	}
}

func workerSnapshot(worker workers.Worker) registry.WorkerSnapshot {
	return registry.WorkerSnapshot{
		Worker:      worker,
		Alive:       true,
		Healthy:     true,
		Schedulable: true,
	}
}

func testSnapshot(now time.Time, values ...registry.WorkerSnapshot) registry.Snapshot {
	return registry.Snapshot{Timestamp: now, Version: 7, Workers: values}
}

func TestHardFilters(t *testing.T) {
	now := time.Date(2026, 8, 5, 18, 0, 0, 0, time.UTC)
	tests := []struct {
		name     string
		mutate   func(*registry.WorkerSnapshot, *tasks.Request)
		expected string
	}{
		{
			name: "stale",
			mutate: func(snapshot *registry.WorkerSnapshot, _ *tasks.Request) {
				snapshot.Healthy, snapshot.Stale, snapshot.Suspect = false, true, true
			},
			expected: ReasonWorkerStale,
		},
		{
			name: "not allowed",
			mutate: func(snapshot *registry.WorkerSnapshot, _ *tasks.Request) {
				snapshot.Worker.Allowed = false
			},
			expected: ReasonWorkerNotAllowed,
		},
		{
			name: "draining",
			mutate: func(snapshot *registry.WorkerSnapshot, _ *tasks.Request) {
				snapshot.Worker.State = workers.StateDraining
			},
			expected: ReasonWorkerDraining,
		},
		{
			name: "not accepting",
			mutate: func(snapshot *registry.WorkerSnapshot, _ *tasks.Request) {
				snapshot.Worker.AcceptingWork = boolPointer(false)
			},
			expected: ReasonWorkerNotAccepting,
		},
		{
			name: "trust",
			mutate: func(snapshot *registry.WorkerSnapshot, _ *tasks.Request) {
				snapshot.Worker.TrustLevel = "untrusted"
			},
			expected: ReasonInsufficientTrust,
		},
		{
			name: "capability",
			mutate: func(snapshot *registry.WorkerSnapshot, _ *tasks.Request) {
				snapshot.Worker.Capabilities.LLM = false
			},
			expected: ReasonCapabilityMissing,
		},
		{
			name: "model unavailable",
			mutate: func(snapshot *registry.WorkerSnapshot, _ *tasks.Request) {
				snapshot.Worker.Capabilities.Models = []string{"other"}
			},
			expected: ReasonModelUnavailable,
		},
		{
			name: "model unknown",
			mutate: func(snapshot *registry.WorkerSnapshot, _ *tasks.Request) {
				snapshot.Worker.Capabilities.Models = nil
			},
			expected: ReasonModelUnknown,
		},
		{
			name: "explicit worker",
			mutate: func(_ *registry.WorkerSnapshot, request *tasks.Request) {
				request.WorkerID = "other-worker"
			},
			expected: ReasonNotRequestedWorker,
		},
		{
			name: "task worker allowlist",
			mutate: func(_ *registry.WorkerSnapshot, request *tasks.Request) {
				request.Constraints.AllowedWorkerIDs = []string{"other-worker"}
			},
			expected: ReasonTaskWorkerAllowlist,
		},
		{
			name: "task group allowlist",
			mutate: func(_ *registry.WorkerSnapshot, request *tasks.Request) {
				request.Constraints.AllowedGroups = []string{"other-group"}
			},
			expected: ReasonTaskGroupAllowlist,
		},
		{
			name: "at capacity",
			mutate: func(snapshot *registry.WorkerSnapshot, _ *tasks.Request) {
				snapshot.Worker.RunningTasks = intPointer(2)
				snapshot.Worker.MaxQueueDepth = nil
			},
			expected: ReasonAtCapacity,
		},
		{
			name: "queue full",
			mutate: func(snapshot *registry.WorkerSnapshot, _ *tasks.Request) {
				snapshot.Worker.RunningTasks = intPointer(2)
				snapshot.Worker.QueueDepth = intPointer(4)
			},
			expected: ReasonQueueFull,
		},
		{
			name: "CPU",
			mutate: func(_ *registry.WorkerSnapshot, request *tasks.Request) {
				request.Constraints.MinCPU = 7
			},
			expected: ReasonInsufficientCPU,
		},
		{
			name: "RAM",
			mutate: func(_ *registry.WorkerSnapshot, request *tasks.Request) {
				request.Constraints.MinRAMBytes = 13 << 30
			},
			expected: ReasonInsufficientRAM,
		},
		{
			name: "GPU unknown",
			mutate: func(snapshot *registry.WorkerSnapshot, request *tasks.Request) {
				request.Constraints.GPURequired = true
				snapshot.Worker.Resources.GPUStatus = workers.MetricUnsupported
				snapshot.Worker.Resources.GPUs = nil
			},
			expected: ReasonGPUUnknown,
		},
		{
			name: "GPU supported but absent",
			mutate: func(snapshot *registry.WorkerSnapshot, request *tasks.Request) {
				request.Constraints.GPURequired = true
				snapshot.Worker.Resources.GPUs = []workers.GPUResource{}
			},
			expected: ReasonGPURequired,
		},
		{
			name: "GPU mismatch",
			mutate: func(_ *registry.WorkerSnapshot, request *tasks.Request) {
				request.Constraints.GPUVendor = "AMD"
			},
			expected: ReasonGPUMismatch,
		},
		{
			name: "runtime",
			mutate: func(snapshot *registry.WorkerSnapshot, request *tasks.Request) {
				request.Constraints.RequiredRuntime = "rocm"
				snapshot.Worker.Capabilities.Tools = nil
			},
			expected: ReasonRuntimeMissing,
		},
	}

	scheduler := testScheduler(t, DefaultConfig().Weights)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := baseRequest()
			snapshot := workerSnapshot(baseWorker("worker-a", now))
			test.mutate(&snapshot, &request)
			decision, err := scheduler.Evaluate(request, testSnapshot(now, snapshot), nil)
			if err != nil {
				t.Fatal(err)
			}
			if len(decision.Candidates) != 1 || decision.Candidates[0].Eligible {
				t.Fatalf("candidate unexpectedly eligible: %#v", decision.Candidates)
			}
			if !hasReason(decision.Candidates[0].FilterReasons, test.expected) {
				t.Fatalf("reasons = %v, want prefix %q", decision.Candidates[0].FilterReasons, test.expected)
			}
		})
	}
}

func TestResourceRequirementsAcceptMatchingWorker(t *testing.T) {
	now := time.Date(2026, 8, 5, 18, 0, 0, 0, time.UTC)
	request := baseRequest()
	request.Constraints.MinCPU = 4
	request.Constraints.MinRAMBytes = 8 << 30
	request.Constraints.GPURequired = true
	request.Constraints.GPUVendor = "nvidia"
	request.Constraints.MinGPUMemoryBytes = 16 << 30
	request.Constraints.RequiredGPUCapabilities = []string{"fp16"}
	request.Constraints.RequiredRuntime = "cuda"
	request.Constraints.AllowedWorkerIDs = []string{"worker-a"}
	request.Constraints.AllowedGroups = []string{"group-a"}
	worker := baseWorker("worker-a", now)
	worker.GroupHash = "group-a"

	decision, err := testScheduler(t, DefaultConfig().Weights).Evaluate(
		request,
		testSnapshot(now, workerSnapshot(worker)),
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if decision.SelectedWorkerID != "worker-a" {
		t.Fatalf("selected = %q, candidates=%#v", decision.SelectedWorkerID, decision.Candidates)
	}
}

func TestWorkerLabelCannotSatisfyGroupAuthorization(t *testing.T) {
	now := time.Date(2026, 8, 5, 18, 0, 0, 0, time.UTC)
	request := baseRequest()
	request.Constraints.AllowedGroups = []string{"coordinator-group"}
	worker := baseWorker("worker-a", now)
	worker.GroupHash = "different-group"
	worker.Labels = []string{"coordinator-group"}

	decision, err := testScheduler(t, DefaultConfig().Weights).Evaluate(
		request,
		testSnapshot(now, workerSnapshot(worker)),
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(decision.Candidates) != 1 || decision.Candidates[0].Eligible {
		t.Fatalf("self-reported label satisfied a group constraint: %#v", decision.Candidates)
	}
	if !hasReason(decision.Candidates[0].FilterReasons, ReasonTaskGroupAllowlist) {
		t.Fatalf("filter reasons = %v, want %q", decision.Candidates[0].FilterReasons, ReasonTaskGroupAllowlist)
	}
}

func TestMissingSoftMetricsAreNeutral(t *testing.T) {
	now := time.Date(2026, 8, 5, 18, 0, 0, 0, time.UTC)
	worker := baseWorker("worker-a", now)
	worker.RunningTasks = nil
	worker.QueueDepth = nil
	worker.Resources = nil
	worker.Performance = nil
	worker.RTTMillis = nil
	worker.CostWeight = nil

	decision, err := testScheduler(t, DefaultConfig().Weights).Evaluate(
		baseRequest(),
		testSnapshot(now, workerSnapshot(worker)),
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if decision.SelectedWorkerID != worker.WorkerID {
		t.Fatalf("worker with missing soft metrics was not eligible: %#v", decision)
	}
	for _, name := range []string{ComponentThroughput, ComponentLoad, ComponentQueueDepth, ComponentRTT, ComponentCost, ComponentResourceHeadroom} {
		component := decision.Candidates[0].Components[name]
		if component.Score != 0.5 || component.Available {
			t.Fatalf("%s component = %#v, want unavailable neutral", name, component)
		}
	}
	if math.IsNaN(decision.Candidates[0].TotalScore) || math.IsInf(decision.Candidates[0].TotalScore, 0) {
		t.Fatalf("invalid total score %v", decision.Candidates[0].TotalScore)
	}

	request := baseRequest()
	request.Constraints.MinCPU = 1
	filtered, err := testScheduler(t, DefaultConfig().Weights).Evaluate(
		request,
		testSnapshot(now, workerSnapshot(worker)),
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !hasReason(filtered.Candidates[0].FilterReasons, ReasonCPUUnknown) {
		t.Fatalf("unknown hard resource did not filter worker: %#v", filtered.Candidates[0])
	}
}

func TestScoringPreferences(t *testing.T) {
	now := time.Date(2026, 8, 5, 18, 0, 0, 0, time.UTC)
	tests := []struct {
		name    string
		weights Weights
		mutate  func(preferred, other *workers.Worker)
	}{
		{
			name:    "throughput",
			weights: Weights{Throughput: 1},
			mutate: func(preferred, other *workers.Worker) {
				*preferred.Performance[0].TokensPerSecond = 100
				*other.Performance[0].TokensPerSecond = 20
			},
		},
		{
			name:    "load",
			weights: Weights{Load: 1},
			mutate: func(preferred, other *workers.Worker) {
				preferred.RunningTasks = intPointer(0)
				other.RunningTasks = intPointer(1)
			},
		},
		{
			name:    "queue",
			weights: Weights{QueueDepth: 1},
			mutate: func(preferred, other *workers.Worker) {
				preferred.QueueDepth = intPointer(0)
				other.QueueDepth = intPointer(3)
			},
		},
		{
			name:    "model residency",
			weights: Weights{ModelResidency: 1},
			mutate: func(preferred, other *workers.Worker) {
				preferred.LoadedModels = []string{"model-a"}
				other.LoadedModels = nil
			},
		},
		{
			name:    "RTT",
			weights: Weights{RTT: 1},
			mutate: func(preferred, other *workers.Worker) {
				preferred.RTTMillis = floatPointer(2)
				other.RTTMillis = floatPointer(20)
			},
		},
		{
			name:    "cost",
			weights: Weights{Cost: 1},
			mutate: func(preferred, other *workers.Worker) {
				preferred.CostWeight = floatPointer(0.5)
				other.CostWeight = floatPointer(2)
			},
		},
		{
			name:    "resource headroom",
			weights: Weights{ResourceHeadroom: 1},
			mutate: func(preferred, other *workers.Worker) {
				*preferred.Resources.RAM.AvailableBytes.Value = 14 << 30
				*other.Resources.RAM.AvailableBytes.Value = 2 << 30
				*preferred.Resources.CPU.AvailableCapacity.Value = 7
				*other.Resources.CPU.AvailableCapacity.Value = 1
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			preferred := baseWorker("preferred", now)
			other := baseWorker("other", now)
			test.mutate(&preferred, &other)
			decision, err := testScheduler(t, test.weights).Evaluate(
				baseRequest(),
				testSnapshot(now, workerSnapshot(other), workerSnapshot(preferred)),
				nil,
			)
			if err != nil {
				t.Fatal(err)
			}
			if decision.SelectedWorkerID != preferred.WorkerID {
				t.Fatalf("selected = %q, want %q; candidates=%#v", decision.SelectedWorkerID, preferred.WorkerID, decision.Candidates)
			}
			if decision.Candidates[0].TotalScore <= decision.Candidates[1].TotalScore {
				t.Fatalf("preferred score did not win: %#v", decision.Candidates)
			}
		})
	}
}

func TestLeastRecentlyUsedAndWorkerIDTieBreaks(t *testing.T) {
	now := time.Date(2026, 8, 5, 18, 0, 0, 0, time.UTC)
	scheduler := testScheduler(t, Weights{Load: 1})
	workerA := workerSnapshot(baseWorker("worker-a", now))
	workerB := workerSnapshot(baseWorker("worker-b", now))
	snapshot := testSnapshot(now, workerB, workerA)

	decision, err := scheduler.Evaluate(baseRequest(), snapshot, AssignmentHistory{
		"worker-a": now.Add(-time.Minute),
		"worker-b": now.Add(-time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	if decision.SelectedWorkerID != "worker-b" || decision.TieBreak != "least_recently_assigned" {
		t.Fatalf("LRU tie break failed: %#v", decision)
	}

	decision, err = scheduler.Evaluate(baseRequest(), snapshot, nil)
	if err != nil {
		t.Fatal(err)
	}
	if decision.SelectedWorkerID != "worker-a" || decision.TieBreak != "worker_id" {
		t.Fatalf("stable worker ID tie break failed: %#v", decision)
	}
}

func TestExplicitWorkerAndAllFiltered(t *testing.T) {
	now := time.Date(2026, 8, 5, 18, 0, 0, 0, time.UTC)
	request := baseRequest()
	request.WorkerID = "worker-b"
	workerA := workerSnapshot(baseWorker("worker-a", now))
	workerB := workerSnapshot(baseWorker("worker-b", now))
	decision, err := testScheduler(t, Weights{Load: 1}).Evaluate(request, testSnapshot(now, workerA, workerB), nil)
	if err != nil {
		t.Fatal(err)
	}
	if decision.SelectedWorkerID != "worker-b" {
		t.Fatalf("explicit worker not selected: %#v", decision)
	}

	workerB.Worker.State = workers.StateUnavailable
	decision, err = testScheduler(t, Weights{Load: 1}).Evaluate(request, testSnapshot(now, workerA, workerB), nil)
	if err != nil {
		t.Fatal(err)
	}
	if decision.SelectedWorkerID != "" || !strings.Contains(decision.Explanation, "No worker") {
		t.Fatalf("all-filtered decision is not actionable: %#v", decision)
	}

	request.WorkerID = "missing"
	decision, err = testScheduler(t, Weights{Load: 1}).Evaluate(request, testSnapshot(now, workerA), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(decision.Warnings) == 0 {
		t.Fatalf("missing explicit worker warning absent: %#v", decision)
	}
}

func TestStaleAndLowSampleMetricsDoNotInfluenceScore(t *testing.T) {
	now := time.Date(2026, 8, 5, 18, 0, 0, 0, time.UTC)
	worker := baseWorker("worker-a", now)
	worker.Performance[0].SampleCount = 1
	worker.Performance[0].UpdatedAt = now.Add(-2 * time.Hour)
	worker.RTTUpdatedAt = now.Add(-2 * time.Hour)
	worker.Resources.CollectedAt = now.Add(-2 * time.Hour)

	decision, err := testScheduler(t, DefaultConfig().Weights).Evaluate(
		baseRequest(),
		testSnapshot(now, workerSnapshot(worker)),
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	candidate := decision.Candidates[0]
	if candidate.Components[ComponentThroughput].Score != 0.5 ||
		candidate.Components[ComponentRTT].Score != 0.5 ||
		candidate.Components[ComponentResourceHeadroom].Score != 0.5 {
		t.Fatalf("stale metrics were not neutral: %#v", candidate.Components)
	}
	if !containsString(candidate.Warnings, "throughput sample count below minimum") {
		t.Fatalf("low sample warning missing: %v", candidate.Warnings)
	}
}

func TestConfigurationValidation(t *testing.T) {
	tests := []Config{
		{Weights: Weights{}, MetricMaxAge: time.Hour},
		{Weights: Weights{Load: -1}, MetricMaxAge: time.Hour},
		{Weights: Weights{Load: math.NaN()}, MetricMaxAge: time.Hour},
		{Weights: Weights{Load: math.Inf(1)}, MetricMaxAge: time.Hour},
		{Weights: Weights{Load: 1}, MinimumMetricSamples: -1},
		{Weights: Weights{Load: 1}, MetricMaxAge: -time.Second},
		{Weights: Weights{Load: 1}, ScoreEpsilon: -1},
	}
	for _, config := range tests {
		if _, err := New(config); err == nil {
			t.Fatalf("invalid config accepted: %#v", config)
		}
	}
	if _, err := New(Config{}); err != nil {
		t.Fatalf("default config rejected: %v", err)
	}
}

func TestEvaluationIsDeterministic(t *testing.T) {
	now := time.Date(2026, 8, 5, 18, 0, 0, 0, time.UTC)
	scheduler := testScheduler(t, DefaultConfig().Weights)
	snapshot := testSnapshot(
		now,
		workerSnapshot(baseWorker("worker-c", now)),
		workerSnapshot(baseWorker("worker-a", now)),
		workerSnapshot(baseWorker("worker-b", now)),
	)
	first, err := scheduler.Evaluate(baseRequest(), snapshot, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		next, err := scheduler.Evaluate(baseRequest(), snapshot, nil)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(first, next) {
			t.Fatalf("evaluation %d was not deterministic\nfirst=%#v\nnext=%#v", i, first, next)
		}
	}
}

func TestInvalidTaskIsRejectedWithoutEvaluation(t *testing.T) {
	now := time.Date(2026, 8, 5, 18, 0, 0, 0, time.UTC)
	request := baseRequest()
	request.Task.Prompt = ""
	_, err := testScheduler(t, Weights{Load: 1}).Evaluate(
		request,
		testSnapshot(now, workerSnapshot(baseWorker("worker-a", now))),
		nil,
	)
	if err == nil {
		t.Fatal("invalid task was accepted")
	}
}

func hasReason(reasons []string, prefix string) bool {
	for _, reason := range reasons {
		if reason == prefix || strings.HasPrefix(reason, prefix+":") {
			return true
		}
	}
	return false
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
