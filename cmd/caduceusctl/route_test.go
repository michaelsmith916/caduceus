package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/caduceus/caduceus/internal/control"
	"github.com/caduceus/caduceus/internal/registry"
	"github.com/caduceus/caduceus/internal/scheduler"
	"github.com/caduceus/caduceus/internal/workers"
)

func TestWriteRouteExplanationSnapshot(t *testing.T) {
	decision := snapshotDecision()
	var output bytes.Buffer
	if err := writeRouteExplanation(&output, decision); err != nil {
		t.Fatal(err)
	}
	const want = `ROUTE EXPLANATION
task: task-1
selection: worker-a
required_model: N/A (not required)
required_capabilities: N/A (not required)
required_trust: N/A (not required)
snapshot: version=3 timestamp=2026-08-05T20:00:00Z
weights: throughput=0.400000 load=0.200000 queue_depth=0.100000 model_residency=0.100000 rtt=0.050000 cost=0.050000 resource_headroom=0.100000
tie-break: least-recently-assigned then worker-id
warnings: alpha warning,zulu warning
explanation: selected highest-scoring eligible worker
candidates: 1

WORKER    HEALTH/STATE       ELIGIBLE  CAPS                MODEL               RESIDENT            TRUST               CPU FREE       RAM FREE       GPU FREE       QUEUE          RUNNING        TOK/S          RTT            COST           LRU            SCORE     RANK  SELECTED  NOTES
worker-a  available/healthy  true      N/A (not required)  N/A (not required)  N/A (not required)  N/A (not required)  N/A (unknown)  N/A (unknown)  N/A (unknown)  N/A (unknown)  N/A (unknown)  N/A (unknown)  N/A (unknown)  N/A (unknown)  N/A (unknown)  0.750000  1     true      first warning,second warning

CANDIDATE DETAILS

[1] worker=worker-a SELECTED eligible=true total_score=0.750000
  identity: name=alpha peer=peer-a session=N/A (unknown) protocol=N/A (unknown) trust=trusted-lan group=N/A (unknown)
  health: state=available alive=true healthy=true suspect=false stale=false schedulable=true accepting=N/A (unknown) last_seen_age=2s
  capabilities: llm=true streaming=true artifacts=false models=model-a,model-z loaded_models=model-a,model-z tools=N/A (none advertised)
  admission: running=N/A (unknown)/2 queue=N/A (unknown)/N/A (unknown)
  resources: status=unknown collected=N/A (unknown)
    cpu: logical=N/A (unknown) available=N/A (unknown) utilization=N/A (unknown)
    ram: total=N/A (unknown) available=N/A (unknown)
    gpu: status=unknown devices=N/A (unknown)
  performance: tokens_per_second=N/A (unknown) samples=N/A updated=N/A (unknown)
  network: rtt_ms=N/A (unknown) updated=N/A (unknown)
  economics: cost_weight=N/A (unknown)
  lru: last_assigned=N/A (unknown)
  filter_reasons: none
  missing_metrics: rtt,throughput
  warnings: first warning,second warning
  tie-break: worker_id
  components:
    throughput: available=false score=0.500000 weight=0.400000 detail=throughput unavailable or stale
    load: N/A (unknown)
    queue_depth: N/A (unknown)
    model_residency: N/A (unknown)
    rtt: N/A (unknown)
    cost: N/A (unknown)
    resource_headroom: N/A (unknown)
`
	if output.String() != want {
		t.Fatalf("route explanation snapshot mismatch\n--- got ---\n%s\n--- want ---\n%s", output.String(), want)
	}
}

func TestWriteRouteExplanationIsDeterministicAndComplete(t *testing.T) {
	decision := snapshotDecision()
	first := decision.Candidates[0]
	second := richCandidate()
	decision.Candidates = []scheduler.CandidateScore{second, first}
	var left bytes.Buffer
	if err := writeRouteExplanation(&left, decision); err != nil {
		t.Fatal(err)
	}
	decision.Candidates = []scheduler.CandidateScore{first, second}
	var right bytes.Buffer
	if err := writeRouteExplanation(&right, decision); err != nil {
		t.Fatal(err)
	}
	if left.String() != right.String() {
		t.Fatalf("candidate input order changed output\nleft:\n%s\nright:\n%s", left.String(), right.String())
	}
	for _, required := range []string{
		"cpu: logical=8 count (current) available=4.000000 cores (stale)",
		"ram: total=17179869184 bytes (current) available=8589934592 bytes (current)",
		"gpu: status=current devices=1",
		"capabilities=cuda,fp16",
		"performance: model=model-a tokens_per_second=42.500000 (unknown/stale) measured=true samples=12",
		"network: rtt_ms=8.250000 (current)",
		"economics: cost_weight=1.250000 (current)",
		"lru: last_assigned=2026-08-05T19:55:00Z",
		"filter_reasons: worker_draining",
	} {
		if !strings.Contains(left.String(), required) {
			t.Errorf("output missing %q\n%s", required, left.String())
		}
	}
}

func TestReadRouteRequestValidation(t *testing.T) {
	valid, err := readRouteRequest("-", strings.NewReader(`{"kind":"prompt","task":{"prompt":"hello"}}`))
	if err != nil || valid.Task.Prompt != "hello" {
		t.Fatalf("valid request=%#v err=%v", valid, err)
	}
	for _, test := range []struct {
		name string
		body string
	}{
		{name: "unknown field", body: `{"task":{"prompt":"hello"},"typo":true}`},
		{name: "multiple values", body: `{"task":{"prompt":"hello"}} {}`},
		{name: "empty prompt", body: `{"task":{"prompt":""}}`},
		{name: "malformed", body: `{"task":`},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := readRouteRequest("-", strings.NewReader(test.body)); err == nil {
				t.Fatal("expected error")
			}
		})
	}
	tooLarge := strings.Repeat(" ", maxRouteRequestBytes+1)
	if _, err := readRouteRequest("-", strings.NewReader(tooLarge)); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("expected size error, got %v", err)
	}
}

func TestRouteDecisionJSONStructure(t *testing.T) {
	decision := snapshotDecision()
	data, err := json.Marshal(control.Success(decision))
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := json.Unmarshal(data, &document); err != nil {
		t.Fatal(err)
	}
	if document["ok"] != true {
		t.Fatalf("missing ok envelope: %s", data)
	}
	payload, ok := document["data"].(map[string]any)
	if !ok || payload["selected_worker_id"] != "worker-a" {
		t.Fatalf("unexpected data shape: %#v", document["data"])
	}
	candidates, ok := payload["candidates"].([]any)
	if !ok || len(candidates) != 1 {
		t.Fatalf("unexpected candidates shape: %#v", payload["candidates"])
	}
	decoded, err := decodeRoutingDecision(payload)
	if err != nil || decoded.SelectedWorkerID != decision.SelectedWorkerID || len(decoded.Candidates) != 1 {
		t.Fatalf("decoded=%#v err=%v", decoded, err)
	}
}

func snapshotDecision() scheduler.RoutingDecision {
	return scheduler.RoutingDecision{
		TaskID:            "task-1",
		SelectedWorkerID:  "worker-a",
		RegistryTimestamp: time.Date(2026, 8, 5, 20, 0, 0, 0, time.UTC),
		RegistryVersion:   3,
		AppliedWeights: scheduler.Weights{
			Throughput: 0.4, Load: 0.2, QueueDepth: 0.1, ModelResidency: 0.1,
			RTT: 0.05, Cost: 0.05, ResourceHeadroom: 0.1,
		},
		Warnings:    []string{"zulu warning", "alpha warning"},
		TieBreak:    "least-recently-assigned then worker-id",
		Explanation: "selected highest-scoring eligible worker",
		Candidates: []scheduler.CandidateScore{{
			WorkerID: "worker-a",
			Worker: registry.WorkerSnapshot{
				Worker: workers.Worker{
					WorkerID: "worker-a", PeerID: "peer-a", Name: "alpha", State: workers.StateAvailable,
					Allowed: true, TrustLevel: "trusted-lan",
					Capabilities: workers.Capabilities{LLM: true, Streaming: true, Models: []string{"model-z", "model-a"}, MaxConcurrentTasks: 2},
					LoadedModels: []string{"model-z", "model-a"},
				},
				Alive: true, Healthy: true, Schedulable: true, LastSeenAge: 2 * time.Second,
			},
			Eligible:       true,
			MissingMetrics: []string{scheduler.ComponentThroughput, scheduler.ComponentRTT},
			Warnings:       []string{"second warning", "first warning"},
			Components: map[string]scheduler.ComponentScore{
				scheduler.ComponentThroughput: {Score: 0.5, Weight: 0.4, Available: false, Detail: "throughput unavailable or stale"},
			},
			TotalScore: 0.75,
			Rank:       1,
			Selected:   true,
			TieBreak:   "worker_id",
		}},
	}
}

func richCandidate() scheduler.CandidateScore {
	logical := int64(8)
	totalRAM := int64(16 << 30)
	availableRAM := int64(8 << 30)
	availableCPU := 4.0
	utilization := 0.5
	gpuTotal := int64(8 << 30)
	gpuAvailable := int64(6 << 30)
	gpuUtilization := 0.25
	tokens := 42.5
	rtt := 8.25
	cost := 1.25
	accepting := false
	running := 2
	maxRunning := 4
	queue := 1
	maxQueue := 8
	lastAssigned := time.Date(2026, 8, 5, 19, 55, 0, 0, time.UTC)
	collected := time.Date(2026, 8, 5, 19, 59, 0, 0, time.UTC)
	return scheduler.CandidateScore{
		WorkerID: "worker-b",
		Worker: registry.WorkerSnapshot{
			Worker: workers.Worker{
				WorkerID: "worker-b", PeerID: "peer-b", Name: "beta", State: workers.StateDraining,
				AcceptingWork: &accepting, RunningTasks: &running, MaxConcurrentTasks: &maxRunning,
				QueueDepth: &queue, MaxQueueDepth: &maxQueue, CostWeight: &cost, RTTMillis: &rtt,
				RTTUpdatedAt: collected, LoadedModels: []string{"model-a"},
				Resources: &workers.ResourceSnapshot{
					Status: workers.MetricCurrent, CollectedAt: collected,
					CPU: workers.CPUResources{
						LogicalProcessors: workers.Int64Metric{Value: &logical, Unit: "count", Status: workers.MetricCurrent},
						AvailableCapacity: workers.FloatMetric{Value: &availableCPU, Unit: "cores", Status: workers.MetricStale},
						Utilization:       workers.FloatMetric{Value: &utilization, Unit: "ratio", Status: workers.MetricCurrent},
					},
					RAM: workers.MemoryResources{
						TotalBytes:     workers.Int64Metric{Value: &totalRAM, Unit: "bytes", Status: workers.MetricCurrent},
						AvailableBytes: workers.Int64Metric{Value: &availableRAM, Unit: "bytes", Status: workers.MetricCurrent},
					},
					GPUStatus: workers.MetricCurrent,
					GPUs: []workers.GPUResource{{
						DeviceID: "gpu-0", Vendor: "NVIDIA", Model: "Example", Runtime: "cuda",
						TotalMemoryBytes:     workers.Int64Metric{Value: &gpuTotal, Unit: "bytes", Status: workers.MetricCurrent},
						AvailableMemoryBytes: workers.Int64Metric{Value: &gpuAvailable, Unit: "bytes", Status: workers.MetricCurrent},
						Utilization:          workers.FloatMetric{Value: &gpuUtilization, Unit: "ratio", Status: workers.MetricCurrent},
						Capabilities:         []string{"fp16", "cuda"},
					}},
				},
				Performance: []workers.ModelPerformance{{Model: "model-a", TokensPerSecond: &tokens, SampleCount: 12, UpdatedAt: collected, Measured: true}},
			},
			Alive: true, Healthy: false, Suspect: true, Stale: true, Schedulable: false, LastSeenAge: 45 * time.Second,
		},
		Eligible: false, FilterReasons: []string{scheduler.ReasonWorkerDraining},
		MissingMetrics: []string{scheduler.ComponentThroughput},
		Components:     map[string]scheduler.ComponentScore{},
		LastAssigned:   &lastAssigned,
	}
}
