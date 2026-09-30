package p2p

import (
	"context"
	"sort"
	"time"

	"github.com/caduceus/caduceus/internal/scheduler"
)

const (
	retryCooldownFilterReason = "retry_cooldown"
	triedWorkerFilterReason   = "already_tried"
	maxWorkerRetryCooldown    = 24 * time.Hour
	maxWorkerCooldownWait     = time.Minute
	maxWorkerCooldownEntries  = 4096
)

func (r *phase2Runtime) cooldownNow() time.Time {
	if r.now != nil {
		return r.now()
	}
	return time.Now()
}

func (r *phase2Runtime) pruneCooldownsLocked(now time.Time) {
	for workerID, until := range r.cooldowns {
		if !now.Before(until) {
			delete(r.cooldowns, workerID)
		}
	}
}

func (r *phase2Runtime) setWorkerCooldown(workerID string, delay time.Duration) {
	if workerID == "" || delay <= 0 {
		return
	}
	if delay > maxWorkerRetryCooldown {
		delay = maxWorkerRetryCooldown
	}
	now := r.cooldownNow()
	until := now.Add(delay)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cooldowns == nil {
		r.cooldowns = make(map[string]time.Time)
	}
	r.pruneCooldownsLocked(now)
	if previous, ok := r.cooldowns[workerID]; ok && previous.After(until) {
		return
	}
	if _, exists := r.cooldowns[workerID]; !exists && len(r.cooldowns) >= maxWorkerCooldownEntries {
		victim := ""
		var earliest time.Time
		for candidate, deadline := range r.cooldowns {
			if victim == "" || deadline.Before(earliest) || (deadline.Equal(earliest) && candidate < victim) {
				victim = candidate
				earliest = deadline
			}
		}
		delete(r.cooldowns, victim)
	}
	r.cooldowns[workerID] = until
}

func candidateHasFilter(candidate scheduler.CandidateScore, reason string) bool {
	for _, value := range candidate.FilterReasons {
		if value == reason {
			return true
		}
	}
	return false
}

func applyWorkerCooldowns(decision *scheduler.RoutingDecision, cooldowns map[string]time.Time, now time.Time) {
	if decision == nil {
		return
	}
	changed := 0
	for index := range decision.Candidates {
		candidate := &decision.Candidates[index]
		if !candidate.Eligible {
			continue
		}
		until, cooling := cooldowns[candidate.WorkerID]
		if !cooling || !now.Before(until) {
			continue
		}
		candidate.Eligible = false
		if !candidateHasFilter(*candidate, retryCooldownFilterReason) {
			candidate.FilterReasons = append(candidate.FilterReasons, retryCooldownFilterReason)
		}
		changed++
	}
	if changed == 0 {
		return
	}
	rerankRoutingDecision(decision)
	decision.Warnings = append(decision.Warnings, "workers in retry cooldown are temporarily ineligible")
	decision.Explanation += " Workers in retry cooldown are excluded until their retry_after deadline."
}

func applyTriedWorkers(decision *scheduler.RoutingDecision, tried map[string]struct{}) {
	if decision == nil || len(tried) == 0 {
		return
	}
	changed := 0
	for index := range decision.Candidates {
		candidate := &decision.Candidates[index]
		if _, alreadyTried := tried[candidate.WorkerID]; !alreadyTried {
			continue
		}
		if !candidateHasFilter(*candidate, triedWorkerFilterReason) {
			candidate.FilterReasons = append(candidate.FilterReasons, triedWorkerFilterReason)
		}
		if candidate.Eligible {
			candidate.Eligible = false
			changed++
		}
	}
	if changed == 0 {
		return
	}
	rerankRoutingDecision(decision)
	decision.Warnings = append(decision.Warnings, "workers already tried for this task are ineligible")
	decision.Explanation += " Workers already tried for this task are excluded from rerouting."
}

func rerankRoutingDecision(decision *scheduler.RoutingDecision) {
	eligible := make([]int, 0, len(decision.Candidates))
	for index := range decision.Candidates {
		decision.Candidates[index].Selected = false
		if decision.Candidates[index].Eligible {
			eligible = append(eligible, index)
		} else {
			decision.Candidates[index].Rank = 0
		}
	}
	sort.SliceStable(eligible, func(i, j int) bool {
		left := decision.Candidates[eligible[i]]
		right := decision.Candidates[eligible[j]]
		if left.Rank != right.Rank {
			return left.Rank < right.Rank
		}
		if left.TotalScore != right.TotalScore {
			return left.TotalScore > right.TotalScore
		}
		return left.WorkerID < right.WorkerID
	})
	decision.SelectedWorkerID = ""
	for rank, index := range eligible {
		decision.Candidates[index].Rank = rank + 1
		if rank == 0 {
			decision.Candidates[index].Selected = true
			decision.SelectedWorkerID = decision.Candidates[index].WorkerID
		}
	}
	if decision.SelectedWorkerID == "" {
		decision.Explanation = "No worker remains eligible after applying runtime retry filters."
	} else {
		decision.Explanation = "Selected " + decision.SelectedWorkerID + ": highest-ranked worker remaining after applying runtime retry filters."
	}
}

func (r *phase2Runtime) nextCooldownDelay(decision scheduler.RoutingDecision, tried map[string]struct{}) (time.Duration, bool) {
	now := r.cooldownNow()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pruneCooldownsLocked(now)
	found := false
	var earliest time.Time
	for _, candidate := range decision.Candidates {
		if _, alreadyTried := tried[candidate.WorkerID]; alreadyTried || !candidateHasFilter(candidate, retryCooldownFilterReason) {
			continue
		}
		found = true
		if until, ok := r.cooldowns[candidate.WorkerID]; ok && (earliest.IsZero() || until.Before(earliest)) {
			earliest = until
		}
	}
	if !found {
		return 0, false
	}
	if earliest.IsZero() || !now.Before(earliest) {
		return 0, true
	}
	return earliest.Sub(now), true
}

func waitForWorkerCooldown(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	if delay > maxWorkerCooldownWait {
		delay = maxWorkerCooldownWait
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (r *phase2Runtime) waitForCooldown(ctx context.Context, delay time.Duration) error {
	if r.waitCooldown != nil {
		return r.waitCooldown(ctx, delay)
	}
	return waitForWorkerCooldown(ctx, delay)
}
