package store

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/caduceus/caduceus/internal/tasks"
)

const (
	CurrentStateVersion       = 1
	rootStateFile             = "state.json"
	defaultQueueOwner         = "coordinator"
	RestartInterruptionReason = "coordinator_restart"
	maxEventLineBytes         = 1024 * 1024
)

var ErrQueueFull = errors.New("durable task queue is full")

type rootState struct {
	Version           int                `json:"version"`
	NextQueueSequence uint64             `json:"next_queue_sequence"`
	Queue             []tasks.QueueEntry `json:"queue"`
	UpdatedAt         time.Time          `json:"updated_at"`
}

type Store struct {
	root        string
	mu          sync.Mutex
	state       rootState
	initialized bool
	beforeWrite func(string) error
}

func New(root string) *Store {
	return &Store{root: root}
}

func (s *Store) Init() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.initLocked()
}

func (s *Store) initLocked() error {
	if s.initialized {
		return nil
	}
	if strings.TrimSpace(s.root) == "" {
		return errors.New("store root is required")
	}
	if err := os.MkdirAll(filepath.Join(s.root, "tasks"), 0o700); err != nil {
		return fmt.Errorf("create task store: %w", err)
	}
	state, created, err := s.loadRootStateLocked()
	if err != nil {
		return err
	}
	s.state = state
	records, err := s.taskRecordsLocked()
	if err != nil {
		return fmt.Errorf("inspect task records during store recovery: %w", err)
	}
	changed, err := s.recoverLocked(records)
	if err != nil {
		return fmt.Errorf("recover task store: %w", err)
	}
	if created || changed {
		if err := s.writeRootStateLocked(); err != nil {
			return fmt.Errorf("persist recovered root state: %w", err)
		}
	}
	s.initialized = true
	return nil
}

func (s *Store) Root() string {
	return s.root
}

func (s *Store) StateVersion() (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.initLocked(); err != nil {
		return 0, err
	}
	return s.state.Version, nil
}

func (s *Store) SaveTask(meta tasks.Metadata) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.initLocked(); err != nil {
		return err
	}
	if err := validateTaskID(meta.TaskID); err != nil {
		return err
	}
	if meta.CreatedAt.IsZero() {
		meta.CreatedAt = time.Now().UTC()
	}
	meta.UpdatedAt = time.Now().UTC()
	meta.Idempotent = meta.Request.Idempotent
	meta.MaxAttempts = meta.Request.EffectiveMaxAttempts()
	if meta.Attempts == nil {
		meta.Attempts = []tasks.TaskAttempt{}
	}
	if meta.Rejections == nil {
		meta.Rejections = []tasks.TaskRejection{}
	}
	dir := s.taskDir(meta.TaskID)
	if err := os.MkdirAll(filepath.Join(dir, "artifacts"), 0o700); err != nil {
		return err
	}
	return writeJSON(filepath.Join(dir, "task.json"), meta)
}

func (s *Store) UpdateTask(taskID string, fn func(*tasks.Metadata) error) (tasks.Metadata, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.initLocked(); err != nil {
		return tasks.Metadata{}, err
	}
	if err := validateTaskID(taskID); err != nil {
		return tasks.Metadata{}, err
	}
	meta, err := s.getTaskLocked(taskID)
	if err != nil {
		return tasks.Metadata{}, err
	}
	if fn != nil {
		if err := fn(&meta); err != nil {
			return tasks.Metadata{}, err
		}
	}
	meta.UpdatedAt = time.Now().UTC()
	if err := writeJSON(filepath.Join(s.taskDir(taskID), "task.json"), meta); err != nil {
		return tasks.Metadata{}, err
	}
	s.applyQueueStatusLocked(&meta)
	return meta, nil
}

func (s *Store) GetTask(taskID string) (tasks.Metadata, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.initLocked(); err != nil {
		return tasks.Metadata{}, err
	}
	if err := validateTaskID(taskID); err != nil {
		return tasks.Metadata{}, err
	}
	meta, err := s.getTaskLocked(taskID)
	if err == nil {
		s.applyQueueStatusLocked(&meta)
	}
	return meta, err
}

func (s *Store) ListTasks() ([]tasks.Metadata, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.initLocked(); err != nil {
		return nil, err
	}
	records, recordErr := s.taskRecordsLocked()
	out := make([]tasks.Metadata, 0, len(records))
	for _, meta := range records {
		s.applyQueueStatusLocked(&meta)
		out = append(out, meta)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].CreatedAt.After(out[j].CreatedAt)
	})
	return out, recordErr
}

func (s *Store) Enqueue(taskID string, owner ...string) (tasks.QueueEntry, error) {
	return s.enqueue(taskID, 0, owner...)
}

// EnqueueBounded atomically rejects a new queue entry once maxDepth pending
// entries exist. Existing entries remain idempotent even when the queue is at
// capacity, which makes recovery safe to retry.
func (s *Store) EnqueueBounded(taskID string, maxDepth int, owner ...string) (tasks.QueueEntry, error) {
	if maxDepth < 1 {
		return tasks.QueueEntry{}, errors.New("maximum durable queue depth must be positive")
	}
	return s.enqueue(taskID, maxDepth, owner...)
}

func (s *Store) enqueue(taskID string, maxDepth int, owner ...string) (tasks.QueueEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.initLocked(); err != nil {
		return tasks.QueueEntry{}, err
	}
	if err := validateTaskID(taskID); err != nil {
		return tasks.QueueEntry{}, err
	}
	if len(owner) > 1 {
		return tasks.QueueEntry{}, errors.New("enqueue accepts at most one queue owner")
	}
	queueOwner := defaultQueueOwner
	if len(owner) == 1 && strings.TrimSpace(owner[0]) != "" {
		queueOwner = strings.TrimSpace(owner[0])
	}
	if len(queueOwner) > 512 {
		return tasks.QueueEntry{}, errors.New("queue owner is too large")
	}
	meta, err := s.getTaskLocked(taskID)
	if err != nil {
		return tasks.QueueEntry{}, err
	}
	if index := s.queueIndexLocked(taskID); index >= 0 {
		entry := s.state.Queue[index]
		entry.Position = index + 1
		return entry, nil
	}
	if maxDepth > 0 && len(s.state.Queue) >= maxDepth {
		return tasks.QueueEntry{}, fmt.Errorf("%w (limit %d)", ErrQueueFull, maxDepth)
	}
	if terminalStatus(meta.Status) {
		if meta.Status != tasks.StatusInterrupted || !meta.Request.Idempotent || attemptCount(meta) >= meta.Request.EffectiveMaxAttempts() {
			return tasks.QueueEntry{}, fmt.Errorf("task %q in terminal state %q cannot be queued", taskID, meta.Status)
		}
	}
	if s.state.NextQueueSequence == 0 || s.state.NextQueueSequence == ^uint64(0) {
		return tasks.QueueEntry{}, errors.New("queue sequence space is exhausted")
	}
	now := time.Now().UTC()
	entry := tasks.QueueEntry{
		TaskID:     taskID,
		Owner:      queueOwner,
		Sequence:   s.state.NextQueueSequence,
		EnqueuedAt: now,
	}
	before := cloneRootState(s.state)
	s.state.NextQueueSequence++
	s.state.Queue = append(s.state.Queue, entry)
	if err := s.writeRootStateLocked(); err != nil {
		s.state = before
		return tasks.QueueEntry{}, fmt.Errorf("persist queue entry: %w", err)
	}
	meta.Status = tasks.StatusQueued
	meta.QueueOwner = entry.Owner
	meta.QueueSequence = entry.Sequence
	meta.QueuePosition = len(s.state.Queue)
	meta.QueuePositionExact = true
	meta.QueuedAt = timePointer(entry.EnqueuedAt)
	meta.FinishedAt = nil
	meta.UpdatedAt = now
	if err := writeJSON(filepath.Join(s.taskDir(taskID), "task.json"), meta); err != nil {
		s.state = before
		rollbackErr := s.writeRootStateLocked()
		return tasks.QueueEntry{}, errors.Join(fmt.Errorf("persist queued task metadata: %w", err), wrapRollbackError(rollbackErr))
	}
	if err := s.syncQueueMetadataLocked(); err != nil {
		return tasks.QueueEntry{}, err
	}
	entry.Position = len(s.state.Queue)
	return entry, nil
}

func (s *Store) Dequeue() (tasks.QueueEntry, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.initLocked(); err != nil {
		return tasks.QueueEntry{}, false, err
	}
	if len(s.state.Queue) == 0 {
		return tasks.QueueEntry{}, false, nil
	}
	before := cloneRootState(s.state)
	entry := s.state.Queue[0]
	s.state.Queue = append([]tasks.QueueEntry(nil), s.state.Queue[1:]...)
	if err := s.writeRootStateLocked(); err != nil {
		s.state = before
		return tasks.QueueEntry{}, false, fmt.Errorf("persist queue dequeue: %w", err)
	}
	meta, err := s.getTaskLocked(entry.TaskID)
	if err != nil {
		s.state = before
		rollbackErr := s.writeRootStateLocked()
		return tasks.QueueEntry{}, false, errors.Join(fmt.Errorf("load dequeued task: %w", err), wrapRollbackError(rollbackErr))
	}
	clearQueueStatus(&meta)
	meta.Status = tasks.StatusAccepted
	meta.UpdatedAt = time.Now().UTC()
	if err := writeJSON(filepath.Join(s.taskDir(entry.TaskID), "task.json"), meta); err != nil {
		s.state = before
		rollbackErr := s.writeRootStateLocked()
		return tasks.QueueEntry{}, false, errors.Join(fmt.Errorf("persist dequeued task metadata: %w", err), wrapRollbackError(rollbackErr))
	}
	if err := s.syncQueueMetadataLocked(); err != nil {
		return entry, false, err
	}
	entry.Position = 1
	return entry, true, nil
}

// CancelQueued commits cancellation in task metadata before removing the
// derived queue entry. Once the metadata write succeeds, a non-nil cleanup
// error can be returned together with canceled=true; recovery will finish the
// queue cleanup without resurrecting the task.
func (s *Store) CancelQueued(taskID string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.initLocked(); err != nil {
		return false, err
	}
	if err := validateTaskID(taskID); err != nil {
		return false, err
	}
	index := s.queueIndexLocked(taskID)
	if index < 0 {
		return false, nil
	}
	meta, err := s.getTaskLocked(taskID)
	if err != nil {
		return false, fmt.Errorf("load canceled queued task: %w", err)
	}
	now := time.Now().UTC()
	meta.Status = tasks.StatusCanceled
	meta.FinishedAt = &now
	meta.UpdatedAt = now
	clearQueueStatus(&meta)
	if err := s.writeJSONLocked(filepath.Join(s.taskDir(taskID), "task.json"), meta); err != nil {
		return false, fmt.Errorf("persist canceled queued task: %w", err)
	}

	s.state.Queue = append(s.state.Queue[:index:index], s.state.Queue[index+1:]...)
	if err := s.writeRootStateLocked(); err != nil {
		return true, fmt.Errorf("task cancellation committed; persist queue cleanup: %w", err)
	}
	if err := s.syncQueueMetadataLocked(); err != nil {
		return true, fmt.Errorf("task cancellation committed; synchronize queue metadata: %w", err)
	}
	return true, nil
}

func (s *Store) ListQueue() ([]tasks.QueueEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.initLocked(); err != nil {
		return nil, err
	}
	return queueCopy(s.state.Queue), nil
}

// RestoreQueue returns the durable queue loaded and reconciled by Init. It is
// named explicitly for coordinator startup code and preserves exact FIFO.
func (s *Store) RestoreQueue() ([]tasks.QueueEntry, error) {
	return s.ListQueue()
}

func (s *Store) QueuePosition(taskID string) (int, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.initLocked(); err != nil {
		return 0, false, err
	}
	if err := validateTaskID(taskID); err != nil {
		return 0, false, err
	}
	index := s.queueIndexLocked(taskID)
	if index < 0 {
		return 0, false, nil
	}
	return index + 1, true, nil
}

func (s *Store) AppendEvent(event tasks.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.initLocked(); err != nil {
		return err
	}
	if err := validateTaskID(event.TaskID); err != nil {
		return err
	}
	dir := s.taskDir(event.TaskID)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	}
	if event.Cursor == 0 {
		next, err := s.nextCursorLocked(event.TaskID)
		if err != nil {
			return err
		}
		event.Cursor = next
	}
	f, err := os.OpenFile(filepath.Join(dir, "events.jsonl"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	encErr := json.NewEncoder(f).Encode(event)
	syncErr := f.Sync()
	closeErr := f.Close()
	return errors.Join(encErr, syncErr, closeErr)
}

func (s *Store) EventsSince(taskID string, cursor int64) ([]tasks.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.initLocked(); err != nil {
		return nil, err
	}
	if err := validateTaskID(taskID); err != nil {
		return nil, err
	}
	events, err := s.eventsLocked(taskID)
	if err != nil {
		return nil, err
	}
	out := make([]tasks.Event, 0, len(events))
	for _, event := range events {
		if event.Cursor > cursor {
			out = append(out, event)
		}
	}
	return out, nil
}

func (s *Store) SaveResult(result tasks.Result) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.initLocked(); err != nil {
		return err
	}
	if err := validateTaskID(result.TaskID); err != nil {
		return err
	}
	if err := os.MkdirAll(s.taskDir(result.TaskID), 0o700); err != nil {
		return err
	}
	return writeJSON(filepath.Join(s.taskDir(result.TaskID), "result.json"), result)
}

func (s *Store) GetResult(taskID string) (tasks.Result, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.initLocked(); err != nil {
		return tasks.Result{}, err
	}
	if err := validateTaskID(taskID); err != nil {
		return tasks.Result{}, err
	}
	var result tasks.Result
	if err := readJSON(filepath.Join(s.taskDir(taskID), "result.json"), &result); err != nil {
		return tasks.Result{}, err
	}
	return result, nil
}

func (s *Store) ListArtifacts(taskID string) ([]tasks.Artifact, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.initLocked(); err != nil {
		return nil, err
	}
	if err := validateTaskID(taskID); err != nil {
		return nil, err
	}
	dir := filepath.Join(s.taskDir(taskID), "artifacts")
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return []tasks.Artifact{}, nil
		}
		return nil, err
	}
	out := make([]tasks.Artifact, 0, len(entries))
	var artifactErrors []error
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			artifactErrors = append(artifactErrors, fmt.Errorf("inspect artifact %q: %w", entry.Name(), err))
			continue
		}
		out = append(out, tasks.Artifact{
			ArtifactID:  entry.Name(),
			Name:        entry.Name(),
			ContentType: "application/octet-stream",
			SizeBytes:   info.Size(),
			Path:        filepath.Join(dir, entry.Name()),
			CreatedAt:   info.ModTime().UTC(),
		})
	}
	return out, errors.Join(artifactErrors...)
}

func (s *Store) recoverLocked(records map[string]tasks.Metadata) (bool, error) {
	now := time.Now().UTC()
	changed := false
	for taskID, meta := range records {
		metaChanged := false
		effectiveMaxAttempts := meta.Request.EffectiveMaxAttempts()
		if meta.Idempotent != meta.Request.Idempotent || meta.MaxAttempts != effectiveMaxAttempts {
			meta.Idempotent = meta.Request.Idempotent
			meta.MaxAttempts = effectiveMaxAttempts
			metaChanged = true
		}
		if meta.Status == tasks.StatusAccepted || meta.Status == tasks.StatusRunning {
			reconciled, err := s.reconcileTerminalResultLocked(taskID, &meta, now)
			if err != nil {
				return changed, err
			}
			if reconciled {
				metaChanged = true
			}
		}
		for i := range meta.Attempts {
			attempt := &meta.Attempts[i]
			if attempt.State != tasks.AttemptStateAccepted && attempt.State != tasks.AttemptStateRunning {
				continue
			}
			attempt.State = tasks.AttemptStateInterrupted
			attempt.Interruption = RestartInterruptionReason
			attempt.Error = "coordinator restarted before the attempt reached a terminal result"
			attempt.UpdatedAt = now
			attempt.FinishedAt = &now
			metaChanged = true
		}
		count := attemptCount(meta)
		if meta.TotalAttempts != count {
			meta.TotalAttempts = count
			metaChanged = true
		}
		if count > meta.CurrentAttempt {
			meta.CurrentAttempt = count
			metaChanged = true
		}
		if meta.Status == tasks.StatusAccepted || meta.Status == tasks.StatusRunning {
			meta.InterruptionReason = RestartInterruptionReason
			if meta.Request.Idempotent && count < meta.Request.EffectiveMaxAttempts() {
				meta.Status = tasks.StatusQueued
				meta.FinishedAt = nil
			} else {
				meta.Status = tasks.StatusInterrupted
				meta.FinishedAt = &now
				meta.Error = "coordinator restarted; task was not automatically replayed"
			}
			metaChanged = true
		}
		if metaChanged {
			meta.UpdatedAt = now
			if err := writeJSON(filepath.Join(s.taskDir(taskID), "task.json"), meta); err != nil {
				return changed, fmt.Errorf("persist recovered task %q: %w", taskID, err)
			}
			records[taskID] = meta
			changed = true
		}
	}

	queue := make([]tasks.QueueEntry, 0, len(s.state.Queue))
	queued := make(map[string]struct{}, len(s.state.Queue))
	for _, entry := range s.state.Queue {
		meta, ok := records[entry.TaskID]
		if !ok {
			return changed, fmt.Errorf("queue references missing task %q", entry.TaskID)
		}
		if _, duplicate := queued[entry.TaskID]; duplicate {
			return changed, fmt.Errorf("queue contains duplicate task %q", entry.TaskID)
		}
		if meta.Status != tasks.StatusQueued {
			changed = true
			continue
		}
		entry.Position = 0
		queue = append(queue, entry)
		queued[entry.TaskID] = struct{}{}
	}

	missing := make([]tasks.Metadata, 0)
	for taskID, meta := range records {
		if meta.Status != tasks.StatusQueued {
			continue
		}
		if _, ok := queued[taskID]; !ok {
			missing = append(missing, meta)
		}
	}
	sort.Slice(missing, func(i, j int) bool {
		if missing[i].QueueSequence != missing[j].QueueSequence && missing[i].QueueSequence > 0 && missing[j].QueueSequence > 0 {
			return missing[i].QueueSequence < missing[j].QueueSequence
		}
		left, right := missing[i].CreatedAt, missing[j].CreatedAt
		if missing[i].QueuedAt != nil {
			left = *missing[i].QueuedAt
		}
		if missing[j].QueuedAt != nil {
			right = *missing[j].QueuedAt
		}
		if !left.Equal(right) {
			return left.Before(right)
		}
		return missing[i].TaskID < missing[j].TaskID
	})
	for _, meta := range missing {
		enqueuedAt := meta.CreatedAt
		if meta.QueuedAt != nil {
			enqueuedAt = *meta.QueuedAt
		}
		if enqueuedAt.IsZero() {
			enqueuedAt = now
		}
		owner := meta.QueueOwner
		if owner == "" {
			owner = defaultQueueOwner
		}
		if s.state.NextQueueSequence == 0 || s.state.NextQueueSequence == ^uint64(0) {
			return changed, errors.New("queue sequence space is exhausted during recovery")
		}
		entry := tasks.QueueEntry{TaskID: meta.TaskID, Owner: owner, Sequence: s.state.NextQueueSequence, EnqueuedAt: enqueuedAt}
		s.state.NextQueueSequence++
		queue = append(queue, entry)
		changed = true
	}
	if len(queue) != len(s.state.Queue) {
		changed = true
	}
	s.state.Queue = queue
	if err := s.syncQueueMetadataLocked(); err != nil {
		return changed, err
	}
	return changed, nil
}

// reconcileTerminalResultLocked closes the crash window between SaveResult
// and the task metadata update. A result is authoritative only for the sole
// current active attempt and only when both opaque fence values match.
func (s *Store) reconcileTerminalResultLocked(taskID string, meta *tasks.Metadata, now time.Time) (bool, error) {
	var result tasks.Result
	path := filepath.Join(s.taskDir(taskID), "result.json")
	if err := readJSON(path, &result); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, fmt.Errorf("load terminal result for task %q: %w", taskID, err)
	}
	if result.TaskID != taskID || !terminalStatus(result.Status) || meta.CurrentAttempt < 1 || attemptCount(*meta) != meta.CurrentAttempt {
		return false, nil
	}

	currentIndex := -1
	for i := range meta.Attempts {
		attempt := &meta.Attempts[i]
		if attempt.AttemptNumber > meta.CurrentAttempt {
			return false, nil
		}
		if attempt.AttemptNumber != meta.CurrentAttempt {
			continue
		}
		if currentIndex >= 0 {
			return false, nil
		}
		currentIndex = i
	}
	if currentIndex < 0 {
		return false, nil
	}

	attempt := &meta.Attempts[currentIndex]
	if attempt.State != tasks.AttemptStateAccepted && attempt.State != tasks.AttemptStateRunning {
		return false, nil
	}
	if attempt.TaskID != taskID || attempt.AttemptID == "" || attempt.AttemptToken == "" || result.AttemptID != attempt.AttemptID || result.AttemptToken != attempt.AttemptToken {
		return false, nil
	}
	if err := attempt.Validate(); err != nil {
		return false, nil
	}

	attemptState, ok := attemptStateForTerminalStatus(result.Status)
	if !ok {
		return false, nil
	}
	detail := resultErrorDetail(result)
	attempt.State = attemptState
	attempt.Error = detail
	attempt.Interruption = ""
	attempt.UpdatedAt = now
	attempt.FinishedAt = timePointer(now)
	meta.Status = result.Status
	meta.Error = detail
	meta.InterruptionReason = ""
	meta.FinishedAt = timePointer(now)
	clearQueueStatus(meta)
	return true, nil
}

func attemptStateForTerminalStatus(status string) (string, bool) {
	switch status {
	case tasks.StatusCompleted:
		return tasks.AttemptStateCompleted, true
	case tasks.StatusFailed:
		return tasks.AttemptStateFailed, true
	case tasks.StatusCanceled:
		return tasks.AttemptStateCanceled, true
	case tasks.StatusInterrupted:
		return tasks.AttemptStateInterrupted, true
	case tasks.StatusLost:
		return tasks.AttemptStateLost, true
	default:
		return "", false
	}
}

func resultErrorDetail(result tasks.Result) string {
	if result.Error == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint(result.Error))
}

func (s *Store) loadRootStateLocked() (rootState, bool, error) {
	path := filepath.Join(s.root, rootStateFile)
	var state rootState
	if err := readJSON(path, &state); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return rootState{Version: CurrentStateVersion, NextQueueSequence: 1, Queue: []tasks.QueueEntry{}}, true, nil
		}
		return rootState{}, false, fmt.Errorf("load root state %s: %w", path, err)
	}
	dirty := false
	if state.Version == 0 {
		state.Version = CurrentStateVersion
		dirty = true
	}
	if state.Version != CurrentStateVersion {
		return rootState{}, false, fmt.Errorf("unsupported store state version %d (current %d)", state.Version, CurrentStateVersion)
	}
	if state.Queue == nil {
		state.Queue = []tasks.QueueEntry{}
		dirty = true
	}
	seenTasks := make(map[string]struct{}, len(state.Queue))
	seenSequences := make(map[uint64]struct{}, len(state.Queue))
	var maxSequence uint64
	for i := range state.Queue {
		entry := &state.Queue[i]
		if err := validateTaskID(entry.TaskID); err != nil {
			return rootState{}, false, fmt.Errorf("invalid queue entry %d: %w", i, err)
		}
		if entry.Sequence == 0 || entry.EnqueuedAt.IsZero() {
			return rootState{}, false, fmt.Errorf("invalid queue entry %d: sequence and enqueued_at are required", i)
		}
		if _, ok := seenTasks[entry.TaskID]; ok {
			return rootState{}, false, fmt.Errorf("duplicate queued task %q", entry.TaskID)
		}
		if _, ok := seenSequences[entry.Sequence]; ok {
			return rootState{}, false, fmt.Errorf("duplicate queue sequence %d", entry.Sequence)
		}
		seenTasks[entry.TaskID] = struct{}{}
		seenSequences[entry.Sequence] = struct{}{}
		entry.Position = 0
		if entry.Owner == "" {
			entry.Owner = defaultQueueOwner
			dirty = true
		}
		if entry.Sequence > maxSequence {
			maxSequence = entry.Sequence
		}
	}
	if maxSequence == ^uint64(0) {
		return rootState{}, false, errors.New("queue sequence space is exhausted")
	}
	if !sort.SliceIsSorted(state.Queue, func(i, j int) bool { return state.Queue[i].Sequence < state.Queue[j].Sequence }) {
		sort.SliceStable(state.Queue, func(i, j int) bool { return state.Queue[i].Sequence < state.Queue[j].Sequence })
		dirty = true
	}
	if state.NextQueueSequence <= maxSequence {
		state.NextQueueSequence = maxSequence + 1
		dirty = true
	}
	if state.NextQueueSequence == 0 {
		state.NextQueueSequence = 1
		dirty = true
	}
	return state, dirty, nil
}

func (s *Store) writeRootStateLocked() error {
	s.state.Version = CurrentStateVersion
	s.state.UpdatedAt = time.Now().UTC()
	return s.writeJSONLocked(filepath.Join(s.root, rootStateFile), s.state)
}

func (s *Store) writeJSONLocked(path string, value any) error {
	if s.beforeWrite != nil {
		if err := s.beforeWrite(path); err != nil {
			return err
		}
	}
	return writeJSON(path, value)
}

func (s *Store) taskRecordsLocked() (map[string]tasks.Metadata, error) {
	entries, err := os.ReadDir(filepath.Join(s.root, "tasks"))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return map[string]tasks.Metadata{}, nil
		}
		return nil, err
	}
	records := make(map[string]tasks.Metadata, len(entries))
	var recordErrors []error
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		meta, err := s.getTaskLocked(entry.Name())
		if err != nil {
			recordErrors = append(recordErrors, fmt.Errorf("load task record %q: %w", entry.Name(), err))
			continue
		}
		if meta.TaskID != entry.Name() {
			recordErrors = append(recordErrors, fmt.Errorf("task directory %q contains task_id %q", entry.Name(), meta.TaskID))
			continue
		}
		records[meta.TaskID] = meta
	}
	return records, errors.Join(recordErrors...)
}

func (s *Store) getTaskLocked(taskID string) (tasks.Metadata, error) {
	if err := validateTaskID(taskID); err != nil {
		return tasks.Metadata{}, err
	}
	var meta tasks.Metadata
	if err := readJSON(filepath.Join(s.taskDir(taskID), "task.json"), &meta); err != nil {
		return tasks.Metadata{}, err
	}
	return meta, nil
}

func (s *Store) syncQueueMetadataLocked() error {
	var syncErrors []error
	for index, entry := range s.state.Queue {
		meta, err := s.getTaskLocked(entry.TaskID)
		if err != nil {
			syncErrors = append(syncErrors, fmt.Errorf("load queued task %q: %w", entry.TaskID, err))
			continue
		}
		position := index + 1
		if meta.QueueOwner == entry.Owner && meta.QueueSequence == entry.Sequence && meta.QueuePosition == position && meta.QueuePositionExact && meta.QueuedAt != nil && meta.QueuedAt.Equal(entry.EnqueuedAt) {
			continue
		}
		meta.QueueOwner = entry.Owner
		meta.QueueSequence = entry.Sequence
		meta.QueuePosition = position
		meta.QueuePositionExact = true
		meta.QueuedAt = timePointer(entry.EnqueuedAt)
		meta.UpdatedAt = time.Now().UTC()
		if err := writeJSON(filepath.Join(s.taskDir(entry.TaskID), "task.json"), meta); err != nil {
			syncErrors = append(syncErrors, fmt.Errorf("persist queue position for task %q: %w", entry.TaskID, err))
		}
	}
	return errors.Join(syncErrors...)
}

func (s *Store) applyQueueStatusLocked(meta *tasks.Metadata) {
	index := s.queueIndexLocked(meta.TaskID)
	if index < 0 {
		return
	}
	entry := s.state.Queue[index]
	meta.QueueOwner = entry.Owner
	meta.QueueSequence = entry.Sequence
	meta.QueuePosition = index + 1
	meta.QueuePositionExact = true
	meta.QueuedAt = timePointer(entry.EnqueuedAt)
}

func (s *Store) queueIndexLocked(taskID string) int {
	for index, entry := range s.state.Queue {
		if entry.TaskID == taskID {
			return index
		}
	}
	return -1
}

func (s *Store) nextCursorLocked(taskID string) (int64, error) {
	events, err := s.eventsLocked(taskID)
	if err != nil {
		return 0, err
	}
	if len(events) == 0 {
		return 1, nil
	}
	return events[len(events)-1].Cursor + 1, nil
}

func (s *Store) eventsLocked(taskID string) ([]tasks.Event, error) {
	path := filepath.Join(s.taskDir(taskID), "events.jsonl")
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return []tasks.Event{}, nil
		}
		return nil, err
	}
	defer f.Close()
	var out []tasks.Event
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64*1024), maxEventLineBytes)
	line := 0
	for scanner.Scan() {
		line++
		var event tasks.Event
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			return nil, fmt.Errorf("parse event line %d in %s: %w", line, path, err)
		}
		out = append(out, event)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan events %s: %w", path, err)
	}
	return out, nil
}

func (s *Store) taskDir(taskID string) string {
	return filepath.Join(s.root, "tasks", taskID)
}

func validateTaskID(taskID string) error {
	if strings.TrimSpace(taskID) == "" {
		return errors.New("task id is required")
	}
	if len(taskID) > 512 {
		return errors.New("task id is too large")
	}
	if filepath.Base(taskID) != taskID || taskID == "." || strings.ContainsAny(taskID, "\x00\r\n") {
		return errors.New("task id contains invalid path or control characters")
	}
	return nil
}

func terminalStatus(status string) bool {
	switch status {
	case tasks.StatusCompleted, tasks.StatusFailed, tasks.StatusCanceled, tasks.StatusInterrupted, tasks.StatusLost:
		return true
	default:
		return false
	}
}

func attemptCount(meta tasks.Metadata) int {
	count := meta.TotalAttempts
	if len(meta.Attempts) > count {
		count = len(meta.Attempts)
	}
	for _, attempt := range meta.Attempts {
		if attempt.AttemptNumber > count {
			count = attempt.AttemptNumber
		}
	}
	if count == 0 && (meta.Status == tasks.StatusAccepted || meta.Status == tasks.StatusRunning) {
		count = 1
	}
	return count
}

func clearQueueStatus(meta *tasks.Metadata) {
	meta.QueueOwner = ""
	meta.QueueSequence = 0
	meta.QueuePosition = 0
	meta.QueuePositionExact = false
	meta.QueuedAt = nil
}

func queueCopy(queue []tasks.QueueEntry) []tasks.QueueEntry {
	out := make([]tasks.QueueEntry, len(queue))
	copy(out, queue)
	for index := range out {
		out[index].Position = index + 1
	}
	return out
}

func cloneRootState(state rootState) rootState {
	clone := state
	clone.Queue = append([]tasks.QueueEntry(nil), state.Queue...)
	return clone
}

func timePointer(value time.Time) *time.Time {
	copyValue := value
	return &copyValue
}

func wrapRollbackError(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("rollback queue state: %w", err)
}

func writeJSON(path string, value any) (returnErr error) {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	temp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tempPath := temp.Name()
	defer func() {
		_ = os.Remove(tempPath)
	}()
	if err := temp.Chmod(0o600); err != nil {
		_ = temp.Close()
		return err
	}
	if _, err := temp.Write(data); err != nil {
		_ = temp.Close()
		return err
	}
	if err := temp.Sync(); err != nil {
		_ = temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	if err := replaceFileAtomic(tempPath, path); err != nil {
		return err
	}
	return syncDirectory(dir)
}

func readJSON(path string, value any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(data, value); err != nil {
		return fmt.Errorf("parse %s: %w", path, err)
	}
	return nil
}

func syncDirectory(path string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
