package store

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/caduceus/caduceus/internal/tasks"
)

type Store struct {
	root string
	mu   sync.Mutex
}

func New(root string) *Store {
	return &Store{root: root}
}

func (s *Store) Init() error {
	return os.MkdirAll(filepath.Join(s.root, "tasks"), 0o700)
}

func (s *Store) Root() string {
	return s.root
}

func (s *Store) SaveTask(meta tasks.Metadata) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if meta.CreatedAt.IsZero() {
		meta.CreatedAt = time.Now().UTC()
	}
	meta.UpdatedAt = time.Now().UTC()
	dir := s.taskDir(meta.TaskID)
	if err := os.MkdirAll(filepath.Join(dir, "artifacts"), 0o700); err != nil {
		return err
	}
	return writeJSON(filepath.Join(dir, "task.json"), meta)
}

func (s *Store) UpdateTask(taskID string, fn func(*tasks.Metadata) error) (tasks.Metadata, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	meta, err := s.getTaskLocked(taskID)
	if err != nil {
		return tasks.Metadata{}, err
	}
	if err := fn(&meta); err != nil {
		return tasks.Metadata{}, err
	}
	meta.UpdatedAt = time.Now().UTC()
	if err := writeJSON(filepath.Join(s.taskDir(taskID), "task.json"), meta); err != nil {
		return tasks.Metadata{}, err
	}
	return meta, nil
}

func (s *Store) GetTask(taskID string) (tasks.Metadata, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.getTaskLocked(taskID)
}

func (s *Store) ListTasks() ([]tasks.Metadata, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entries, err := os.ReadDir(filepath.Join(s.root, "tasks"))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return []tasks.Metadata{}, nil
		}
		return nil, err
	}
	out := make([]tasks.Metadata, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		meta, err := s.getTaskLocked(entry.Name())
		if err == nil {
			out = append(out, meta)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].CreatedAt.After(out[j].CreatedAt)
	})
	return out, nil
}

func (s *Store) AppendEvent(event tasks.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
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
	defer f.Close()
	enc := json.NewEncoder(f)
	return enc.Encode(event)
}

func (s *Store) EventsSince(taskID string, cursor int64) ([]tasks.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	events, err := s.eventsLocked(taskID)
	if err != nil {
		return nil, err
	}
	out := make([]tasks.Event, 0, len(events))
	for _, e := range events {
		if e.Cursor > cursor {
			out = append(out, e)
		}
	}
	return out, nil
}

func (s *Store) SaveResult(result tasks.Result) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := os.MkdirAll(s.taskDir(result.TaskID), 0o700); err != nil {
		return err
	}
	return writeJSON(filepath.Join(s.taskDir(result.TaskID), "result.json"), result)
}

func (s *Store) GetResult(taskID string) (tasks.Result, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var result tasks.Result
	if err := readJSON(filepath.Join(s.taskDir(taskID), "result.json"), &result); err != nil {
		return tasks.Result{}, err
	}
	return result, nil
}

func (s *Store) ListArtifacts(taskID string) ([]tasks.Artifact, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	dir := filepath.Join(s.taskDir(taskID), "artifacts")
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return []tasks.Artifact{}, nil
		}
		return nil, err
	}
	out := make([]tasks.Artifact, 0, len(entries))
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil || entry.IsDir() {
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
	return out, nil
}

func (s *Store) getTaskLocked(taskID string) (tasks.Metadata, error) {
	if taskID == "" {
		return tasks.Metadata{}, errors.New("task id is required")
	}
	var meta tasks.Metadata
	if err := readJSON(filepath.Join(s.taskDir(taskID), "task.json"), &meta); err != nil {
		return tasks.Metadata{}, err
	}
	return meta, nil
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
	for scanner.Scan() {
		var event tasks.Event
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			return nil, fmt.Errorf("parse event: %w", err)
		}
		out = append(out, event)
	}
	return out, scanner.Err()
}

func (s *Store) taskDir(taskID string) string {
	return filepath.Join(s.root, "tasks", filepath.Base(taskID))
}

func writeJSON(path string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func readJSON(path string, v any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, v)
}
