package workers

import "time"

type Capabilities struct {
	LLM                bool     `json:"llm" yaml:"llm"`
	Streaming          bool     `json:"streaming" yaml:"streaming"`
	Artifacts          bool     `json:"artifacts" yaml:"artifacts"`
	Models             []string `json:"models" yaml:"models"`
	MaxConcurrentTasks int      `json:"max_concurrent_tasks" yaml:"max_concurrent_tasks"`
	Tools              []string `json:"tools,omitempty" yaml:"tools,omitempty"`
}

type Worker struct {
	WorkerID     string       `json:"worker_id" yaml:"worker_id"`
	PeerID       string       `json:"peer_id" yaml:"peer_id"`
	Name         string       `json:"name" yaml:"name"`
	Capabilities Capabilities `json:"capabilities" yaml:"capabilities"`
	ListenAddrs  []string     `json:"listen_addrs" yaml:"listen_addrs"`
	Labels       []string     `json:"labels" yaml:"labels"`
	Version      string       `json:"version" yaml:"version"`
	GroupHash    string       `json:"group_hash" yaml:"group_hash"`
	LastSeen     time.Time    `json:"last_seen" yaml:"last_seen"`
	Local        bool         `json:"local" yaml:"local"`
	Allowed      bool         `json:"allowed" yaml:"allowed"`
	TrustLevel   string       `json:"trust_level" yaml:"trust_level"`
}

type Registry struct {
	Workers []Worker `json:"workers" yaml:"workers"`
}
