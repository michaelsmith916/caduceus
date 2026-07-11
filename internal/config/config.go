package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	DefaultMDNSServiceName = "_caduceus._tcp"
	DefaultModel           = "qwen3.5:9b"
	DefaultOpenAIBaseURL   = "http://127.0.0.1:11434/v1"
	DefaultControlPort     = "37391"
)

type Config struct {
	Node     NodeConfig     `json:"node" yaml:"node"`
	Security SecurityConfig `json:"security" yaml:"security"`
	Worker   WorkerConfig   `json:"worker" yaml:"worker"`
	OpenAI   OpenAIConfig   `json:"openai" yaml:"openai"`
	Control  ControlConfig  `json:"control" yaml:"control"`
	Logging  LoggingConfig  `json:"logging" yaml:"logging"`
	Storage  StorageConfig  `json:"storage" yaml:"storage"`
}

type NodeConfig struct {
	Name            string   `json:"name" yaml:"name"`
	PeerID          string   `json:"peer_id" yaml:"peer_id"`
	PrivateKeyPath  string   `json:"private_key_path" yaml:"private_key_path"`
	ListenAddrs     []string `json:"listen_addrs" yaml:"listen_addrs"`
	MDNSServiceName string   `json:"mdns_service_name" yaml:"mdns_service_name"`
	EnableMDNS      bool     `json:"enable_mdns" yaml:"enable_mdns"`
}

type SecurityConfig struct {
	P2PKeyHash        string `json:"p2p_key_hash" yaml:"p2p_key_hash"`
	P2PKeyPath        string `json:"p2p_key_path" yaml:"p2p_key_path"`
	AllowedPeersPath  string `json:"allowed_peers_path" yaml:"allowed_peers_path"`
	RequireAllowlist  bool   `json:"require_allowlist" yaml:"require_allowlist"`
	TrustLevelDefault string `json:"trust_level_default" yaml:"trust_level_default"`
}

type WorkerConfig struct {
	Enabled            bool               `json:"enabled" yaml:"enabled"`
	Labels             []string           `json:"labels" yaml:"labels"`
	MaxConcurrentTasks int                `json:"max_concurrent_tasks" yaml:"max_concurrent_tasks"`
	Capabilities       WorkerCapabilities `json:"capabilities" yaml:"capabilities"`
}

type WorkerCapabilities struct {
	LLM       bool     `json:"llm" yaml:"llm"`
	Streaming bool     `json:"streaming" yaml:"streaming"`
	Artifacts bool     `json:"artifacts" yaml:"artifacts"`
	Tools     []string `json:"tools" yaml:"tools"`
}

type OpenAIConfig struct {
	BaseURL        string `json:"base_url" yaml:"base_url"`
	APIKeyEnv      string `json:"api_key_env" yaml:"api_key_env"`
	DefaultModel   string `json:"default_model" yaml:"default_model"`
	TimeoutSeconds int    `json:"timeout_seconds" yaml:"timeout_seconds"`
}

type ControlConfig struct {
	Mode          string `json:"mode" yaml:"mode"`
	Listen        string `json:"listen" yaml:"listen"`
	AuthTokenPath string `json:"auth_token_path" yaml:"auth_token_path"`
}

type LoggingConfig struct {
	Level string `json:"level" yaml:"level"`
	JSON  bool   `json:"json" yaml:"json"`
}

type StorageConfig struct {
	DataDir       string `json:"data_dir" yaml:"data_dir"`
	RetentionDays int    `json:"retention_days" yaml:"retention_days"`
}

type Paths struct {
	ConfigDir  string
	ConfigPath string
	DataDir    string
}

func DefaultPaths() (Paths, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return Paths{}, err
	}
	var paths Paths
	switch runtime.GOOS {
	case "darwin":
		base := filepath.Join(home, "Library", "Application Support", "Caduceus")
		paths = Paths{ConfigDir: base, ConfigPath: filepath.Join(base, "config.yaml"), DataDir: base}
	case "windows":
		appData := os.Getenv("APPDATA")
		if appData == "" {
			appData = filepath.Join(home, "AppData", "Roaming")
		}
		localAppData := os.Getenv("LOCALAPPDATA")
		if localAppData == "" {
			localAppData = filepath.Join(home, "AppData", "Local")
		}
		paths = Paths{
			ConfigDir:  filepath.Join(appData, "Caduceus"),
			ConfigPath: filepath.Join(appData, "Caduceus", "config.yaml"),
			DataDir:    filepath.Join(localAppData, "Caduceus"),
		}
	default:
		configHome := os.Getenv("XDG_CONFIG_HOME")
		if configHome == "" {
			configHome = filepath.Join(home, ".config")
		}
		dataHome := os.Getenv("XDG_DATA_HOME")
		if dataHome == "" {
			dataHome = filepath.Join(home, ".local", "share")
		}
		paths = Paths{
			ConfigDir:  filepath.Join(configHome, "caduceus"),
			ConfigPath: filepath.Join(configHome, "caduceus", "config.yaml"),
			DataDir:    filepath.Join(dataHome, "caduceus"),
		}
	}
	if configDir := strings.TrimSpace(os.Getenv("CADUCEUS_CONFIG_DIR")); configDir != "" {
		paths.ConfigDir = configDir
		paths.ConfigPath = filepath.Join(configDir, "config.yaml")
	}
	if dataDir := strings.TrimSpace(os.Getenv("CADUCEUS_DATA_DIR")); dataDir != "" {
		paths.DataDir = dataDir
	}
	return paths, nil
}

func Default() (Config, error) {
	paths, err := DefaultPaths()
	if err != nil {
		return Config{}, err
	}
	name, _ := os.Hostname()
	name = strings.TrimSpace(name)
	if name == "" {
		name = "caduceus-node"
	}
	cfg := Config{
		Node: NodeConfig{
			Name:            name,
			ListenAddrs:     []string{"/ip4/0.0.0.0/tcp/0"},
			MDNSServiceName: DefaultMDNSServiceName,
			EnableMDNS:      true,
		},
		Security: SecurityConfig{
			RequireAllowlist:  true,
			TrustLevelDefault: "trusted-lan",
		},
		Worker: WorkerConfig{
			Enabled:            true,
			Labels:             []string{},
			MaxConcurrentTasks: 1,
			Capabilities: WorkerCapabilities{
				LLM:       true,
				Streaming: true,
				Artifacts: true,
				Tools:     []string{"run_remote_prompt"},
			},
		},
		OpenAI: OpenAIConfig{
			BaseURL:        DefaultOpenAIBaseURL,
			APIKeyEnv:      "CADUCEUS_OPENAI_API_KEY",
			DefaultModel:   DefaultModel,
			TimeoutSeconds: 300,
		},
		Control: ControlConfig{
			Mode: "auto",
		},
		Logging: LoggingConfig{
			Level: "info",
			JSON:  false,
		},
		Storage: StorageConfig{
			DataDir:       paths.DataDir,
			RetentionDays: 14,
		},
	}
	cfg.applyDerivedDefaults(paths)
	return cfg, nil
}

func Load(path string) (Config, string, error) {
	if strings.TrimSpace(path) == "" {
		paths, err := DefaultPaths()
		if err != nil {
			return Config{}, "", err
		}
		path = paths.ConfigPath
	}
	cfg, err := Default()
	if err != nil {
		return Config{}, "", err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			cfg.ApplyEnv()
			return cfg, path, nil
		}
		return Config{}, "", err
	}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return Config{}, "", fmt.Errorf("parse config %s: %w", path, err)
	}
	paths, err := DefaultPaths()
	if err != nil {
		return Config{}, "", err
	}
	cfg.ApplyEnv()
	cfg.applyDerivedDefaults(paths)
	return cfg, path, nil
}

func Save(path string, cfg Config) error {
	if strings.TrimSpace(path) == "" {
		paths, err := DefaultPaths()
		if err != nil {
			return err
		}
		path = paths.ConfigPath
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	data, err := yaml.Marshal(&cfg)
	if err != nil {
		return err
	}
	return atomicWrite(path, data, 0o600)
}

func EnsureConfig(path string) (Config, string, error) {
	cfg, resolved, err := Load(path)
	if err != nil {
		return Config{}, "", err
	}
	if _, err := os.Stat(resolved); errors.Is(err, os.ErrNotExist) {
		if err := Save(resolved, cfg); err != nil {
			return Config{}, "", err
		}
	}
	if err := EnsureDirs(cfg); err != nil {
		return Config{}, "", err
	}
	return cfg, resolved, nil
}

func EnsureDirs(cfg Config) error {
	dirs := []string{
		cfg.Storage.DataDir,
		filepath.Dir(cfg.Node.PrivateKeyPath),
		filepath.Dir(cfg.Security.P2PKeyPath),
		filepath.Dir(cfg.Security.AllowedPeersPath),
		filepath.Dir(cfg.Control.AuthTokenPath),
	}
	for _, dir := range dirs {
		if strings.TrimSpace(dir) == "" || dir == "." {
			continue
		}
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return err
		}
	}
	return nil
}

func (c *Config) ApplyEnv() {
	if v := strings.TrimSpace(os.Getenv("CADUCEUS_OPENAI_BASE_URL")); v != "" {
		c.OpenAI.BaseURL = v
	}
	if v := strings.TrimSpace(os.Getenv("CADUCEUS_DEFAULT_MODEL")); v != "" {
		c.OpenAI.DefaultModel = v
	}
	if v := strings.TrimSpace(os.Getenv("CADUCEUS_DATA_DIR")); v != "" {
		c.Storage.DataDir = v
	}
}

func (c *Config) applyDerivedDefaults(paths Paths) {
	if strings.TrimSpace(c.Storage.DataDir) == "" {
		c.Storage.DataDir = paths.DataDir
	}
	if len(c.Node.ListenAddrs) == 0 {
		c.Node.ListenAddrs = []string{"/ip4/0.0.0.0/tcp/0"}
	}
	if strings.TrimSpace(c.Node.MDNSServiceName) == "" {
		c.Node.MDNSServiceName = DefaultMDNSServiceName
	}
	if strings.TrimSpace(c.Node.Name) == "" {
		c.Node.Name = "caduceus-node"
	}
	if strings.TrimSpace(c.Node.PrivateKeyPath) == "" {
		c.Node.PrivateKeyPath = filepath.Join(c.Storage.DataDir, "node.key")
	}
	if strings.TrimSpace(c.Security.P2PKeyPath) == "" {
		c.Security.P2PKeyPath = filepath.Join(paths.ConfigDir, "p2p.key")
	}
	if strings.TrimSpace(c.Security.AllowedPeersPath) == "" {
		c.Security.AllowedPeersPath = filepath.Join(paths.ConfigDir, "allowed-peers.yaml")
	}
	if strings.TrimSpace(c.Security.TrustLevelDefault) == "" {
		c.Security.TrustLevelDefault = "trusted-lan"
	}
	if strings.TrimSpace(c.OpenAI.BaseURL) == "" {
		c.OpenAI.BaseURL = DefaultOpenAIBaseURL
	}
	if strings.TrimSpace(c.OpenAI.APIKeyEnv) == "" {
		c.OpenAI.APIKeyEnv = "CADUCEUS_OPENAI_API_KEY"
	}
	if strings.TrimSpace(c.OpenAI.DefaultModel) == "" {
		c.OpenAI.DefaultModel = DefaultModel
	}
	if c.OpenAI.TimeoutSeconds <= 0 {
		c.OpenAI.TimeoutSeconds = 300
	}
	if c.Worker.MaxConcurrentTasks <= 0 {
		c.Worker.MaxConcurrentTasks = 1
	}
	if c.Worker.Capabilities.Tools == nil {
		c.Worker.Capabilities.Tools = []string{"run_remote_prompt"}
	}
	if strings.TrimSpace(c.Control.Mode) == "" {
		c.Control.Mode = "auto"
	}
	if strings.TrimSpace(c.Control.AuthTokenPath) == "" {
		c.Control.AuthTokenPath = filepath.Join(c.Storage.DataDir, "control.token")
	}
	if c.Storage.RetentionDays <= 0 {
		c.Storage.RetentionDays = 14
	}
	if strings.TrimSpace(c.Logging.Level) == "" {
		c.Logging.Level = "info"
	}
}

func (c Config) ControlEndpoint() string {
	if strings.TrimSpace(c.Control.Listen) != "" {
		return c.Control.Listen
	}
	if runtime.GOOS == "windows" {
		return "http://127.0.0.1:" + DefaultControlPort
	}
	return "unix://" + filepath.Join(c.Storage.DataDir, "caduceusd.sock")
}

func (c Config) OpenAITimeout() time.Duration {
	return time.Duration(c.OpenAI.TimeoutSeconds) * time.Second
}

func atomicWrite(path string, data []byte, mode os.FileMode) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, mode); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
