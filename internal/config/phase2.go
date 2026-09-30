package config

import (
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"net"
	"strings"
	"time"
)

const (
	AvailabilityAlways      = "always"
	AvailabilityManual      = "manual"
	AvailabilityUnavailable = "unavailable"
	AvailabilityDraining    = "draining"
	AvailabilityIdle        = "idle"
	AvailabilityLoggedOut   = "logged_out"
	AvailabilityScheduled   = "scheduled"
)

// AvailabilityConfig controls only the opaque schedulability state advertised
// to peers. User names and local activity details never leave the worker.
type AvailabilityConfig struct {
	Mode             string               `json:"mode" yaml:"mode"`
	ManualAvailable  bool                 `json:"manual_available" yaml:"manual_available"`
	IdleAfterSeconds int                  `json:"idle_after_seconds" yaml:"idle_after_seconds"`
	Timezone         string               `json:"timezone,omitempty" yaml:"timezone,omitempty"`
	Schedule         []AvailabilityWindow `json:"schedule,omitempty" yaml:"schedule,omitempty"`
}

type AvailabilityWindow struct {
	Days  []string `json:"days" yaml:"days"`
	Start string   `json:"start" yaml:"start"`
	End   string   `json:"end" yaml:"end"`
}

type HeartbeatConfig struct {
	IntervalSeconds     int `json:"interval_seconds" yaml:"interval_seconds"`
	SuspectAfterSeconds int `json:"suspect_after_seconds" yaml:"suspect_after_seconds"`
	EvictAfterSeconds   int `json:"evict_after_seconds" yaml:"evict_after_seconds"`
	LeaseSeconds        int `json:"lease_seconds" yaml:"lease_seconds"`
}

type SchedulerConfig struct {
	Weights              SchedulerWeights `json:"weights" yaml:"weights"`
	MinimumMetricSamples int              `json:"minimum_metric_samples" yaml:"minimum_metric_samples"`
	MetricMaxAgeSeconds  int              `json:"metric_max_age_seconds" yaml:"metric_max_age_seconds"`
	MaxRerouteAttempts   int              `json:"max_reroute_attempts" yaml:"max_reroute_attempts"`
	MaxInFlightTasks     int              `json:"max_in_flight_tasks" yaml:"max_in_flight_tasks"`
	MaxQueueDepth        int              `json:"max_queue_depth" yaml:"max_queue_depth"`
	ScoreEpsilon         float64          `json:"score_epsilon" yaml:"score_epsilon"`
}

type SchedulerWeights struct {
	Throughput       float64 `json:"throughput" yaml:"throughput"`
	RunningTasks     float64 `json:"running_tasks" yaml:"running_tasks"`
	QueueDepth       float64 `json:"queue_depth" yaml:"queue_depth"`
	ModelResidency   float64 `json:"model_residency" yaml:"model_residency"`
	RTT              float64 `json:"rtt" yaml:"rtt"`
	Cost             float64 `json:"cost" yaml:"cost"`
	ResourceHeadroom float64 `json:"resource_headroom" yaml:"resource_headroom"`
}

type EnrollmentConfig struct {
	TrustedLAN TrustedLANEnrollmentConfig `json:"trusted_lan" yaml:"trusted_lan"`
}

type TrustedLANEnrollmentConfig struct {
	CoordinatorAddress string   `json:"coordinator_address,omitempty" yaml:"coordinator_address,omitempty"`
	HermesMode         string   `json:"hermes_mode" yaml:"hermes_mode"`
	Enabled            bool     `json:"enabled" yaml:"enabled"`
	RequestTTLSeconds  int      `json:"request_ttl_seconds" yaml:"request_ttl_seconds"`
	TokenTTLSeconds    int      `json:"token_ttl_seconds" yaml:"token_ttl_seconds"`
	RequireApproval    bool     `json:"require_approval" yaml:"require_approval"`
	AllowedNetworks    []string `json:"allowed_networks" yaml:"allowed_networks"`
	RateLimit          int      `json:"rate_limit" yaml:"rate_limit"`
	RateWindowSeconds  int      `json:"rate_window_seconds" yaml:"rate_window_seconds"`
}

func (c Config) Validate() error {
	if value := strings.TrimSpace(c.Security.P2PKeyHash); value != "" {
		decoded, err := hex.DecodeString(value)
		if err != nil || len(decoded) != 32 || strings.ToLower(value) != value {
			return errors.New("security.p2p_key_hash must be a lowercase SHA-256 hex digest")
		}
	}
	if c.Worker.MaxConcurrentTasks < 1 || c.Worker.MaxConcurrentTasks > 1024 {
		return errors.New("worker.max_concurrent_tasks must be between 1 and 1024; zero is not unlimited")
	}
	if c.Worker.MaxQueueDepth < 0 || c.Worker.MaxQueueDepth > 100000 {
		return errors.New("worker.max_queue_depth must be between 0 and 100000")
	}
	if !finitePositive(c.Worker.CostWeight) {
		return errors.New("worker.cost_weight must be finite and greater than zero")
	}
	switch c.Worker.Availability.Mode {
	case AvailabilityAlways, AvailabilityManual, AvailabilityUnavailable, AvailabilityDraining, AvailabilityIdle, AvailabilityLoggedOut, AvailabilityScheduled:
	default:
		return fmt.Errorf("unsupported worker.availability.mode %q", c.Worker.Availability.Mode)
	}
	if c.Worker.Availability.Mode == AvailabilityIdle && c.Worker.Availability.IdleAfterSeconds < 1 {
		return errors.New("worker.availability.idle_after_seconds must be positive in idle mode")
	}
	if c.Worker.Availability.Timezone != "" {
		if _, err := time.LoadLocation(c.Worker.Availability.Timezone); err != nil {
			return fmt.Errorf("invalid worker availability timezone: %w", err)
		}
	}
	validDay := map[string]bool{"sun": true, "mon": true, "tue": true, "wed": true, "thu": true, "fri": true, "sat": true}
	for _, window := range c.Worker.Availability.Schedule {
		if len(window.Days) == 0 {
			return errors.New("worker availability schedule window requires at least one day")
		}
		for _, day := range window.Days {
			if !validDay[strings.ToLower(strings.TrimSpace(day))] {
				return fmt.Errorf("invalid worker availability schedule day %q", day)
			}
		}
		if _, err := time.Parse("15:04", window.Start); err != nil {
			return fmt.Errorf("invalid worker availability start time %q", window.Start)
		}
		if _, err := time.Parse("15:04", window.End); err != nil {
			return fmt.Errorf("invalid worker availability end time %q", window.End)
		}
	}
	if c.Heartbeat.IntervalSeconds < 1 {
		return errors.New("heartbeat.interval_seconds must be positive")
	}
	if c.Heartbeat.SuspectAfterSeconds <= c.Heartbeat.IntervalSeconds {
		return errors.New("heartbeat.suspect_after_seconds must exceed heartbeat.interval_seconds")
	}
	if c.Heartbeat.EvictAfterSeconds <= c.Heartbeat.SuspectAfterSeconds {
		return errors.New("heartbeat.evict_after_seconds must exceed heartbeat.suspect_after_seconds")
	}
	if c.Heartbeat.LeaseSeconds < 0 {
		return errors.New("heartbeat.lease_seconds must be non-negative")
	}
	if err := c.Scheduler.Validate(); err != nil {
		return err
	}
	return c.Enrollment.Validate()
}

func (c SchedulerConfig) Validate() error {
	weights := []float64{c.Weights.Throughput, c.Weights.RunningTasks, c.Weights.QueueDepth, c.Weights.ModelResidency, c.Weights.RTT, c.Weights.Cost, c.Weights.ResourceHeadroom}
	total := 0.0
	for _, weight := range weights {
		if math.IsNaN(weight) || math.IsInf(weight, 0) || weight < 0 {
			return errors.New("scheduler weights must be finite and non-negative")
		}
		total += weight
	}
	if total <= 0 {
		return errors.New("at least one scheduler weight must be positive")
	}
	if c.MinimumMetricSamples < 1 {
		return errors.New("scheduler.minimum_metric_samples must be positive")
	}
	if c.MetricMaxAgeSeconds < 1 {
		return errors.New("scheduler.metric_max_age_seconds must be positive")
	}
	if c.MaxRerouteAttempts < 0 || c.MaxRerouteAttempts > 100 {
		return errors.New("scheduler.max_reroute_attempts must be between 0 and 100")
	}
	if c.MaxInFlightTasks < 1 || c.MaxInFlightTasks > 4096 {
		return errors.New("scheduler.max_in_flight_tasks must be between 1 and 4096")
	}
	if c.MaxQueueDepth < 1 || c.MaxQueueDepth > 1_000_000 {
		return errors.New("scheduler.max_queue_depth must be between 1 and 1000000")
	}
	if !finitePositive(c.ScoreEpsilon) || c.ScoreEpsilon > 1 {
		return errors.New("scheduler.score_epsilon must be finite, positive, and at most 1")
	}
	return nil
}

func (c EnrollmentConfig) Validate() error {
	e := c.TrustedLAN
	if e.RequestTTLSeconds < 1 || e.TokenTTLSeconds < 1 {
		return errors.New("enrollment request and token TTLs must be positive")
	}
	if e.RateLimit < 1 || e.RateWindowSeconds < 1 {
		return errors.New("enrollment rate limit and window must be positive")
	}
	if len(e.CoordinatorAddress) > 2048 || strings.ContainsAny(e.CoordinatorAddress, "\x00\r\n") {
		return errors.New("enrollment coordinator address is invalid")
	}
	switch e.HermesMode {
	case "disabled", "invoked":
	default:
		return fmt.Errorf("unsupported enrollment Hermes mode %q", e.HermesMode)
	}
	for _, network := range e.AllowedNetworks {
		if _, _, err := net.ParseCIDR(strings.TrimSpace(network)); err != nil {
			return fmt.Errorf("invalid enrollment allowed network %q: %w", network, err)
		}
	}
	return nil
}

func finitePositive(value float64) bool {
	return value > 0 && !math.IsNaN(value) && !math.IsInf(value, 0)
}

func (c HeartbeatConfig) Interval() time.Duration {
	return time.Duration(c.IntervalSeconds) * time.Second
}
func (c HeartbeatConfig) SuspectAfter() time.Duration {
	return time.Duration(c.SuspectAfterSeconds) * time.Second
}
func (c HeartbeatConfig) EvictAfter() time.Duration {
	return time.Duration(c.EvictAfterSeconds) * time.Second
}
func (c HeartbeatConfig) Lease() time.Duration { return time.Duration(c.LeaseSeconds) * time.Second }
func (c SchedulerConfig) MetricMaxAge() time.Duration {
	return time.Duration(c.MetricMaxAgeSeconds) * time.Second
}
func (c TrustedLANEnrollmentConfig) RequestTTL() time.Duration {
	return time.Duration(c.RequestTTLSeconds) * time.Second
}
func (c TrustedLANEnrollmentConfig) TokenTTL() time.Duration {
	return time.Duration(c.TokenTTLSeconds) * time.Second
}
func (c TrustedLANEnrollmentConfig) RateWindow() time.Duration {
	return time.Duration(c.RateWindowSeconds) * time.Second
}
