package config

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"time"
)

type Config struct {
	HTTPAddr string

	NATSURL        string
	NATSStream     string
	NATSSubjects   []string
	NATSFilter     string
	NATSFetchBatch int
	NATSFetchWait  time.Duration
	NATSAckWait    time.Duration
	MaxAckPending  int

	WindowDuration    time.Duration
	BucketDuration    time.Duration
	LatenessTolerance time.Duration
	SnapshotInterval  time.Duration
	ShardCount        int
	MaxTopLimit       int
	QueueCapacity     int
	WorkerCount       int
}

func Load() (Config, error) {
	cfg := Config{
		HTTPAddr:          envString("HTTP_ADDR", ":8080"),
		NATSURL:           envString("NATS_URL", "nats://localhost:4222"),
		NATSStream:        envString("NATS_STREAM", "SEARCH_EVENTS"),
		NATSSubjects:      []string{envString("NATS_SUBJECTS", "search.events.*")},
		NATSFilter:        envString("NATS_FILTER", "search.events.*"),
		NATSFetchBatch:    envInt("NATS_FETCH_BATCH", 500),
		NATSFetchWait:     envDuration("NATS_FETCH_TIMEOUT", 100*time.Millisecond),
		NATSAckWait:       envDuration("NATS_ACK_WAIT", 30*time.Second),
		MaxAckPending:     envInt("NATS_MAX_ACK_PENDING", 10_000),
		WindowDuration:    envDuration("WINDOW_DURATION", 5*time.Minute),
		BucketDuration:    envDuration("BUCKET_DURATION", time.Second),
		LatenessTolerance: envDuration("LATENESS_TOLERANCE", 10*time.Second),
		SnapshotInterval:  envDuration("SNAPSHOT_INTERVAL", time.Second),
		ShardCount:        envInt("SHARD_COUNT", 256),
		MaxTopLimit:       envInt("MAX_TOP_LIMIT", 1000),
		QueueCapacity:     envInt("QUEUE_CAPACITY", 10_000),
		WorkerCount:       envInt("WORKER_COUNT", 0),
	}
	if cfg.WorkerCount <= 0 {
		cfg.WorkerCount = runtime.NumCPU()
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) Validate() error {
	if c.WindowDuration <= 0 {
		return fmt.Errorf("WINDOW_DURATION must be positive")
	}
	if c.BucketDuration <= 0 {
		return fmt.Errorf("BUCKET_DURATION must be positive")
	}
	if c.WindowDuration%c.BucketDuration != 0 {
		return fmt.Errorf("WINDOW_DURATION must be divisible by BUCKET_DURATION")
	}
	if c.BucketDuration < time.Second || c.BucketDuration%time.Second != 0 {
		return fmt.Errorf("BUCKET_DURATION must be a whole number of seconds and at least 1s")
	}
	if c.ShardCount <= 0 {
		return fmt.Errorf("SHARD_COUNT must be positive")
	}
	if c.MaxTopLimit <= 0 {
		return fmt.Errorf("MAX_TOP_LIMIT must be positive")
	}
	if c.NATSFetchBatch <= 0 {
		return fmt.Errorf("NATS_FETCH_BATCH must be positive")
	}
	if c.QueueCapacity <= 0 {
		return fmt.Errorf("QUEUE_CAPACITY must be positive")
	}
	return nil
}

func envString(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}

func envDuration(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return fallback
	}
	return d
}
