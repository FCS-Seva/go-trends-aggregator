package aggregator

import (
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"
)

type Counters struct {
	Raw   int64
	Score int64
}

type IngestStatus string

const (
	StatusAccepted  IngestStatus = "accepted"
	StatusExpired   IngestStatus = "expired"
	StatusFuture    IngestStatus = "future"
	StatusDuplicate IngestStatus = "duplicate_vote"
)

type IngestResult struct {
	Status IngestStatus
	Raw    bool
	Score  bool
}

type Config struct {
	WindowDuration    time.Duration
	BucketDuration    time.Duration
	LatenessTolerance time.Duration
	ShardCount        int
	Clock             func() time.Time
}

type Aggregator struct {
	windowSec int64
	bucketSec int64
	buckets   []bucket
	shards    []countShard
	seen      *seenVotes
	clock     func() time.Time
}

type bucket struct {
	mu          sync.Mutex
	epoch       int64
	countDelta  map[string]Counters
	votesIssued map[string]int64
}

type countShard struct {
	mu     sync.RWMutex
	counts map[string]Counters
}

func New(cfg Config) *Aggregator {
	if cfg.BucketDuration <= 0 {
		cfg.BucketDuration = time.Second
	}
	if cfg.WindowDuration <= 0 {
		cfg.WindowDuration = 5 * time.Minute
	}
	if cfg.LatenessTolerance < 0 {
		cfg.LatenessTolerance = 0
	}
	if cfg.ShardCount <= 0 {
		cfg.ShardCount = 256
	}
	if cfg.Clock == nil {
		cfg.Clock = time.Now
	}
	bucketCount := int(cfg.WindowDuration / cfg.BucketDuration)
	a := &Aggregator{
		windowSec: int64(cfg.WindowDuration / time.Second),
		bucketSec: int64(cfg.BucketDuration / time.Second),
		buckets:   make([]bucket, bucketCount),
		shards:    make([]countShard, cfg.ShardCount),
		seen:      newSeenVotes(cfg.ShardCount),
		clock:     cfg.Clock,
	}
	for i := range a.buckets {
		a.buckets[i].countDelta = make(map[string]Counters)
		a.buckets[i].votesIssued = make(map[string]int64)
	}
	for i := range a.shards {
		a.shards[i].counts = make(map[string]Counters)
	}
	return a
}

func (a *Aggregator) UniqueQueries() int {
	total := 0
	for i := range a.shards {
		sh := &a.shards[i]
		sh.mu.RLock()
		total += len(sh.counts)
		sh.mu.RUnlock()
	}
	return total
}

func (a *Aggregator) shard(query string) *countShard {
	return &a.shards[hashString(query)%uint32(len(a.shards))]
}

func (a *Aggregator) bucketIndex(epoch int64) int {
	return int((epoch / a.bucketSec) % int64(len(a.buckets)))
}

func addCounters(m map[string]Counters, query string, delta Counters) {
	c := m[query]
	c.Raw += delta.Raw
	c.Score += delta.Score
	m[query] = c
}

func bucketEpoch(sec, bucketSec int64) int64 {
	return sec - sec%bucketSec
}

func voteKey(actorID, query string) string {
	return actorID + "\x00" + query
}

func hashString(s string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	return h.Sum32()
}

type Entry struct {
	Query string
	Score int64
	Raw   int64
}

type Snapshot struct {
	Top           []Entry
	UpdatedAt     time.Time
	WindowSeconds int64
}

type SnapshotStore struct {
	ptr atomic.Pointer[Snapshot]
}

func NewSnapshotStore() *SnapshotStore {
	s := &SnapshotStore{}
	s.Store(&Snapshot{Top: []Entry{}, UpdatedAt: time.Time{}, WindowSeconds: 0})
	return s
}

func (s *SnapshotStore) Store(snapshot *Snapshot) {
	s.ptr.Store(snapshot)
}

func (s *SnapshotStore) Load() *Snapshot {
	return s.ptr.Load()
}

type seenVotes struct {
	shards []seenShard
}

type seenShard struct {
	mu sync.Mutex
	m  map[string]int64
}

func newSeenVotes(shardCount int) *seenVotes {
	s := &seenVotes{shards: make([]seenShard, shardCount)}
	for i := range s.shards {
		s.shards[i].m = make(map[string]int64)
	}
	return s
}

func (s *seenVotes) tryVote(key string, eventSec, expiresAt int64) bool {
	sh := &s.shards[hashString(key)%uint32(len(s.shards))]
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if exp, ok := sh.m[key]; ok && exp > eventSec {
		return false
	}
	sh.m[key] = expiresAt
	return true
}

func (s *seenVotes) deleteIfValue(key string, expiresAt int64) {
	sh := &s.shards[hashString(key)%uint32(len(s.shards))]
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if exp, ok := sh.m[key]; ok && exp == expiresAt {
		delete(sh.m, key)
	}
}
