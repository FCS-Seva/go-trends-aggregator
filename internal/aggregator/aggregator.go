package aggregator

import (
	"container/heap"
	"hash/fnv"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/FCS-Seva/go-trends-aggregator/internal/domain"
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

func (a *Aggregator) Ingest(ev domain.NormalizedEvent) IngestResult {
	nowSec := a.clock().Unix()
	eventSec := ev.Timestamp.Unix()
	if eventSec <= nowSec-a.windowSec {
		return IngestResult{Status: StatusExpired}
	}
	if eventSec > nowSec {
		return IngestResult{Status: StatusFuture}
	}

	key := voteKey(ev.ActorID, ev.Query)
	expiresAt := bucketEpoch(eventSec, a.bucketSec) + a.windowSec
	epoch := bucketEpoch(eventSec, a.bucketSec)
	b := &a.buckets[a.bucketIndex(epoch)]

	b.mu.Lock()
	if b.epoch != epoch {
		a.expireBucketLocked(b)
		b.epoch = epoch
	}

	scoreDelta := int64(0)
	status := StatusDuplicate
	if a.seen.tryVote(key, eventSec, expiresAt) {
		scoreDelta = 1
		status = StatusAccepted
	}

	delta := Counters{Raw: 1, Score: scoreDelta}
	addCounters(b.countDelta, ev.Query, delta)
	if scoreDelta > 0 {
		b.votesIssued[key] = expiresAt
	}
	a.addGlobal(ev.Query, delta)
	b.mu.Unlock()

	return IngestResult{Status: status, Raw: true, Score: scoreDelta > 0}
}

func (a *Aggregator) Rotate(now time.Time) {
	cutoff := bucketEpoch(now.Unix(), a.bucketSec) - a.windowSec
	for i := range a.buckets {
		b := &a.buckets[i]
		b.mu.Lock()
		if b.epoch != 0 && b.epoch <= cutoff {
			a.expireBucketLocked(b)
		}
		b.mu.Unlock()
	}
}

func (a *Aggregator) BuildSnapshot(stop func(string) bool, limit int, now time.Time) *Snapshot {
	if limit <= 0 {
		limit = 1
	}
	h := &entryHeap{}
	heap.Init(h)
	for i := range a.shards {
		sh := &a.shards[i]
		sh.mu.RLock()
		for query, counters := range sh.counts {
			if counters.Score <= 0 {
				continue
			}
			if stop != nil && stop(query) {
				continue
			}
			entry := Entry{Query: query, Score: counters.Score, Raw: counters.Raw}
			if h.Len() < limit {
				heap.Push(h, entry)
				continue
			}
			if lessEntry((*h)[0], entry) {
				heap.Pop(h)
				heap.Push(h, entry)
			}
		}
		sh.mu.RUnlock()
	}
	items := make([]Entry, h.Len())
	for i := len(items) - 1; i >= 0; i-- {
		items[i] = heap.Pop(h).(Entry)
	}
	sort.Slice(items, func(i, j int) bool {
		return betterEntry(items[i], items[j])
	})
	return &Snapshot{Top: items, UpdatedAt: now, WindowSeconds: a.windowSec}
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

func (a *Aggregator) expireBucketLocked(b *bucket) {
	for query, delta := range b.countDelta {
		a.subGlobal(query, delta)
	}
	for key, expiresAt := range b.votesIssued {
		a.seen.deleteIfValue(key, expiresAt)
	}
	clear(b.countDelta)
	clear(b.votesIssued)
	b.epoch = 0
}

func (a *Aggregator) addGlobal(query string, delta Counters) {
	sh := a.shard(query)
	sh.mu.Lock()
	addCounters(sh.counts, query, delta)
	sh.mu.Unlock()
}

func (a *Aggregator) subGlobal(query string, delta Counters) {
	sh := a.shard(query)
	sh.mu.Lock()
	c := sh.counts[query]
	c.Raw -= delta.Raw
	c.Score -= delta.Score
	if c.Raw <= 0 && c.Score <= 0 {
		delete(sh.counts, query)
	} else {
		sh.counts[query] = c
	}
	sh.mu.Unlock()
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

type entryHeap []Entry

func (h entryHeap) Len() int { return len(h) }
func (h entryHeap) Less(i, j int) bool {
	return lessEntry(h[i], h[j])
}
func (h entryHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *entryHeap) Push(x any) {
	*h = append(*h, x.(Entry))
}
func (h *entryHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

func lessEntry(a, b Entry) bool {
	if a.Score != b.Score {
		return a.Score < b.Score
	}
	if a.Raw != b.Raw {
		return a.Raw < b.Raw
	}
	return a.Query > b.Query
}

func betterEntry(a, b Entry) bool {
	if a.Score != b.Score {
		return a.Score > b.Score
	}
	if a.Raw != b.Raw {
		return a.Raw > b.Raw
	}
	return a.Query < b.Query
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
