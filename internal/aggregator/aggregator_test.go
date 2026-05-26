package aggregator

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/FCS-Seva/go-trends-aggregator/internal/domain"
)

func TestAggregatorCountsRawAndUniqueScore(t *testing.T) {
	now := time.Unix(1_000, 0)
	agg := testAggregator(&now, 5*time.Second)

	agg.Ingest(event("u1", "iphone", now))
	now = now.Add(time.Second)
	agg.Ingest(event("u1", "iphone", now))
	agg.Ingest(event("u2", "iphone", now))

	snapshot := agg.BuildSnapshot(nil, 10, now)
	if len(snapshot.Top) != 1 {
		t.Fatalf("top len = %d", len(snapshot.Top))
	}
	if snapshot.Top[0].Raw != 3 {
		t.Fatalf("raw = %d", snapshot.Top[0].Raw)
	}
	if snapshot.Top[0].Score != 2 {
		t.Fatalf("score = %d", snapshot.Top[0].Score)
	}
}

func TestAggregatorRotatesExpiredBuckets(t *testing.T) {
	now := time.Unix(1_000, 0)
	agg := testAggregator(&now, 5*time.Second)

	agg.Ingest(event("u1", "old", now))
	now = now.Add(6 * time.Second)
	agg.Rotate(now)

	snapshot := agg.BuildSnapshot(nil, 10, now)
	if len(snapshot.Top) != 0 {
		t.Fatalf("expected empty top, got %+v", snapshot.Top)
	}
}

func TestAggregatorAllowsNewVoteAfterWindow(t *testing.T) {
	now := time.Unix(1_000, 0)
	agg := testAggregator(&now, 5*time.Second)

	agg.Ingest(event("u1", "iphone", now))
	now = now.Add(6 * time.Second)
	agg.Rotate(now)
	agg.Ingest(event("u1", "iphone", now))

	snapshot := agg.BuildSnapshot(nil, 10, now)
	if len(snapshot.Top) != 1 {
		t.Fatalf("top len = %d", len(snapshot.Top))
	}
	if snapshot.Top[0].Raw != 1 || snapshot.Top[0].Score != 1 {
		t.Fatalf("entry = %+v", snapshot.Top[0])
	}
}

func TestAggregatorDropsExpiredAndFutureEvents(t *testing.T) {
	now := time.Unix(1_000, 0)
	agg := testAggregator(&now, 5*time.Second)

	expired := agg.Ingest(event("u1", "old", now.Add(-6*time.Second)))
	if expired.Status != StatusExpired {
		t.Fatalf("expired status = %s", expired.Status)
	}
	future := agg.Ingest(event("u1", "future", now.Add(3*time.Second)))
	if future.Status != StatusFuture {
		t.Fatalf("future status = %s", future.Status)
	}
}

func TestAggregatorSnapshotAppliesStoplist(t *testing.T) {
	now := time.Unix(1_000, 0)
	agg := testAggregator(&now, 5*time.Second)

	agg.Ingest(event("u1", "iphone", now))
	agg.Ingest(event("u2", "shoes", now))
	snapshot := agg.BuildSnapshot(func(q string) bool { return q == "iphone" }, 10, now)

	if len(snapshot.Top) != 1 {
		t.Fatalf("top len = %d", len(snapshot.Top))
	}
	if snapshot.Top[0].Query != "shoes" {
		t.Fatalf("query = %q", snapshot.Top[0].Query)
	}
}

func TestAggregatorConcurrentIngestRotateKeepsCountersConsistent(t *testing.T) {
	var nowSec atomic.Int64
	nowSec.Store(1_000)
	agg := New(Config{
		WindowDuration: time.Second,
		BucketDuration: time.Second,
		ShardCount:     8,
		Clock: func() time.Time {
			return time.Unix(nowSec.Load(), 0)
		},
	})

	const workers = 8
	const iterations = 1_000
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			actor := fmt.Sprintf("u-%d", worker)
			for j := 0; j < iterations; j++ {
				sec := nowSec.Load()
				agg.Ingest(event(actor, "q", time.Unix(sec, 0)))
				if j%10 == 0 {
					agg.Rotate(time.Unix(sec, 0))
				}
			}
		}(i)
	}

	for tick := int64(1_001); tick < 1_050; tick++ {
		nowSec.Store(tick)
		agg.Rotate(time.Unix(tick, 0))
		time.Sleep(100 * time.Microsecond)
	}
	wg.Wait()

	agg.assertConsistent(t)
	nowSec.Store(1_060)
	agg.Rotate(time.Unix(1_060, 0))
	agg.assertConsistent(t)
	if got := agg.UniqueQueries(); got != 0 {
		t.Fatalf("UniqueQueries() after full expiration = %d", got)
	}
}

func testAggregator(now *time.Time, window time.Duration) *Aggregator {
	return New(Config{
		WindowDuration:    window,
		BucketDuration:    time.Second,
		LatenessTolerance: time.Second,
		ShardCount:        8,
		Clock: func() time.Time {
			return *now
		},
	})
}

func event(actor, query string, ts time.Time) domain.NormalizedEvent {
	return domain.NormalizedEvent{
		EventID:   actor + "-" + query,
		Query:     query,
		ActorID:   actor,
		Timestamp: ts,
	}
}

func (a *Aggregator) assertConsistent(t *testing.T) {
	t.Helper()
	fromBuckets := make(map[string]Counters)
	for i := range a.buckets {
		b := &a.buckets[i]
		b.mu.Lock()
		for query, counters := range b.countDelta {
			addCounters(fromBuckets, query, counters)
			if counters.Raw < 0 || counters.Score < 0 {
				t.Fatalf("negative bucket counters for %q: %+v", query, counters)
			}
		}
		b.mu.Unlock()
	}

	fromGlobal := make(map[string]Counters)
	for i := range a.shards {
		sh := &a.shards[i]
		sh.mu.RLock()
		for query, counters := range sh.counts {
			fromGlobal[query] = counters
			if counters.Raw < 0 || counters.Score < 0 {
				t.Fatalf("negative global counters for %q: %+v", query, counters)
			}
		}
		sh.mu.RUnlock()
	}

	if len(fromBuckets) != len(fromGlobal) {
		t.Fatalf("bucket/global query count mismatch: buckets=%v global=%v", fromBuckets, fromGlobal)
	}
	for query, bucketCounters := range fromBuckets {
		if globalCounters, ok := fromGlobal[query]; !ok || globalCounters != bucketCounters {
			t.Fatalf("counter mismatch for %q: bucket=%+v global=%+v ok=%v", query, bucketCounters, globalCounters, ok)
		}
	}
}
