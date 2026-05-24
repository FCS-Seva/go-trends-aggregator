package aggregator

import (
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
