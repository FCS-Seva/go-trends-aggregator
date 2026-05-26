package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/FCS-Seva/go-trends-aggregator/internal/domain"
)

var queries = []string{
	"iphone 15 pro",
	"кроссовки мужские",
	"платье летнее",
	"наушники беспроводные",
	"рюкзак",
	"детские игрушки",
	"футболка оверсайз",
	"ноутбук",
	"кофемашина",
	"шампунь",
}

func main() {
	natsURL := envString("NATS_URL", "nats://localhost:4222")
	streamName := envString("NATS_STREAM", "SEARCH_EVENTS")
	subjectPrefix := envString("SUBJECT_PREFIX", "search.events")
	rps := envInt("RPS", 1000)
	duration := envDuration("DURATION", 0)

	ctx := context.Background()
	nc, err := nats.Connect(natsURL, nats.Name("wb-search-trends-loadgen"))
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()
	var asyncErrors atomic.Int64
	js, err := jetstream.New(
		nc,
		jetstream.WithPublishAsyncMaxPending(max(1_000, rps*2)),
		jetstream.WithPublishAsyncErrHandler(func(_ jetstream.JetStream, _ *nats.Msg, err error) {
			asyncErrors.Add(1)
			log.Printf("async publish: %v", err)
		}),
	)
	if err != nil {
		log.Fatal(err)
	}
	waitForStream(ctx, js, streamName)

	interval := time.Second / time.Duration(rps)
	if interval <= 0 {
		interval = time.Nanosecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var deadline <-chan time.Time
	if duration > 0 {
		timer := time.NewTimer(duration)
		defer timer.Stop()
		deadline = timer.C
	}

	log.Printf("publishing synthetic events to %s at %d rps", natsURL, rps)
	var sent int64
	for {
		select {
		case <-ticker.C:
			event := nextEvent(sent)
			body, _ := json.Marshal(event)
			subject := fmt.Sprintf("%s.%d", subjectPrefix, shard(event.ActorID, 16))
			if _, err := js.PublishAsync(subject, body); err != nil {
				log.Printf("publish: %v", err)
				continue
			}
			sent++
			if sent%int64(max(1, rps)) == 0 {
				log.Printf("sent=%d", sent)
			}
		case <-deadline:
			waitAsyncComplete(js)
			log.Printf("done, sent=%d async_errors=%d", sent, asyncErrors.Load())
			return
		}
	}
}

func waitForStream(ctx context.Context, js jetstream.JetStream, streamName string) {
	deadline, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		_, err := js.Stream(deadline, streamName)
		if err == nil {
			return
		}
		select {
		case <-ticker.C:
			log.Printf("waiting for stream %s: %v", streamName, err)
		case <-deadline.Done():
			log.Fatalf("stream %s is not available: %v", streamName, err)
		}
	}
}

func waitAsyncComplete(js jetstream.JetStream) {
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(10 * time.Second):
		log.Printf("async publish flush timed out, pending=%d", js.PublishAsyncPending())
	}
}

func nextEvent(n int64) domain.SearchEvent {
	actor := fmt.Sprintf("u_%06d", rand.Intn(20_000))
	query := queries[rand.Intn(len(queries))]
	if rand.Intn(100) < 5 {
		actor = "bot_1"
		query = "iphone 15 pro"
	}
	return domain.SearchEvent{
		EventID:   fmt.Sprintf("%d-%d", time.Now().UnixNano(), n),
		Query:     query,
		ActorID:   actor,
		SessionID: fmt.Sprintf("s_%06d", rand.Intn(100_000)),
		Timestamp: time.Now().UTC(),
	}
}

func shard(s string, mod uint32) uint32 {
	var h uint32 = 2166136261
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h % mod
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
	if err != nil || n <= 0 {
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
