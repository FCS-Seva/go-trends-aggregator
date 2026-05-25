package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/FCS-Seva/go-trends-aggregator/internal/aggregator"
	"github.com/FCS-Seva/go-trends-aggregator/internal/config"
	"github.com/FCS-Seva/go-trends-aggregator/internal/domain"
	"github.com/FCS-Seva/go-trends-aggregator/internal/metrics"
	"github.com/FCS-Seva/go-trends-aggregator/internal/normalize"
)

type NATSConsumer struct {
	cfg     config.Config
	agg     *aggregator.Aggregator
	metrics *metrics.Metrics
	log     *slog.Logger
}

func NewNATSConsumer(cfg config.Config, agg *aggregator.Aggregator, m *metrics.Metrics, log *slog.Logger) *NATSConsumer {
	return &NATSConsumer{cfg: cfg, agg: agg, metrics: m, log: log}
}

func (c *NATSConsumer) Run(ctx context.Context) error {
	nc, err := c.connect(ctx)
	if err != nil {
		return err
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		return fmt.Errorf("create jetstream context: %w", err)
	}
	stream, err := c.ensureStream(ctx, js)
	if err != nil {
		return err
	}
	consumer, err := c.createEphemeralConsumer(ctx, stream)
	if err != nil {
		return err
	}

	msgs := make(chan jetstream.Msg, c.cfg.QueueCapacity)
	go c.runConsumerMetrics(ctx, consumer, msgs)

	var wg sync.WaitGroup
	for i := 0; i < c.cfg.WorkerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for msg := range msgs {
				c.handleMessage(ctx, msg)
			}
		}()
	}
	defer func() {
		close(msgs)
		wg.Wait()
	}()

	c.log.Info("jetstream consumer started", "stream", c.cfg.NATSStream, "filter", c.cfg.NATSFilter)
	for ctx.Err() == nil {
		batch, err := consumer.Fetch(c.cfg.NATSFetchBatch, jetstream.FetchMaxWait(c.cfg.NATSFetchWait))
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			c.log.Warn("fetch messages", "error", err)
			time.Sleep(200 * time.Millisecond)
			continue
		}
		for msg := range batch.Messages() {
			select {
			case msgs <- msg:
			case <-ctx.Done():
				_ = msg.Nak()
				return ctx.Err()
			}
		}
		if err := batch.Error(); err != nil && !errors.Is(err, context.Canceled) {
			c.log.Debug("fetch batch completed with error", "error", err)
		}
	}
	return ctx.Err()
}

func (c *NATSConsumer) connect(ctx context.Context) (*nats.Conn, error) {
	for {
		nc, err := nats.Connect(c.cfg.NATSURL, nats.Name("wb-search-trends"))
		if err == nil {
			return nc, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		c.log.Warn("connect nats", "url", c.cfg.NATSURL, "error", err)
		select {
		case <-time.After(time.Second):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (c *NATSConsumer) ensureStream(ctx context.Context, js jetstream.JetStream) (jetstream.Stream, error) {
	stream, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      c.cfg.NATSStream,
		Subjects:  c.cfg.NATSSubjects,
		Retention: jetstream.LimitsPolicy,
		Storage:   jetstream.FileStorage,
		MaxAge:    c.cfg.WindowDuration + c.cfg.LatenessTolerance + 5*time.Minute,
		MaxBytes:  1 << 30,
		Discard:   jetstream.DiscardOld,
	})
	if err != nil {
		return nil, fmt.Errorf("ensure stream %s: %w", c.cfg.NATSStream, err)
	}
	return stream, nil
}

func (c *NATSConsumer) createEphemeralConsumer(ctx context.Context, stream jetstream.Stream) (jetstream.Consumer, error) {
	start := time.Now().Add(-c.cfg.WindowDuration - c.cfg.LatenessTolerance)
	consumer, err := stream.CreateConsumer(ctx, jetstream.ConsumerConfig{
		DeliverPolicy:     jetstream.DeliverByStartTimePolicy,
		OptStartTime:      &start,
		AckPolicy:         jetstream.AckExplicitPolicy,
		AckWait:           c.cfg.NATSAckWait,
		MaxAckPending:     c.cfg.MaxAckPending,
		FilterSubject:     c.cfg.NATSFilter,
		ReplayPolicy:      jetstream.ReplayInstantPolicy,
		InactiveThreshold: 10 * time.Minute,
	})
	if err != nil {
		return nil, fmt.Errorf("create ephemeral consumer: %w", err)
	}
	return consumer, nil
}

func (c *NATSConsumer) runConsumerMetrics(ctx context.Context, consumer jetstream.Consumer, msgs <-chan jetstream.Msg) {
	queueTicker := time.NewTicker(250 * time.Millisecond)
	defer queueTicker.Stop()
	jsTicker := time.NewTicker(2 * time.Second)
	defer jsTicker.Stop()

	for {
		select {
		case <-queueTicker.C:
			c.metrics.QueueDepth.Set(float64(len(msgs)))
		case <-jsTicker.C:
			infoCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			info, err := consumer.Info(infoCtx)
			cancel()
			if err != nil {
				c.log.Debug("consumer info", "error", err)
				continue
			}
			c.metrics.JetStreamPending.Set(float64(info.NumPending))
			c.metrics.JetStreamAckPending.Set(float64(info.NumAckPending))
			c.metrics.JetStreamWaiting.Set(float64(info.NumWaiting))
		case <-ctx.Done():
			return
		}
	}
}

func (c *NATSConsumer) handleMessage(ctx context.Context, msg jetstream.Msg) {
	c.metrics.EventsReceived.WithLabelValues(msg.Subject()).Inc()
	var event domain.SearchEvent
	if err := json.Unmarshal(msg.Data(), &event); err != nil {
		c.metrics.EventsInvalid.WithLabelValues("bad_json").Inc()
		c.ack(ctx, msg)
		return
	}
	normalized, reason, ok := normalizeEvent(event)
	if !ok {
		c.metrics.EventsInvalid.WithLabelValues(reason).Inc()
		c.ack(ctx, msg)
		return
	}
	result := c.agg.Ingest(normalized)
	switch result.Status {
	case aggregator.StatusExpired, aggregator.StatusFuture:
		c.metrics.EventsDropped.WithLabelValues(string(result.Status)).Inc()
	case aggregator.StatusDuplicate:
		c.metrics.EventsDuplicateVote.Inc()
	}
	c.ack(ctx, msg)
}

func (c *NATSConsumer) ack(ctx context.Context, msg jetstream.Msg) {
	if ctx.Err() != nil {
		return
	}
	if err := msg.Ack(); err != nil {
		c.log.Warn("ack message", "error", err)
	}
}

func normalizeEvent(event domain.SearchEvent) (domain.NormalizedEvent, string, bool) {
	if event.EventID == "" {
		return domain.NormalizedEvent{}, "empty_event_id", false
	}
	query := normalize.Query(event.Query)
	if query == "" {
		return domain.NormalizedEvent{}, "empty_query", false
	}
	if event.ActorID == "" {
		return domain.NormalizedEvent{}, "empty_actor_id", false
	}
	if event.Timestamp.IsZero() {
		return domain.NormalizedEvent{}, "empty_timestamp", false
	}
	return domain.NormalizedEvent{
		EventID:   event.EventID,
		Query:     query,
		ActorID:   event.ActorID,
		SessionID: event.SessionID,
		Timestamp: event.Timestamp,
	}, "", true
}
