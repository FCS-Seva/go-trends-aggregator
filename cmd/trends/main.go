package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/FCS-Seva/go-trends-aggregator/internal/aggregator"
	"github.com/FCS-Seva/go-trends-aggregator/internal/config"
	"github.com/FCS-Seva/go-trends-aggregator/internal/consumer"
	"github.com/FCS-Seva/go-trends-aggregator/internal/httpapi"
	"github.com/FCS-Seva/go-trends-aggregator/internal/metrics"
	"github.com/FCS-Seva/go-trends-aggregator/internal/stoplist"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg, err := config.Load()
	if err != nil {
		log.Error("load config", "error", err)
		os.Exit(1)
	}

	m := metrics.New(prometheus.DefaultRegisterer)
	stop := stoplist.New()
	agg := aggregator.New(aggregator.Config{
		WindowDuration:    cfg.WindowDuration,
		BucketDuration:    cfg.BucketDuration,
		LatenessTolerance: cfg.LatenessTolerance,
		ShardCount:        cfg.ShardCount,
	})
	snapshots := aggregator.NewSnapshotStore()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	go runSnapshotLoop(ctx, agg, stop, snapshots, m, cfg, log)

	errCh := make(chan error, 2)
	go func() {
		nc := consumer.NewNATSConsumer(cfg, agg, m, log)
		if err := nc.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			errCh <- err
		}
	}()

	server := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           httpapi.NewServer(snapshots, stop, m, cfg.MaxTopLimit),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		log.Info("http server started", "addr", cfg.HTTPAddr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
	case err := <-errCh:
		log.Error("service error", "error", err)
		cancel()
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Warn("http shutdown", "error", err)
	}
}

func runSnapshotLoop(ctx context.Context, agg *aggregator.Aggregator, stop *stoplist.Manager, snapshots *aggregator.SnapshotStore, m *metrics.Metrics, cfg config.Config, log *slog.Logger) {
	build := func() {
		start := time.Now()
		now := time.Now()
		agg.Rotate(now)
		snapshot := agg.BuildSnapshot(stop.ContainsNormalized, cfg.MaxTopLimit, now)
		snapshots.Store(snapshot)
		m.SnapshotBuildDuration.Observe(time.Since(start).Seconds())
		m.SnapshotTopSize.Set(float64(len(snapshot.Top)))
		m.SnapshotAgeSeconds.Set(time.Since(snapshot.UpdatedAt).Seconds())
		m.UniqueQueries.Set(float64(agg.UniqueQueries()))
		m.StoplistSize.Set(float64(stop.Size()))
	}

	build()
	ticker := time.NewTicker(cfg.SnapshotInterval)
	defer ticker.Stop()
	ageTicker := time.NewTicker(200 * time.Millisecond)
	defer ageTicker.Stop()

	for {
		select {
		case <-ticker.C:
			build()
		case <-ageTicker.C:
			snapshot := snapshots.Load()
			if snapshot != nil && !snapshot.UpdatedAt.IsZero() {
				m.SnapshotAgeSeconds.Set(time.Since(snapshot.UpdatedAt).Seconds())
			}
		case <-ctx.Done():
			log.Info("snapshot loop stopped")
			return
		}
	}
}
