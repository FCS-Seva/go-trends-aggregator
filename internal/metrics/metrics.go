package metrics

import "github.com/prometheus/client_golang/prometheus"

type Metrics struct {
	EventsReceived      *prometheus.CounterVec
	EventsInvalid       *prometheus.CounterVec
	EventsDropped       *prometheus.CounterVec
	EventsDuplicateVote prometheus.Counter

	QueueDepth    prometheus.Gauge
	UniqueQueries prometheus.Gauge

	JetStreamPending    prometheus.Gauge
	JetStreamAckPending prometheus.Gauge
	JetStreamWaiting    prometheus.Gauge

	SnapshotBuildDuration prometheus.Histogram
	SnapshotTopSize       prometheus.Gauge
	SnapshotAgeSeconds    prometheus.Gauge

	APIRequests        *prometheus.CounterVec
	APIRequestDuration *prometheus.HistogramVec

	StoplistSize    prometheus.Gauge
	StoplistUpdates *prometheus.CounterVec
}

func New(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		EventsReceived: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "events_received_total",
			Help: "Search events received from JetStream.",
		}, []string{"subject"}),
		EventsInvalid: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "events_invalid_total",
			Help: "Invalid search events acknowledged and dropped.",
		}, []string{"reason"}),
		EventsDropped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "events_dropped_total",
			Help: "Valid search events dropped by business rules.",
		}, []string{"reason"}),
		EventsDuplicateVote: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "events_duplicate_vote_total",
			Help: "Events that increased raw_count but did not increase score.",
		}),
		QueueDepth: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "agg_queue_depth",
			Help: "Current bounded ingest queue depth.",
		}),
		UniqueQueries: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "unique_queries",
			Help: "Current number of normalized queries in the rolling window.",
		}),
		JetStreamPending: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "jetstream_pending_messages",
			Help: "Messages matching the consumer filter that have not been delivered yet.",
		}),
		JetStreamAckPending: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "jetstream_ack_pending_messages",
			Help: "Messages delivered by JetStream but not yet acknowledged.",
		}),
		JetStreamWaiting: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "jetstream_waiting_fetches",
			Help: "Active pull requests waiting on the JetStream consumer.",
		}),
		SnapshotBuildDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "snapshot_build_duration_seconds",
			Help:    "Snapshot build duration.",
			Buckets: prometheus.DefBuckets,
		}),
		SnapshotTopSize: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "snapshot_top_size",
			Help: "Number of entries in the current top snapshot.",
		}),
		SnapshotAgeSeconds: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "snapshot_age_seconds",
			Help: "Current snapshot age in seconds.",
		}),
		APIRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "api_requests_total",
			Help: "HTTP API requests.",
		}, []string{"endpoint", "code"}),
		APIRequestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "api_request_duration_seconds",
			Help:    "HTTP API request duration.",
			Buckets: prometheus.DefBuckets,
		}, []string{"endpoint"}),
		StoplistSize: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "stoplist_size",
			Help: "Current number of stop-list terms.",
		}),
		StoplistUpdates: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "stoplist_updates_total",
			Help: "Stop-list update operations.",
		}, []string{"op"}),
	}
	reg.MustRegister(
		m.EventsReceived,
		m.EventsInvalid,
		m.EventsDropped,
		m.EventsDuplicateVote,
		m.QueueDepth,
		m.UniqueQueries,
		m.JetStreamPending,
		m.JetStreamAckPending,
		m.JetStreamWaiting,
		m.SnapshotBuildDuration,
		m.SnapshotTopSize,
		m.SnapshotAgeSeconds,
		m.APIRequests,
		m.APIRequestDuration,
		m.StoplistSize,
		m.StoplistUpdates,
	)
	return m
}
