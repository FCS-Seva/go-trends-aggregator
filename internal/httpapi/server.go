package httpapi

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/FCS-Seva/go-trends-aggregator/internal/aggregator"
	"github.com/FCS-Seva/go-trends-aggregator/internal/domain"
	"github.com/FCS-Seva/go-trends-aggregator/internal/metrics"
	"github.com/FCS-Seva/go-trends-aggregator/internal/stoplist"
)

type Server struct {
	snapshots   *aggregator.SnapshotStore
	stoplist    *stoplist.Manager
	metrics     *metrics.Metrics
	maxTopLimit int
}

func NewServer(snapshots *aggregator.SnapshotStore, stoplist *stoplist.Manager, metrics *metrics.Metrics, maxTopLimit int) http.Handler {
	s := &Server{snapshots: snapshots, stoplist: stoplist, metrics: metrics, maxTopLimit: maxTopLimit}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.healthz)
	mux.HandleFunc("GET /readyz", s.readyz)
	mux.HandleFunc("GET /v1/trends", s.trends)
	mux.HandleFunc("GET /v1/stoplist", s.getStoplist)
	mux.HandleFunc("POST /v1/stoplist", s.addStoplist)
	mux.HandleFunc("DELETE /v1/stoplist/", s.deleteStoplist)
	mux.Handle("GET /metrics", promhttp.Handler())
	return s.instrument(mux)
}

func (s *Server) healthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	snapshot := s.snapshots.Load()
	if snapshot == nil || snapshot.UpdatedAt.IsZero() || time.Since(snapshot.UpdatedAt) > 2*time.Second {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not_ready"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (s *Server) trends(w http.ResponseWriter, r *http.Request) {
	limit := parseLimit(r, s.maxTopLimit)
	debug := r.URL.Query().Get("debug") == "true"
	snapshot := s.snapshots.Load()
	if snapshot == nil {
		writeJSON(w, http.StatusOK, domain.TrendsResponse{Items: []domain.TrendItem{}})
		return
	}
	if limit > len(snapshot.Top) {
		limit = len(snapshot.Top)
	}
	items := make([]domain.TrendItem, 0, limit)
	for i := 0; i < limit; i++ {
		entry := snapshot.Top[i]
		item := domain.TrendItem{
			Rank:  i + 1,
			Query: entry.Query,
			Score: entry.Score,
		}
		if debug {
			item.Raw = entry.Raw
		}
		items = append(items, item)
	}
	writeJSON(w, http.StatusOK, domain.TrendsResponse{
		WindowSeconds: snapshot.WindowSeconds,
		GeneratedAt:   snapshot.UpdatedAt,
		Items:         items,
	})
}

func (s *Server) getStoplist(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"terms": s.stoplist.List()})
}

func (s *Server) addStoplist(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	var req struct {
		Term string `json:"term"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad_json"})
		return
	}
	term, ok := s.stoplist.Add(req.Term)
	if !ok {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "empty_term"})
		return
	}
	s.metrics.StoplistSize.Set(float64(s.stoplist.Size()))
	s.metrics.StoplistUpdates.WithLabelValues("add").Inc()
	writeJSON(w, http.StatusOK, map[string]string{"term": term})
}

func (s *Server) deleteStoplist(w http.ResponseWriter, r *http.Request) {
	raw := strings.TrimPrefix(r.URL.Path, "/v1/stoplist/")
	term, err := url.PathUnescape(raw)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad_term"})
		return
	}
	normalized, deleted := s.stoplist.Delete(term)
	s.metrics.StoplistSize.Set(float64(s.stoplist.Size()))
	if deleted {
		s.metrics.StoplistUpdates.WithLabelValues("delete").Inc()
	}
	writeJSON(w, http.StatusOK, map[string]any{"term": normalized, "deleted": deleted})
}

func parseLimit(r *http.Request, maxLimit int) int {
	limit := 20
	if raw := r.URL.Query().Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err == nil && n > 0 {
			limit = n
		}
	}
	if limit > maxLimit {
		return maxLimit
	}
	return limit
}

func (s *Server) instrument(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rw, r)
		endpoint := routeLabel(r)
		s.metrics.APIRequests.WithLabelValues(endpoint, strconv.Itoa(rw.status)).Inc()
		s.metrics.APIRequestDuration.WithLabelValues(endpoint).Observe(time.Since(start).Seconds())
	})
}

func routeLabel(r *http.Request) string {
	switch {
	case r.URL.Path == "/v1/trends":
		return "trends"
	case r.URL.Path == "/v1/stoplist":
		return "stoplist"
	case strings.HasPrefix(r.URL.Path, "/v1/stoplist/"):
		return "stoplist_term"
	case r.URL.Path == "/metrics":
		return "metrics"
	case r.URL.Path == "/healthz":
		return "healthz"
	case r.URL.Path == "/readyz":
		return "readyz"
	default:
		return "unknown"
	}
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
