package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/FCS-Seva/go-trends-aggregator/internal/aggregator"
	"github.com/FCS-Seva/go-trends-aggregator/internal/metrics"
	"github.com/FCS-Seva/go-trends-aggregator/internal/stoplist"
)

func TestTrendsLimitValidation(t *testing.T) {
	snapshots := aggregator.NewSnapshotStore()
	snapshots.Store(&aggregator.Snapshot{
		Top: []aggregator.Entry{
			{Query: "iphone", Score: 3, Raw: 5},
			{Query: "shoes", Score: 2, Raw: 2},
			{Query: "bag", Score: 1, Raw: 1},
		},
		UpdatedAt:     time.Now(),
		WindowSeconds: 300,
	})
	handler := NewServer(snapshots, stoplist.New(), metrics.New(prometheus.NewRegistry()), 2)

	tests := []struct {
		name      string
		target    string
		wantCode  int
		wantItems int
	}{
		{name: "valid limit", target: "/v1/trends?limit=1", wantCode: http.StatusOK, wantItems: 1},
		{name: "clamp max limit", target: "/v1/trends?limit=100", wantCode: http.StatusOK, wantItems: 2},
		{name: "invalid limit", target: "/v1/trends?limit=abc", wantCode: http.StatusBadRequest},
		{name: "non positive limit", target: "/v1/trends?limit=0", wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tt.target, nil)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			if rec.Code != tt.wantCode {
				t.Fatalf("status = %d, want %d, body=%s", rec.Code, tt.wantCode, rec.Body.String())
			}
			if tt.wantCode != http.StatusOK {
				return
			}

			var resp struct {
				Items []struct {
					Query string `json:"query"`
				} `json:"items"`
			}
			if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
				t.Fatalf("decode response: %v", err)
			}
			if len(resp.Items) != tt.wantItems {
				t.Fatalf("items len = %d, want %d", len(resp.Items), tt.wantItems)
			}
		})
	}
}
