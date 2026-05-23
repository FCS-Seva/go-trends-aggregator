package domain

import "time"

type SearchEvent struct {
	EventID   string    `json:"event_id"`
	Query     string    `json:"query"`
	ActorID   string    `json:"actor_id"`
	SessionID string    `json:"session_id,omitempty"`
	Timestamp time.Time `json:"timestamp"`
}

type NormalizedEvent struct {
	EventID   string
	Query     string
	ActorID   string
	SessionID string
	Timestamp time.Time
}

type TrendItem struct {
	Rank  int    `json:"rank"`
	Query string `json:"query"`
	Score int64  `json:"score"`
	Raw   int64  `json:"raw,omitempty"`
}

type TrendsResponse struct {
	WindowSeconds int64       `json:"window_seconds"`
	GeneratedAt   time.Time   `json:"generated_at"`
	Items         []TrendItem `json:"items"`
}
