package normalize

import "testing"

func TestQuery(t *testing.T) {
	got := Query("  iPhone   15 PRO  ")
	if got != "iphone 15 pro" {
		t.Fatalf("Query() = %q", got)
	}

	got = Query("Ёлка")
	if got != "елка" {
		t.Fatalf("Query() = %q", got)
	}
}
