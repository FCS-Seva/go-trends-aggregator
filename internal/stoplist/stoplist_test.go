package stoplist

import "testing"

func TestManagerNormalizesTerms(t *testing.T) {
	s := New()
	term, ok := s.Add("  iPhone   15 ")
	if !ok {
		t.Fatal("Add() returned false")
	}
	if term != "iphone 15" {
		t.Fatalf("term = %q", term)
	}
	if !s.ContainsNormalized("iphone 15") {
		t.Fatal("term not found")
	}
	if _, deleted := s.Delete("IPHONE 15"); !deleted {
		t.Fatal("Delete() returned false")
	}
	if s.ContainsNormalized("iphone 15") {
		t.Fatal("term still found after delete")
	}
}
