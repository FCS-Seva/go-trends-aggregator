package stoplist

import (
	"sort"
	"sync"
	"sync/atomic"

	"github.com/FCS-Seva/go-trends-aggregator/internal/normalize"
)

type Manager struct {
	mu  sync.Mutex
	ptr atomic.Pointer[map[string]struct{}]
}

func New() *Manager {
	m := make(map[string]struct{})
	s := &Manager{}
	s.ptr.Store(&m)
	return s
}

func (s *Manager) ContainsNormalized(query string) bool {
	m := s.ptr.Load()
	if m == nil {
		return false
	}
	_, ok := (*m)[query]
	return ok
}

func (s *Manager) Add(term string) (string, bool) {
	term = normalize.StopTerm(term)
	if term == "" {
		return "", false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	oldPtr := s.ptr.Load()
	old := map[string]struct{}{}
	if oldPtr != nil {
		old = *oldPtr
	}
	next := make(map[string]struct{}, len(old)+1)
	for k := range old {
		next[k] = struct{}{}
	}
	next[term] = struct{}{}
	s.ptr.Store(&next)
	return term, true
}

func (s *Manager) Delete(term string) (string, bool) {
	term = normalize.StopTerm(term)
	if term == "" {
		return "", false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	oldPtr := s.ptr.Load()
	if oldPtr == nil {
		return term, false
	}
	old := *oldPtr
	if _, ok := old[term]; !ok {
		return term, false
	}
	next := make(map[string]struct{}, len(old)-1)
	for k := range old {
		if k != term {
			next[k] = struct{}{}
		}
	}
	s.ptr.Store(&next)
	return term, true
}

func (s *Manager) List() []string {
	m := s.ptr.Load()
	if m == nil {
		return nil
	}
	terms := make([]string, 0, len(*m))
	for k := range *m {
		terms = append(terms, k)
	}
	sort.Strings(terms)
	return terms
}

func (s *Manager) Size() int {
	m := s.ptr.Load()
	if m == nil {
		return 0
	}
	return len(*m)
}
