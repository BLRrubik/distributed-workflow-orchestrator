package service

import "sync"

type unique struct {
	items map[string]struct{}
	mu    sync.Mutex
}

func newUnique() *unique {
	return &unique{
		items: map[string]struct{}{},
	}
}

func (u *unique) Add(id string) bool {
	if u == nil {
		return true
	}

	u.mu.Lock()
	defer u.mu.Unlock()

	_, ok := u.items[id]
	if !ok {
		u.items[id] = struct{}{}
	}

	return !ok
}

func (u *unique) Remove(id string) {
	if u == nil {
		return
	}
	u.mu.Lock()
	defer u.mu.Unlock()

	if _, ok := u.items[id]; ok {
		delete(u.items, id)
	}
}
