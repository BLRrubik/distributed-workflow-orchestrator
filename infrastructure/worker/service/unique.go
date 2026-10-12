package service

import (
	"context"
	"sync"
)

// unique — реестр задач в полёте: ключ есть, пока задача не отработала успешно.
// Значение — cancel её текущего исполнения (nil, пока Do ещё не выставил его через
// SetCancel), чтобы CancelTask мог остановить её тем же ключом, без отдельной мапы.
type unique struct {
	items map[string]context.CancelFunc
	mu    sync.Mutex
}

func newUnique() *unique {
	return &unique{
		items: map[string]context.CancelFunc{},
	}
}

// Add помечает id в полёте, если его там ещё нет. false — дубликат, id уже в полёте.
func (u *unique) Add(id string) bool {
	if u == nil {
		return true
	}

	u.mu.Lock()
	defer u.mu.Unlock()

	_, ok := u.items[id]
	if !ok {
		u.items[id] = nil
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

// SetCancel привязывает cancel текущей попытки исполнения к уже занятому Add id —
// вызывается в начале каждой попытки (и первого диспатча, и внутреннего ретрая пула).
func (u *unique) SetCancel(id string, cancel context.CancelFunc) {
	if u == nil {
		return
	}

	u.mu.Lock()
	defer u.mu.Unlock()

	u.items[id] = cancel
}

// Cancel — обработчик CancelTask: false, если id не в полёте или его cancel ещё
// не выставлен (гонка с самим стартом Do) — оба случая не ошибка, а норма.
func (u *unique) Cancel(id string) bool {
	if u == nil {
		return false
	}

	u.mu.Lock()
	cancel := u.items[id]
	u.mu.Unlock()

	if cancel == nil {
		return false
	}

	cancel()

	return true
}
