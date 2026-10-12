package service

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUnique_Add_FirstTimeTrueThenFalse(t *testing.T) {
	u := newUnique()

	assert.True(t, u.Add("a"), "первый Add по ключу должен вернуть true")
	assert.False(t, u.Add("a"), "повторный Add тем же ключом — дубликат, false")
}

func TestUnique_Remove_AllowsReAdd(t *testing.T) {
	u := newUnique()

	require := assert.New(t)
	require.True(u.Add("a"))

	u.Remove("a")

	require.True(u.Add("a"), "после Remove ключ снова свободен")
}

func TestUnique_Remove_UnknownKey_NoPanic(t *testing.T) {
	u := newUnique()

	assert.NotPanics(t, func() { u.Remove("missing") })
}

func TestUnique_NilReceiver_AddAlwaysTrue(t *testing.T) {
	var u *unique

	assert.True(t, u.Add("a"))
	assert.True(t, u.Add("a"), "nil unique не хранит состояние — всегда true")
}

func TestUnique_NilReceiver_RemoveNoPanic(t *testing.T) {
	var u *unique

	assert.NotPanics(t, func() { u.Remove("a") })
}

func TestUnique_ConcurrentAdd_OnlyOneWinner(t *testing.T) {
	u := newUnique()

	const goroutines = 50

	var wg sync.WaitGroup

	wins := make(chan bool, goroutines)

	for range goroutines {
		wg.Add(1)

		go func() {
			defer wg.Done()

			wins <- u.Add("race-key")
		}()
	}

	wg.Wait()
	close(wins)

	trueCount := 0
	for w := range wins {
		if w {
			trueCount++
		}
	}

	assert.Equal(t, 1, trueCount, "только один вызов Add должен выиграть гонку за ключ")
}
