package sched

import (
	"sync"
	"time"
)

// fakeClock 手动推进的时钟，定时器在 Advance 调用方 goroutine 中同步触发。
type fakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []fakeTimer
}

type fakeTimer struct {
	at time.Time
	f  func()
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) AfterFunc(d time.Duration, f func()) {
	c.mu.Lock()
	c.timers = append(c.timers, fakeTimer{at: c.now.Add(d), f: f})
	c.mu.Unlock()
}

// Advance 将时间推进 d，并同步触发沿途所有到期定时器。
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	target := c.now.Add(d)
	for {
		idx := -1
		for i, tm := range c.timers {
			if !tm.at.After(target) && (idx == -1 || tm.at.Before(c.timers[idx].at)) {
				idx = i
			}
		}
		if idx == -1 {
			break
		}
		tm := c.timers[idx]
		c.timers = append(c.timers[:idx], c.timers[idx+1:]...)
		c.now = tm.at
		c.mu.Unlock()
		tm.f()
		c.mu.Lock()
	}
	c.now = target
	c.mu.Unlock()
}
