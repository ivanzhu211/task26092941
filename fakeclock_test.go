package collector

import (
	"sync"
	"time"
)

// fakeTimer 假时钟定时器。
type fakeTimer struct {
	at      time.Time
	f       func()
	stopped bool
}

// fakeClock 手动推进的时钟，用于确定性测试。
type fakeClock struct {
	mu      sync.Mutex
	now     time.Time
	timers  []*fakeTimer
	changed chan struct{} // 每次新增定时器时通知（非阻塞）
}

func newFakeClock() *fakeClock {
	return &fakeClock{
		now:     time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		changed: make(chan struct{}, 64),
	}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) AfterFunc(d time.Duration, f func()) Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &fakeTimer{at: c.now.Add(d), f: f}
	c.timers = append(c.timers, t)
	select {
	case c.changed <- struct{}{}:
	default:
	}
	return t
}

// Advance 推进时钟并依次触发到期定时器。
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	target := c.now.Add(d)
	for {
		// 找最早到期的定时器。
		var next *fakeTimer
		for _, t := range c.timers {
			if t.stopped || t.at.After(target) {
				continue
			}
			if next == nil || t.at.Before(next.at) {
				next = t
			}
		}
		if next == nil {
			break
		}
		next.stopped = true
		c.now = next.at
		f := next.f
		c.mu.Unlock()
		f()
		c.mu.Lock()
	}
	c.now = target
	c.mu.Unlock()
}

// Stop 实现 Timer 接口。
func (t *fakeTimer) Stop() bool {
	t.stopped = true
	return true
}
