// Package sched 进程内采集调度库：状态仅保存在内存。
package sched

import (
	"sync"
	"time"
)

// State 任务状态。
type State int

const (
	StatePending   State = iota // 已受理，等待准入或退避
	StateRunning                // 回调执行中
	StateSucceeded              // 成功终态
	StateFailed                 // 失败终态（重试耗尽）
	StateCancelled              // 取消终态
)

func (s State) terminal() bool {
	return s == StateSucceeded || s == StateFailed || s == StateCancelled
}

// Callback 采集回调，返回 true 表示成功，false 表示失败。
type Callback func() bool

const (
	globalLimit   = 8                 // 全库最大并行执行数
	normalLimit   = 4                 // 单数据源正常并行度
	degradedLimit = 1                 // 单数据源降级并行度
	maxAttempts   = 5                 // 每任务最大执行次数
	window        = 300 * time.Second // 错误统计窗口
)

// 第 1~4 次失败后的退避等待。
var backoffs = []time.Duration{60 * time.Second, 120 * time.Second, 240 * time.Second, 480 * time.Second}

// sample 窗口内一次完成的尝试。
type sample struct {
	at     time.Time
	failed bool
}

// sourceState 单个数据源的共享状态：并行度计数与错误窗口。
type sourceState struct {
	inflight int
	window   []sample // 按完成时间升序
}

// Task 一个采集任务，属于某个数据源。
type Task struct {
	s      *Scheduler
	source string
	cb     Callback

	state     State
	attempts  int       // 已失败次数
	notBefore time.Time // 退避到期时间
	done      chan struct{}
}

// Cancel 取消任务；取消成功返回 true，已终结时返回 false。
// 已获准执行的回调继续运行并占名额，但其结果被丢弃。
func (t *Task) Cancel() bool {
	t.s.mu.Lock()
	defer t.s.mu.Unlock()
	if t.state.terminal() {
		return false
	}
	t.state = StateCancelled
	close(t.done)
	return true
}

// Wait 阻塞直到任务进入终态并返回该终态。
func (t *Task) Wait() State {
	<-t.done
	return t.State()
}

// State 返回任务当前状态。
func (t *Task) State() State {
	t.s.mu.Lock()
	defer t.s.mu.Unlock()
	return t.state
}

// Scheduler 采集调度器。
type Scheduler struct {
	mu       sync.Mutex
	clock    Clock
	queue    []*Task // 按最初受理顺序
	sources  map[string]*sourceState
	inflight int // 全局在途数

	wakeAt        time.Time // 已调度的唤醒时刻
	wakeScheduled bool
}

// New 创建使用真实时钟的调度器。
func New() *Scheduler { return NewWithClock(realClock{}) }

// NewWithClock 创建使用指定时钟的调度器（测试用）。
func NewWithClock(c Clock) *Scheduler {
	return &Scheduler{clock: c, sources: make(map[string]*sourceState)}
}

// Submit 提交任务，返回任务句柄。
func (s *Scheduler) Submit(source string, cb Callback) *Task {
	t := &Task{s: s, source: source, cb: cb, state: StatePending, done: make(chan struct{})}
	s.mu.Lock()
	s.queue = append(s.queue, t)
	s.dispatchLocked()
	s.mu.Unlock()
	return t
}

func (s *Scheduler) src(name string) *sourceState {
	st, ok := s.sources[name]
	if !ok {
		st = &sourceState{}
		s.sources[name] = st
	}
	return st
}

// degradedLocked 判断源当前是否降级：窗口内失败占比严格大于 15%。
func (s *Scheduler) degradedLocked(st *sourceState) bool {
	total := len(st.window)
	if total == 0 {
		return false
	}
	failures := 0
	for _, sm := range st.window {
		if sm.failed {
			failures++
		}
	}
	return failures*20 > total*3
}

// dispatchLocked 准入所有当前合资格的任务，并安排下一次唤醒。
func (s *Scheduler) dispatchLocked() {
	now := s.clock.Now()

	// 修剪各源窗口：保留 at > now-300s（含当前时刻，不含恰好 300 秒前）。
	cutoff := now.Add(-window)
	for _, st := range s.sources {
		i := 0
		for i < len(st.window) && !st.window[i].at.After(cutoff) {
			i++
		}
		st.window = st.window[i:]
	}

	// 按受理顺序扫描，被挡住的任务不影响后面的合资格任务。
	for _, t := range s.queue {
		if s.inflight >= globalLimit {
			break
		}
		if t.state != StatePending || now.Before(t.notBefore) {
			continue
		}
		st := s.src(t.source)
		limit := normalLimit
		if s.degradedLocked(st) {
			limit = degradedLimit
		}
		if st.inflight >= limit {
			continue
		}
		st.inflight++
		s.inflight++
		t.state = StateRunning
		go s.runAttempt(t)
	}

	s.scheduleWakeLocked(now)
}

// scheduleWakeLocked 为最近的一次未来事件（退避到期或窗口样本过期）安排唤醒。
func (s *Scheduler) scheduleWakeLocked(now time.Time) {
	var next time.Time
	consider := func(ts time.Time) {
		if ts.After(now) && (next.IsZero() || ts.Before(next)) {
			next = ts
		}
	}
	for _, t := range s.queue {
		if t.state == StatePending {
			consider(t.notBefore)
		}
	}
	for _, st := range s.sources {
		if len(st.window) > 0 {
			consider(st.window[0].at.Add(window))
		}
	}
	if next.IsZero() {
		return
	}
	if s.wakeScheduled && !s.wakeAt.After(next) {
		return // 已有不晚于 next 的唤醒
	}
	s.wakeScheduled = true
	s.wakeAt = next
	s.clock.AfterFunc(next.Sub(now), func() {
		s.mu.Lock()
		s.wakeScheduled = false
		s.wakeAt = time.Time{}
		s.dispatchLocked()
		s.mu.Unlock()
	})
}

// runAttempt 执行一次回调，返回后释放名额并更新状态与窗口。
func (s *Scheduler) runAttempt(t *Task) {
	ok := t.cb()

	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.src(t.source)
	st.inflight--
	s.inflight--

	if t.state == StateCancelled {
		// 取消后的晚到结果：不进窗口、不改终态。
		s.dispatchLocked()
		return
	}

	st.window = append(st.window, sample{at: s.clock.Now(), failed: !ok})

	switch {
	case ok:
		t.state = StateSucceeded
		close(t.done)
	default:
		t.attempts++
		if t.attempts >= maxAttempts {
			t.state = StateFailed
			close(t.done)
		} else {
			t.state = StatePending
			t.notBefore = s.clock.Now().Add(backoffs[t.attempts-1])
		}
	}
	s.dispatchLocked()
}
