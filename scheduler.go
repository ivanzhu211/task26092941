package collector

import (
	"sync"
)

// globalLimit 全库最大同时执行次数。
const globalLimit = 8

// Scheduler 进程内采集调度器，状态仅保存在内存。
type Scheduler struct {
	clk Clock

	mu      sync.Mutex
	running int // 全局在途尝试数
	seq     uint64
	sources map[string]*sourceState
	queue   []*Task // 待准入任务，按 seq 升序
}

// New 创建调度器。clk 为 nil 时使用真实时钟。
func New(clk Clock) *Scheduler {
	if clk == nil {
		clk = realClock{}
	}
	return &Scheduler{clk: clk, sources: make(map[string]*sourceState)}
}

// Submit 提交一个属于 sourceID 的采集任务。
func (s *Scheduler) Submit(sourceID string, fn CollectFunc) *Task {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seq++
	t := &Task{
		seq:      s.seq,
		sourceID: sourceID,
		fn:       fn,
		state:    StatePending,
		done:     make(chan struct{}),
	}
	s.queue = append(s.queue, t)
	s.tryAdmitLocked()
	return t
}

// Cancel 取消任务；取消成功即进入取消终态。已终结的任务返回 false。
func (s *Scheduler) Cancel(t *Task) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if t.state.terminal() {
		return false
	}
	t.cancelled = true
	t.state = StateCancelled
	close(t.done)
	// 不在队列中说明在途：回调继续执行，返回前仍占名额。
	t.removeFromQueueLocked(s)
	return true
}

// Wait 阻塞等待任务到达终态并返回终态。
func (s *Scheduler) Wait(t *Task) State {
	<-t.done
	return t.state
}

// sourceLocked 取或建数据源状态。
func (s *Scheduler) sourceLocked(id string) *sourceState {
	ss, ok := s.sources[id]
	if !ok {
		ss = &sourceState{}
		s.sources[id] = ss
	}
	return ss
}

// removeFromQueueLocked 将任务移出等待队列，返回是否原本在队列中。
func (t *Task) removeFromQueueLocked(s *Scheduler) bool {
	for i, q := range s.queue {
		if q == t {
			s.queue = append(s.queue[:i], s.queue[i+1:]...)
			return true
		}
	}
	return false
}

// tryAdmitLocked 扫描等待队列，准入所有当前合资格的任务。
// 被退避或源并行度挡住的任务不阻塞后续任务。
func (s *Scheduler) tryAdmitLocked() {
	if s.running >= globalLimit {
		return
	}
	now := s.clk.Now()
	rest := s.queue[:0]
	for _, t := range s.queue {
		if s.running < globalLimit && t.state == StatePending &&
			!t.readyAt.After(now) && s.sourceLocked(t.sourceID).canAdmit() {
			s.admitLocked(t)
			continue
		}
		rest = append(rest, t)
	}
	s.queue = rest
}

// admitLocked 占用名额并启动一次尝试。
func (s *Scheduler) admitLocked(t *Task) {
	ss := s.sourceLocked(t.sourceID)
	s.running++
	ss.inflight++
	t.state = StateRunning
	t.attempts++
	go s.run(t, ss)
}

// run 执行回调并处理结果。回调在锁外执行。
func (s *Scheduler) run(t *Task, ss *sourceState) {
	ok := t.fn()
	s.mu.Lock()
	defer s.mu.Unlock()

	s.running--
	ss.inflight--

	if t.cancelled {
		// 已取消：结果不进入错误窗口，也不能覆盖取消终态。
		s.tryAdmitLocked()
		return
	}

	now := s.clk.Now()
	ss.samples = append(ss.samples, sample{at: now, failed: !ok})
	ss.evaluate(now)
	// 样本自然过期时触发重新判断。
	s.clk.AfterFunc(window, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		ss.evaluate(s.clk.Now())
		s.tryAdmitLocked()
	})

	switch {
	case ok:
		t.state = StateSucceeded
		close(t.done)
	case t.attempts >= maxAttempts:
		t.state = StateFailed
		close(t.done)
	default:
		delay := retryDelays[t.attempts-1]
		t.state = StatePending
		t.readyAt = now.Add(delay)
		s.requeueLocked(t)
		s.clk.AfterFunc(delay, func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			s.tryAdmitLocked()
		})
	}
	s.tryAdmitLocked()
}

// requeueLocked 将重试任务按原 seq 顺序插回等待队列。
func (s *Scheduler) requeueLocked(t *Task) {
	i := len(s.queue)
	for i > 0 && s.queue[i-1].seq > t.seq {
		i--
	}
	s.queue = append(s.queue, nil)
	copy(s.queue[i+1:], s.queue[i:])
	s.queue[i] = t
}
