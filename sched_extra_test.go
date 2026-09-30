package sched

import (
	"sync/atomic"
	"testing"
	"time"
)

// 重试退避：60/120/240/480 秒，从回调返回起算；等待期间不占名额；
// 第 5 次失败进入失败终态，每次重试都计入窗口。
func TestRetryBackoffAndMaxAttempts(t *testing.T) {
	clk := newFakeClock()
	s := NewWithClock(clk)

	var calls int32
	tk := s.Submit("A", func() bool {
		atomic.AddInt32(&calls, 1)
		return false
	})

	inflight := func() int {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.inflight
	}
	// 等待第 n 次尝试开始且其结果已完整记录（含退避时间），再推进时钟。
	waitSettled := func(n int32) {
		t.Helper()
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if atomic.LoadInt32(&calls) == n && inflight() == 0 {
				return
			}
			time.Sleep(time.Millisecond)
		}
		t.Fatalf("attempt %d did not settle", n)
	}

	waitSettled(1) // 第 1 次立即执行
	clk.Advance(59 * time.Second)
	if c := atomic.LoadInt32(&calls); c != 1 {
		t.Fatalf("calls = %d before first backoff elapsed, want 1", c)
	}
	if n := inflight(); n != 0 {
		t.Fatalf("inflight = %d during backoff, want 0 (retry wait holds no slot)", n)
	}

	clk.Advance(1 * time.Second) // 满 60 秒，第 2 次
	waitSettled(2)
	clk.Advance(120 * time.Second) // 第 3 次
	waitSettled(3)
	// 此刻 t=180，三次失败样本都在 300 秒窗口内。
	if n := windowLen(s, "A"); n != 3 {
		t.Fatalf("window len = %d at t=180s, want 3 (every attempt recorded)", n)
	}
	clk.Advance(240 * time.Second) // 第 4 次
	waitSettled(4)
	clk.Advance(479 * time.Second)
	if c := atomic.LoadInt32(&calls); c != 4 {
		t.Fatalf("calls = %d before last backoff elapsed, want 4", c)
	}
	clk.Advance(1 * time.Second) // 满 480 秒，第 5 次
	waitSettled(5)

	if got := tk.Wait(); got != StateFailed {
		t.Fatalf("task state = %v, want StateFailed", got)
	}
	if tk.Cancel() {
		t.Fatal("cancel of terminal task should fail")
	}
}

// 全局最多 8 路；被全局名额或退避挡住的任务不堵住后面的合资格任务；
// 有空闲名额时按最初受理顺序准入。
func TestGlobalLimitAndAdmissionOrder(t *testing.T) {
	clk := newFakeClock()
	s := NewWithClock(clk)

	mkGates := func(src string, n int) ([]*gate, []*Task) {
		gates := make([]*gate, n)
		tasks := make([]*Task, n)
		for i := range gates {
			gates[i] = newGate(true)
			tasks[i] = s.Submit(src, gates[i].cb)
		}
		return gates, tasks
	}

	gatesA, _ := mkGates("A", 4) // A 四路
	gatesB, _ := mkGates("B", 4) // B 四路，全局满 8
	for _, g := range gatesA {
		waitStarted(t, g)
	}
	for _, g := range gatesB {
		waitStarted(t, g)
	}

	gC := newGate(true)
	tC := s.Submit("C", gC.cb)
	assertNotStarted(t, gC) // 全局名额已满，C 被挡住

	close(gatesA[0].release) // 释放一个全局名额
	waitStarted(t, gC)       // 按受理顺序，C 补位
	close(gC.release)
	if got := tC.Wait(); got != StateSucceeded {
		t.Fatalf("tC state = %v, want StateSucceeded", got)
	}

	// 同一源内：退避中的任务不堵住后面合资格的任务。
	var d1calls int32
	d1retry := newGate(true)
	d1 := s.Submit("D", func() bool {
		if atomic.AddInt32(&d1calls, 1) == 1 {
			return false // 首次失败，进入 60 秒退避
		}
		return d1retry.cb()
	})
	waitWindowLen(t, s, "D", 1) // d1 第 1 次失败已记录

	d2 := newGate(true)
	tD2 := s.Submit("D", d2.cb)
	waitStarted(t, d2) // d1 在退避，d2 立即获准
	if c := atomic.LoadInt32(&d1calls); c != 1 {
		t.Fatalf("d1 calls = %d, want 1 (backoff must not fire early)", c)
	}

	// d2 成功返回后释放名额（D 源 1/2 失败仍降级，但降级上限 1 可容纳）。
	close(d2.release)
	if got := tD2.Wait(); got != StateSucceeded {
		t.Fatalf("tD2 state = %v, want StateSucceeded", got)
	}

	clk.Advance(60 * time.Second) // 退避到期，d1 重试
	waitStarted(t, d1retry)
	close(d1retry.release)
	if got := d1.Wait(); got != StateSucceeded {
		t.Fatalf("d1 state = %v, want StateSucceeded", got)
	}

	for _, g := range gatesA[1:] {
		close(g.release)
	}
	for _, g := range gatesB {
		close(g.release)
	}
}
