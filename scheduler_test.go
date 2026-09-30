package collector

import (
	"testing"
	"time"
)

// gate 可控回调：启动时发信号，阻塞直到测试释放结果。
type gate struct {
	started chan struct{}
	release chan bool
}

func newGate() *gate {
	return &gate{started: make(chan struct{}, 1), release: make(chan bool, 1)}
}

func (g *gate) fn() bool {
	g.started <- struct{}{}
	return <-g.release
}

// signalFn 每次调用发信号并返回固定结果。
func signalFn(sig chan<- struct{}, result bool) CollectFunc {
	return func() bool {
		sig <- struct{}{}
		return result
	}
}

// stats 读取数据源内部统计（测试专用）。
func (s *Scheduler) stats(id string) (total, failed int, degraded bool, inflight int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ss := s.sources[id]
	if ss == nil {
		return 0, 0, false, 0
	}
	for _, sm := range ss.samples {
		if sm.failed {
			failed++
		}
	}
	return len(ss.samples), failed, ss.degraded, ss.inflight
}

func waitFor(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timeout waiting: %s", msg)
}

func waitStart(t *testing.T, g *gate, msg string) {
	t.Helper()
	select {
	case <-g.started:
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting start: %s", msg)
	}
}

func assertNotStarted(t *testing.T, g *gate, msg string) {
	t.Helper()
	select {
	case <-g.started:
		t.Fatalf("unexpected start: %s", msg)
	case <-time.After(50 * time.Millisecond):
	}
}

// env 测试环境：假时钟 + 调度器 + 辅助函数。
type env struct {
	t   *testing.T
	clk *fakeClock
	s   *Scheduler
}

func newEnv(t *testing.T) *env {
	clk := newFakeClock()
	return &env{t: t, clk: clk, s: New(clk)}
}

// succeed 提交一个立即成功的任务并等待终态。
func (e *env) succeed(src string) {
	tk := e.s.Submit(src, signalFn(make(chan struct{}, 1), true))
	if st := e.s.Wait(tk); st != StateSucceeded {
		e.t.Fatalf("expected Succeeded, got %v", st)
	}
}

// failOnce 提交一个失败一次的任务，记录样本后取消（避免重试干扰）。
func (e *env) failOnce(src string, wantTotal int) {
	sig := make(chan struct{}, 1)
	tk := e.s.Submit(src, signalFn(sig, false))
	<-sig
	waitFor(e.t, func() bool {
		total, _, _, _ := e.s.stats(src)
		return total == wantTotal
	}, "failure sample recorded")
	if !e.s.Cancel(tk) {
		e.t.Fatalf("cancel of backing-off task should succeed")
	}
	if st := e.s.Wait(tk); st != StateCancelled {
		e.t.Fatalf("expected Cancelled, got %v", st)
	}
}

// TestDegradeThreshold 20 次完成中 3 次失败正常、4 次失败降级。
func TestDegradeThreshold(t *testing.T) {
	e := newEnv(t)

	// 17 成功 + 3 失败 = 3/20 = 15%，不严格大于 15%，正常）。
	for i := 0; i < 17; i++ {
		e.succeed("A")
	}
	for i := 0; i < 3; i++ {
		e.failOnce("A", 18+i)
	}
	total, failed, degraded, _ := e.s.stats("A")
	if total != 20 || failed != 3 || degraded {
		t.Fatalf("3/20 should be normal, got total=%d failed=%d degraded=%v", total, failed, degraded)
	}

	// 正常时同源可四路并行。
	gs := make([]*gate, 4)
	for i := range gs {
		gs[i] = newGate()
		e.s.Submit("A", gs[i].fn)
	}
	for i, g := range gs {
		waitStart(t, g, "normal source admits 4 concurrent")
		gs[i].release <- true
	}
	waitFor(t, func() bool {
		total, _, _, _ := e.s.stats("A")
		return total == 24
	}, "gate completions recorded")

	// 清空窗口。
	e.clk.Advance(301 * time.Second)

	// 16 成功 + 4 失败 = 4/20 = 20%，降级。
	for i := 0; i < 16; i++ {
		e.succeed("A")
	}
	for i := 0; i < 4; i++ {
		e.failOnce("A", 17+i)
	}
	_, _, degraded, _ = e.s.stats("A")
	if !degraded {
		t.Fatalf("4/20 should be degraded")
	}

	// 降级时同源最多 1 路。
	g1, g2 := newGate(), newGate()
	e.s.Submit("A", g1.fn)
	waitStart(t, g1, "degraded source admits 1")
	e.s.Submit("A", g2.fn)
	assertNotStarted(t, g2, "degraded source must not admit 2nd")
	g1.release <- true
	waitStart(t, g2, "slot freed after inflight returns")
	g2.release <- true
}

// TestExpiryRestoresParallelism 失败样本恰好过期时，无新完成也恢复四路。
func TestExpiryRestoresParallelism(t *testing.T) {
	e := newEnv(t)

	// 1/1 = 100%，立即降级。
	e.failOnce("A", 1)
	_, _, degraded, _ := e.s.stats("A")
	if !degraded {
		t.Fatalf("1/1 should be degraded")
	}

	gs := make([]*gate, 4)
	for i := range gs {
		gs[i] = newGate()
		e.s.Submit("A", gs[i].fn)
	}
	waitStart(t, gs[0], "degraded admits 1")
	for i := 1; i < 4; i++ {
		assertNotStarted(t, gs[i], "degraded source admits at most 1")
	}

	// 恰好推进 300 秒：样本不在窗口内，应恢复四路，无需新完成。
	e.clk.Advance(300 * time.Second)
	for i := 1; i < 4; i++ {
		waitStart(t, gs[i], "expiry restores 4-way parallelism")
	}
	for _, g := range gs {
		g.release <- true
	}
}

// TestDegradeWithInflightAndCancel 四路在途时降级，取消在途任务：
// 返回前不占新名额、晚到结果不改终态不污染窗口，健康来源继续执行。
func TestDegradeWithInflightAndCancel(t *testing.T) {
	e := newEnv(t)

	// 预置 17 成功 + 3 失败（15%，正常）。
	for i := 0; i < 17; i++ {
		e.succeed("A")
	}
	for i := 0; i < 3; i++ {
		e.failOnce("A", 18+i)
	}

	// 四路在途。
	gs := make([]*gate, 4)
	tks := make([]*Task, 4)
	for i := range gs {
		gs[i] = newGate()
		tks[i] = e.s.Submit("A", gs[i].fn)
		waitStart(t, gs[i], "4-way inflight")
	}

	// g0 失败：4/21 ≈ 19% > 15%，触发降级；其余三路仍在途。
	gs[0].release <- false
	waitFor(t, func() bool {
		_, _, degraded, _ := e.s.stats("A")
		return degraded
	}, "degraded by 4/21")

	// 降级且有多路在途：新任务不能准入。
	g5 := newGate()
	e.s.Submit("A", g5.fn)
	assertNotStarted(t, g5, "degraded with inflight must not admit")

	// 取消两个在途任务：立即进入取消终态，但名额仍占用。
	if !e.s.Cancel(tks[1]) || !e.s.Cancel(tks[2]) {
		t.Fatalf("cancel inflight should succeed")
	}
	if st := e.s.Wait(tks[1]); st != StateCancelled {
		t.Fatalf("expected Cancelled, got %v", st)
	}
	if st := e.s.Wait(tks[2]); st != StateCancelled {
		t.Fatalf("expected Cancelled, got %v", st)
	}
	g6 := newGate()
	e.s.Submit("A", g6.fn)
	assertNotStarted(t, g5, "cancelled inflight still holds slot (g5)")
	assertNotStarted(t, g6, "cancelled inflight still holds slot (g6)")

	// 健康来源照常执行。
	h := newGate()
	htk := e.s.Submit("B", h.fn)
	waitStart(t, h, "healthy source keeps running")

	// 晚到的成功结果被丢弃：不污染窗口、名额释放但仍不够准入。
	gs[1].release <- true
	waitFor(t, func() bool {
		_, _, _, inflight := e.s.stats("A")
		return inflight == 2
	}, "cancelled callback releases slot")
	total, failed, _, _ := e.s.stats("A")
	if total != 21 || failed != 4 {
		t.Fatalf("late success must not enter window, got total=%d failed=%d", total, failed)
	}
	assertNotStarted(t, g5, "still 2 inflight, no admit")

	// 晚到的失败结果同样被丢弃。
	gs[2].release <- false
	waitFor(t, func() bool {
		_, _, _, inflight := e.s.stats("A")
		return inflight == 1
	}, "second cancelled callback releases slot")
	total, failed, _, _ = e.s.stats("A")
	if total != 21 || failed != 4 {
		t.Fatalf("late failure must not enter window, got total=%d failed=%d", total, failed)
	}
	assertNotStarted(t, g5, "still 1 inflight, no admit")

	// 最后一路返回（未取消，计入窗口）：4/22 ≈ 18% 仍降级，
	// 在途清零后按顺序准入 g5。
	gs[3].release <- true
	waitStart(t, g5, "admit after all inflight returned")
	total, failed, degraded, _ := e.s.stats("A")
	if total != 22 || failed != 4 || !degraded {
		t.Fatalf("got total=%d failed=%d degraded=%v", total, failed, degraded)
	}
	assertNotStarted(t, g6, "degraded limit 1: g6 waits")

	// 取消任务的终态不被晚到结果覆盖（已断言），再次取消无效。
	if e.s.Cancel(tks[1]) {
		t.Fatalf("cancel of terminal task should fail")
	}

	// 收尾。
	if !e.s.Cancel(tks[0]) {
		t.Fatalf("cancel backing-off task should succeed")
	}
	g5.release <- true
	waitStart(t, g6, "g6 admitted after g5 completes")
	g6.release <- true
	h.release <- true
	if st := e.s.Wait(htk); st != StateSucceeded {
		t.Fatalf("expected Succeeded, got %v", st)
	}
}

// TestRetryBackoff 失败退避 60/120/240/480 秒，第 5 次失败进入失败终态。
func TestRetryBackoff(t *testing.T) {
	e := newEnv(t)

	calls := make(chan struct{}, 10)
	tk := e.s.Submit("A", signalFn(calls, false))

	delays := []time.Duration{60, 120, 240, 480}
	<-calls // 第 1 次立即执行
	for i, d := range delays {
		// 等回调返回后的记账完成（重试定时器已布置），再推进时钟。
		waitFor(t, func() bool {
			e.s.mu.Lock()
			defer e.s.mu.Unlock()
			return tk.state == StatePending
		}, "retry scheduled")
		e.clk.Advance(d*time.Second - time.Second)
		select {
		case <-calls:
			t.Fatalf("attempt %d started before backoff elapsed", i+2)
		case <-time.After(20 * time.Millisecond):
		}
		e.clk.Advance(time.Second)
		select {
		case <-calls:
		case <-time.After(2 * time.Second):
			t.Fatalf("attempt %d not started after %ds backoff", i+2, d)
		}
	}
	if st := e.s.Wait(tk); st != StateFailed {
		t.Fatalf("5th failure should be Failed, got %v", st)
	}
	select {
	case <-calls:
		t.Fatalf("no 6th attempt allowed")
	default:
	}
}

// TestGlobalLimitAndOrder 全局最多 8 路，同源最多 4 路，按受理顺序准入。
func TestGlobalLimitAndOrder(t *testing.T) {
	e := newEnv(t)

	var gates []*gate
	start := func(src string) *gate {
		g := newGate()
		e.s.Submit(src, g.fn)
		gates = append(gates, g)
		return g
	}
	// A、B 各 4 路，占满全局 8 路。
	for i := 0; i < 4; i++ {
		waitStart(t, start("A"), "A fills 4")
	}
	for i := 0; i < 4; i++ {
		waitStart(t, start("B"), "B fills 4")
	}
	a5 := start("A")
	b5 := start("B")
	assertNotStarted(t, a5, "global limit 8 reached")
	assertNotStarted(t, b5, "global limit 8 reached")

	// 释放一路 A：按受理顺序 a5 先准入。
	gates[0].release <- true
	waitStart(t, a5, "a5 admitted in submission order")
	assertNotStarted(t, b5, "global full again")

	gates[4].release <- true
	waitStart(t, b5, "b5 admitted after slot frees")

	for _, g := range gates {
		select {
		case g.release <- true:
		default:
		}
	}
}
