package sched

import (
	"testing"
	"time"
)

// gate 可控回调：started 关闭表示回调已开始，release 关闭后回调返回 result。
type gate struct {
	started chan struct{}
	release chan struct{}
	result  bool
}

func newGate(result bool) *gate {
	return &gate{started: make(chan struct{}), release: make(chan struct{}), result: result}
}

func (g *gate) cb() bool {
	close(g.started)
	<-g.release
	return g.result
}

func waitStarted(t *testing.T, g *gate) {
	t.Helper()
	select {
	case <-g.started:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for gate to start")
	}
}

func assertNotStarted(t *testing.T, g *gate) {
	t.Helper()
	select {
	case <-g.started:
		t.Fatal("gate started unexpectedly")
	case <-time.After(50 * time.Millisecond):
	}
}

func windowLen(s *Scheduler, src string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.src(src).window)
}

func waitWindowLen(t *testing.T, s *Scheduler, src string, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if windowLen(s, src) == n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("window len for %q did not reach %d", src, n)
}

// buildWindow 让 src 产生 total 次完成，其中 failures 次失败；
// 失败任务在首次失败后被取消（样本保留，不再重试）。
func buildWindow(t *testing.T, s *Scheduler, src string, total, failures int) {
	t.Helper()
	var failing []*Task
	for i := 0; i < total; i++ {
		if i < failures {
			tk := s.Submit(src, func() bool { return false })
			failing = append(failing, tk)
		} else {
			s.Submit(src, func() bool { return true })
		}
	}
	waitWindowLen(t, s, src, total)
	for _, tk := range failing {
		if !tk.Cancel() {
			t.Fatal("cancel of failing task failed")
		}
	}
}

// 场景一·a：20 次完成中 3 次失败（15%，不严格大于）保持正常，四路并行。
func TestWindowThreeFailuresStaysNormal(t *testing.T) {
	clk := newFakeClock()
	s := NewWithClock(clk)
	buildWindow(t, s, "A", 20, 3)

	gates := make([]*gate, 4)
	for i := range gates {
		gates[i] = newGate(true)
		s.Submit("A", gates[i].cb)
	}
	for _, g := range gates {
		waitStarted(t, g) // 正常：四路全部获准
	}
	for _, g := range gates {
		close(g.release)
	}
}

// 场景一·b：20 次完成中 4 次失败（20%）降级为一路；
// 失败样本恰好过期时，即使没有新的完成，也恢复四路。
func TestWindowFourFailuresDegradesAndExpiryRecovers(t *testing.T) {
	clk := newFakeClock()
	s := NewWithClock(clk)
	buildWindow(t, s, "A", 20, 4)

	gates := make([]*gate, 4)
	tasks := make([]*Task, 4)
	for i := range gates {
		gates[i] = newGate(true)
		tasks[i] = s.Submit("A", gates[i].cb)
	}
	waitStarted(t, gates[0]) // 降级：只有一路获准
	for _, g := range gates[1:] {
		assertNotStarted(t, g)
	}

	clk.Advance(299 * time.Second) // 样本尚未过期，仍降级
	for _, g := range gates[1:] {
		assertNotStarted(t, g)
	}

	clk.Advance(1 * time.Second) // 恰好 300 秒，样本全部过期
	for _, g := range gates[1:] {
		waitStarted(t, g) // 无新完成也恢复四路
	}
	for _, g := range gates {
		close(g.release)
	}
	for _, tk := range tasks {
		if got := tk.Wait(); got != StateSucceeded {
			t.Fatalf("task state = %v, want StateSucceeded", got)
		}
	}
}

// 场景二：四路在途时触发降级，取消在途任务；回调返回前不误补新任务，
// 晚到结果不改终态、不污染窗口，健康来源继续执行。
func TestDegradeWithInflightCancelAndHealthySource(t *testing.T) {
	clk := newFakeClock()
	s := NewWithClock(clk)

	// 预置窗口：18 次完成、2 次失败（11.1%，正常）。
	buildWindow(t, s, "A", 18, 2)

	// 四路在途。
	gates := make([]*gate, 4)
	tasks := make([]*Task, 4)
	for i := range gates {
		gates[i] = newGate(false)
		tasks[i] = s.Submit("A", gates[i].cb)
	}
	for _, g := range gates {
		waitStarted(t, g)
	}

	// 第 1 路返回失败：窗口 19 次完成、3 次失败（15.8%）触发降级。
	close(gates[0].release)
	waitWindowLen(t, s, "A", 19)

	// 降级后新任务不能准入（在途 3 >= 降级上限 1）。
	g5 := newGate(true)
	t5 := s.Submit("A", g5.cb)
	assertNotStarted(t, g5)

	// 取消：1 个等待重试的任务 + 3 个在途任务。
	if !tasks[0].Cancel() {
		t.Fatal("cancel pending-retry task failed")
	}
	for _, tk := range tasks[1:] {
		if !tk.Cancel() {
			t.Fatal("cancel running task failed")
		}
	}

	// 健康来源 B 不受 A 降级影响，继续执行。
	gB := newGate(true)
	tB := s.Submit("B", gB.cb)
	waitStarted(t, gB)
	close(gB.release)
	if got := tB.Wait(); got != StateSucceeded {
		t.Fatalf("healthy source task state = %v, want StateSucceeded", got)
	}

	// 已取消的回调返回前仍占名额：不能误补新任务。
	assertNotStarted(t, g5)
	close(gates[1].release)
	close(gates[2].release)
	assertNotStarted(t, g5) // 仍有 1 路在途，必须等全部返回

	close(gates[3].release) // 全部返回后才准入下一次尝试
	waitStarted(t, g5)

	// 晚到结果不污染错误窗口：仍是 19 次完成、3 次失败。
	if n := windowLen(s, "A"); n != 19 {
		t.Fatalf("window len = %d, want 19 (late results must be discarded)", n)
	}

	// 晚到结果不覆盖取消终态。
	for _, tk := range tasks {
		if got := tk.Wait(); got != StateCancelled {
			t.Fatalf("cancelled task state = %v, want StateCancelled", got)
		}
	}

	close(g5.release)
	if got := t5.Wait(); got != StateSucceeded {
		t.Fatalf("t5 state = %v, want StateSucceeded", got)
	}
}
