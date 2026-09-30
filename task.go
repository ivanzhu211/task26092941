package collector

import "time"

// State 任务状态；Succeeded/Failed/Cancelled 为终态。
type State int

const (
	StatePending State = iota
	StateRunning
	StateSucceeded
	StateFailed
	StateCancelled
)

// terminal 报告状态是否为终态。
func (st State) terminal() bool {
	return st == StateSucceeded || st == StateFailed || st == StateCancelled
}

// CollectFunc 采集回调，返回 true 表示成功，false 表示失败。
type CollectFunc func() bool

// maxAttempts 每个任务最多执行次数。
const maxAttempts = 5

// retryDelays 第 1~4 次失败后的退避等待秒数。
var retryDelays = [maxAttempts - 1]time.Duration{
	60 * time.Second,
	120 * time.Second,
	240 * time.Second,
	480 * time.Second,
}

// Task 任务句柄。字段仅由调度器在持锁状态下访问。
type Task struct {
	seq       uint64 // 受理顺序号，决定准入优先级
	sourceID  string
	fn        CollectFunc
	state     State
	attempts  int       // 已执行的尝试次数
	readyAt   time.Time // 退避结束时刻；零值表示立即可准入
	cancelled bool      // 已取消（含在途取消）
	done      chan struct{}
}
