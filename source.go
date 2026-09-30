package collector

import "time"

// window 错误统计窗口时长。
const window = 300 * time.Second

// 并行度上限。
const (
	normalLimit   = 4
	degradedLimit = 1
)

// sample 一次已完成尝试的记录。
type sample struct {
	at     time.Time
	failed bool
}

// sourceState 单个数据源的共享状态：并行度与错误统计。
type sourceState struct {
	samples  []sample // 窗口内的完成记录，按时间升序
	inflight int      // 在途尝试数
	degraded bool
}

// prune 丢弃不晚于 now-window 的样本。
// 窗口包含当前时刻的完成，不包含恰好 300 秒前的完成。
func (ss *sourceState) prune(now time.Time) {
	cutoff := now.Add(-window)
	i := 0
	for i < len(ss.samples) && !ss.samples[i].at.After(cutoff) {
		i++
	}
	ss.samples = append(ss.samples[:0:0], ss.samples[i:]...)
}

// evaluate 依据当前窗口重新判断降级状态。
// 失败率严格大于 15% 时降级；无样本时正常。
func (ss *sourceState) evaluate(now time.Time) {
	ss.prune(now)
	total := len(ss.samples)
	if total == 0 {
		ss.degraded = false
		return
	}
	failed := 0
	for _, sm := range ss.samples {
		if sm.failed {
			failed++
		}
	}
	// failed/total > 0.15  <=>  failed*100 > total*15
	ss.degraded = failed*100 > total*15
}

// limit 当前并行度上限。
func (ss *sourceState) limit() int {
	if ss.degraded {
		return degradedLimit
	}
	return normalLimit
}

// canAdmit 是否还能接纳新尝试。
// 降级时上限为 1，自然要求所有在途返回后才能开始下一次。
func (ss *sourceState) canAdmit() bool {
	return ss.inflight < ss.limit()
}
