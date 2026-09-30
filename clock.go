package sched

import "time"

// Clock 抽象时间源，便于测试注入假时钟。
type Clock interface {
	Now() time.Time
	// AfterFunc 在 d 之后触发 f（用于退避到期与窗口过期唤醒）。
	AfterFunc(d time.Duration, f func())
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

func (realClock) AfterFunc(d time.Duration, f func()) { time.AfterFunc(d, f) }
