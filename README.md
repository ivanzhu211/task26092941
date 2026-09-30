# collector

进程内采集调度库（纯内存状态，无外部依赖）。

## 用法

```go
s := collector.New(nil) // nil 使用真实时钟；测试可注入 collector.Clock

tk := s.Submit("source-id", func() bool {
    return doCollect() // true=成功, false=失败
})

go s.Cancel(tk)        // 取消：立即进入取消终态；在途回调继续但结果被丢弃
st := s.Wait(tk)       // 阻塞等待终态：Succeeded / Failed / Cancelled
```

## 语义

- 全库最多 8 路并发；同源正常最多 4 路，降级最多 1 路。名额从获准执行占用到回调返回。
- 每个源独立统计最近 300 秒完成的尝试（含当前时刻、不含恰好 300 秒前），失败率严格大于 15% 降级；样本过期自动触发重新判断。
- 失败后按 60/120/240/480 秒退避重试，最多 5 次尝试，第 5 次失败进入失败终态。
- 准入按最初受理顺序；被退避或源并行度挡住的任务不阻塞后续合资格任务。

## 测试

```sh
go test -race ./...
```
