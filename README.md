# worker
worker是一个后台常驻服务库，就像创建一个操作系统服务。  

# 适用场景：
* 长时间后台处理任务
* mq数据消费服务
* 定时计划
* 异步任务处理

# 待支持
* 脚本任务（linux脚本）
* 失效转移（当某个实例在执行任务中宕机时，将重新由其它实例执行）
* 支持后台动态添加任务,支持Cron任务暂停,支持手动停止正在执行的任务(有条件)
* 任务依赖，多任务关联执行
* 监控

# 支持
* 服务集群,任务分片
* cron
* 出错重试
* 自定义重试策略
* 自定义logger配置

# [Example](https://github.com/LuFred/goworker/tree/master/examples)

# Getting Started

1. 定义worker类
实现接口：Handler
```go
type Handler interface {
    Before(ctx context.Context) error
    Do(ctx context.Context) error
    After(ctx context.Context) error
    ErrorMessage(ctx context.Context, err error)
}
```

执行顺序：  
Before -> Do -> After  
ErrorMessage贯穿handler的执行全周期，负责接收执行期间出现的error

2. 开始使用
例子
```go
package main

import (
	"context"
	"fmt"

	"github.com/lufred/goworker"
)

func main() {
	w := workers.NewWorker(
		context.Background(),
		workers.DefaultWorkerConfig(),
		&MyWorker{})
	
	if err := w.Run(); err != nil {
		fmt.Println(err)
		return
	}
	if err := w.Wait(); err != nil {
		fmt.Println(err.Error())
	}
}

type MyWorker struct {
}

func (w *MyWorker) Before(ctx context.Context) error {
	fmt.Println("任务初始化")
	return nil
}

func (w *MyWorker) Do(ctx context.Context) error {
	fmt.Println("任务执行中：helloWorld")
	return nil
}

func (w *MyWorker) After(ctx context.Context) error {
	fmt.Println("任务处理完成")
	return nil
}

func (w *MyWorker) ErrorMessage(ctx context.Context, err error) {
	fmt.Println(fmt.Sprintf("err:%+v", err))
}

```
