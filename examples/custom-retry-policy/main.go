package main

import (
	"context"
	"fmt"
	"time"

	"github.com/lufred/goworker/config"

	workers "github.com/lufred/goworker"
)

func main() {
	// 【Before、Do、After】任何一个出错都会进入重试机制
	// 自定义出错重试间隔:错误小于5次 每次间隔2秒，超过5次不再继续重试
	w := workers.NewWorker(
		context.Background(),
		config.DefaultWorkerConfig(),
		&MyWorker{},
		workers.WithRetryPolicy(MyRetryPolicy{}))

	if err := w.Run(); err != nil {
		fmt.Println(err)
		return
	}

	if err := w.Wait(); err != nil {
		fmt.Println(fmt.Sprintf("end  =  %s", err.Error()))
	}
}

type MyRetryPolicy struct {
}

func (p MyRetryPolicy) ComputeNextDelay(numAttempts int) time.Duration {
	if numAttempts < 5 {
		return time.Second * 2
	}

	return workers.RetryDone
}

type MyWorker struct {
}

func (w *MyWorker) Before(ctx context.Context) error {
	fmt.Println("before")
	return fmt.Errorf("unknown error")
}
func (w *MyWorker) Do(ctx context.Context) error {
	fmt.Println("Do")
	return nil
}
func (w *MyWorker) After(ctx context.Context) error {
	fmt.Println("After")
	return nil
}
func (w *MyWorker) ErrorMessage(ctx context.Context, err error) {
	t := time.Now()
	fmt.Println(fmt.Sprintf("err:%+v [%v:%v:%v]", err, t.Hour(), t.Minute(), t.Second()))
}
