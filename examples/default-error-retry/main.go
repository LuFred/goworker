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
	// 默认的重试规则是:
	// 首次重试间隔：1s
	// 后续重试间隔：前一次重试间隔 * 2
	// 最大重试间隔：10s
	// 间隔列表：1-2-4-8-10-10-10-♾️

	w := workers.NewWorker(
		context.Background(),
		config.DefaultWorkerConfig(),
		&MyWorker{})
	if err := w.Run(); err != nil {
		fmt.Println(err)
		return
	}

	if err := w.Wait(); err != nil {
		fmt.Println(fmt.Sprintf("end  =  %s", err.Error()))
	}
}

type MyWorker struct {
}

func (w *MyWorker) Before(ctx context.Context) error {
	fmt.Println("Before")
	return nil
}

func (w *MyWorker) Do(ctx context.Context) error {
	return fmt.Errorf("unknown error")
}

func (w *MyWorker) After(ctx context.Context) error {
	fmt.Println("After")
	return nil
}

func (w *MyWorker) ErrorMessage(ctx context.Context, err error) {
	t := time.Now()
	fmt.Println(fmt.Sprintf("err:%+v [%v:%v:%v]", err, t.Hour(), t.Minute(), t.Second()))
}
