package main

import (
	"context"
	"fmt"
	"time"

	"github.com/lufred/goworker/config"

	workers "github.com/lufred/goworker"
)

func main() {
	w := workers.NewWorker(
		context.Background(),
		config.DefaultWorkerConfig(),
		&MyWorker{})

	if err := w.Run(); err != nil {
		fmt.Println(err)
		return
	}
	if err := w.Wait(); err != nil {
		fmt.Println(err.Error())
	}
	time.Sleep(1 * time.Second)
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
	fmt.Printf("%s\n", err.Error())
}
