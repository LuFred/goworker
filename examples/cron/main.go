package main

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/lufred/goworker/config"

	workers "github.com/lufred/goworker"
)

func main() {
	// 每分钟执行一次
	w := workers.NewWorker(
		context.Background(),
		config.DefaultWorkerConfig(),
		&MyWorker{},
		workers.WithCronSchedule("* * * * *"))
	if err := w.Run(); err != nil {
		fmt.Println(err)
		return
	}

	if err := w.Wait(); err != nil {
		fmt.Println(err.Error())
	}
}

type MyWorker struct {
	Num int
}

func (w *MyWorker) Before(ctx context.Context) error {
	rand.Seed(time.Now().UnixNano())
	w.Num = rand.Intn(100)
	fmt.Println("Before")
	return nil
}
func (w *MyWorker) Do(ctx context.Context) error {
	fmt.Println(fmt.Sprintf("helloWorld  %d", w.Num))
	return nil
}
func (w *MyWorker) After(ctx context.Context) error {
	fmt.Println("After")
	return nil
}
func (w *MyWorker) ErrorMessage(ctx context.Context, err error) {
	fmt.Println(fmt.Sprintf("err:%+v", err))
}
