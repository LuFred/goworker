package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/lufred/goworker/config"

	workers "github.com/lufred/goworker"
)

func main() {
	//  使用默认重试策略
	retryPolicy := workers.GetDefaultRetryPolicy()
	retryPolicy.SetInitialInterval(time.Second * 2)
	retryPolicy.SetMaximumInterval(time.Second * 20)
	retryPolicy.SetMaximumAttempts(3)

	w := workers.NewWorker(
		context.Background(),
		config.DefaultWorkerConfig(),
		&MyWorker{},
		workers.WithRetryPolicy(retryPolicy))
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
	return errors.New("before err")
}
func (w *MyWorker) Do(ctx context.Context) error {
	fmt.Println("helloWorld")
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
