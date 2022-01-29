package main

import (
	"context"
	"fmt"
	"os"

	workers "github.com/lufred/goworker"
	"github.com/lufred/goworker/config"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func main() {
	atom := zap.NewAtomicLevel()
	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(zap.NewDevelopmentEncoderConfig()),
		os.Stdout,
		atom,
	)
	atom.SetLevel(zap.DebugLevel)
	log := zap.New(core)

	cfg := config.WorkerConfig{
		Logger: log,
	}

	w := workers.NewWorker(
		context.Background(),
		cfg,
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
	fmt.Println("helloWorld")
	return nil
}
func (w *MyWorker) After(ctx context.Context) error {
	fmt.Println("After")
	return nil
}
func (w *MyWorker) ErrorMessage(ctx context.Context, err error) {
	fmt.Println(fmt.Sprintf("err:%+v", err))
}
