package goworker

import (
	"context"
	"fmt"
	"testing"

	"github.com/lufred/goworker/common"
	"github.com/lufred/goworker/config"
)

type HandlerOne struct{}

func (h *HandlerOne) Before(ctx context.Context) error {
	fmt.Println("before")
	return nil
}

func (h *HandlerOne) Do(ctx context.Context) error {
	fmt.Println("do")
	return nil
}

func (h *HandlerOne) After(ctx context.Context) error {
	fmt.Println("after")
	return nil
}

func (h *HandlerOne) ErrorMessage(ctx context.Context, err error) {
	fmt.Printf("err:%+v\n", err)
}

type HandlerTwo struct {
	ID int
}

func (h HandlerTwo) Before(ctx context.Context) error {
	fmt.Println("before")
	return nil
}

func (h HandlerTwo) Do(ctx context.Context) error {
	fmt.Println("do")
	return nil
}

func (h HandlerTwo) After(ctx context.Context) error {
	fmt.Println("after")
	return nil
}

func (h HandlerTwo) ErrorMessage(ctx context.Context, err error) {
	fmt.Printf("err:%+v\n", err)
}

func TestStartWorkerCronWrong(t *testing.T) {
	cfg := config.WorkerConfig{}
	ctx := context.Background()
	w := NewWorker(ctx, cfg, &HandlerOne{},
		WithCronSchedule("******"))
	if err := w.Run(); err != common.ErrInvalidCron {
		t.Fatalf("expected %v, got %v", common.ErrInvalidCron, err)
	}
}

func TestStartWorkerHandlerWrong(t *testing.T) {
	cfg := config.WorkerConfig{}
	ctx := context.Background()
	w := NewWorker(ctx, cfg, HandlerTwo{})
	if err := w.Run(); err != common.ErrHandlerMostBePtr {
		t.Fatalf("expected %v, got %v", common.ErrHandlerMostBePtr, err)
	}
}

func TestValidateHandler(t *testing.T) {
	h := HandlerTwo{}
	err := ValidateHandler(h)
	if err != common.ErrHandlerMostBePtr {
		t.Fatalf("expected %v, got %v", common.ErrHandlerMostBePtr, err)
	}
}

func TestStart(t *testing.T) {
	cfg := config.WorkerConfig{}
	ctx := context.Background()
	w := NewWorker(ctx, cfg, &HandlerTwo{})
	if err := w.Run(); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}

	if err := w.Wait(); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}
