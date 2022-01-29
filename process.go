package goworker

import (
	"context"
	"fmt"
	"reflect"
	"runtime"
	"sync"

	"github.com/lufred/goworker/common"

	"go.uber.org/zap"
)

type process struct {
	parent  *Worker
	errChan chan error

	startChan chan struct{}

	ctx        context.Context
	cancelFunc context.CancelFunc
	cancelOnce *sync.Once

	handler          interface{}
	beforeFunc       func(ctx context.Context, startChan <-chan struct{}) <-chan struct{}
	afterFunc        func(ctx context.Context, startChan <-chan struct{}) <-chan struct{}
	runFunc          func(ctx context.Context, startChan <-chan struct{}) <-chan struct{}
	errorMessageFunc func(ctx context.Context, err error)
}

func newHandler(i interface{}) interface{} {
	ref := reflect.TypeOf(i)
	vl := reflect.New(ref.Elem())
	return vl.Interface()
}

func newProcess(w *Worker) *process {
	ctx, cancelFunc := context.WithCancel(w.ctx)

	p := &process{
		parent:     w,
		ctx:        ctx,
		handler:    newHandler(w.handler),
		cancelFunc: cancelFunc,
		cancelOnce: new(sync.Once),
		startChan:  make(chan struct{}),
		errChan:    make(chan error),
	}

	beforeErrChan := make(chan error)
	wrapHandler := p.handler.(BaseHandler)
	p.beforeFunc = wrapPipeline(wrapRetry(wrapRecovery(wrapHandler.Before), p.parent.opts.Retry, beforeErrChan))

	runErrChan := make(chan error)
	p.runFunc = wrapPipeline(wrapRetry(wrapRecovery(wrapHandler.Do), p.parent.opts.Retry, runErrChan))

	afterErrChan := make(chan error)
	p.afterFunc = wrapPipeline(wrapRetry(wrapRecovery(wrapHandler.After), p.parent.opts.Retry, afterErrChan))

	p.errorMessageFunc = wrapErrorMessage(wrapHandler.ErrorMessage, p.parent.cfg.Logger)

	p.errorMessageListenAsync(FanIn(p.ctx, p.errChan, beforeErrChan, runErrChan, afterErrChan))

	return p
}

// Run 执行
func (p *process) Run() {
	if wt, ok := p.handler.(WatchHandler); ok {
		info := WatchInfo{
			CurrentInstance:  p.parent.workerInstance,
			Param:            p.parent.opts.Params,
			ClusterInstances: make([]DefaultWorkerInstance, 0),
		}
		if p.parent.opts.Model == Cluster && p.parent.r != nil && len(p.parent.r.state.Instances) > 0 {
			cpIns := make([]DefaultWorkerInstance, len(p.parent.r.state.Instances))
			copy(cpIns, p.parent.r.state.Instances)
			info.ClusterInstances = cpIns
		}

		wt.Watch(info)
	}

	p.run()
}

func (p *process) run() {
	p.parent.cfg.Logger.Info("worker: start handler")
	defer p.parent.cfg.Logger.Info("worker: handler execution completed")
	defer close(p.errChan)
	startChan := make(chan struct{}, 1)
	defer close(startChan)

	startChan <- struct{}{}
	finish := p.afterFunc(p.ctx, p.runFunc(p.ctx, p.beforeFunc(p.ctx, startChan)))
	<-finish
}

// Cancel 取消任务
func (p *process) Cancel() {
	p.cancelOnce.Do(func() {
		if p.cancelFunc != nil {
			p.cancelFunc()
		}
	})
}

// wrapRetry 包装函数,赋予panic捕获能力
func wrapRecovery(f func(ctx context.Context) error) func(ctx context.Context) error {
	return func(ctx context.Context) (err error) {
		if f == nil {
			return nil
		}
		defer func() {
			if pErr := recover(); pErr != nil {
				var buf [1024 * 8]byte
				n := runtime.Stack(buf[:], false)
				we := common.WrapError(pErr, common.ErrorTypeAny, fmt.Sprintf("panic(%+v)", pErr))
				we.SetStack(string(buf[:n]))
				err = we
			}
		}()

		err = f(ctx)
		return
	}
}

const (
	HandleRunLabelRun   = true
	HandleRunLabelUnRun = false
)

// wrapRetry 包装函数,赋予出错重试能力;
func wrapRetry(f func(ctx context.Context) error, policy Retrier, errChan chan<- error) func(ctx context.Context, runLabel bool) bool {
	return func(ctx context.Context, runLabel bool) (success bool) {
		defer close(errChan)
		policy.Reset()
		//TODO 待优化，临时解决方案，避免errChan无法关闭
		//nolint
		if runLabel == HandleRunLabelUnRun {
			return
		}

		for {
			select {
			case <-ctx.Done():
				return
			default:
				err := f(ctx)
				if err == nil {
					success = true
					return
				}

				switch err {
				case common.ErrTryAgain:
					continue
				}

				errChan <- err

				if !policy.TryAgain() {
					errChan <- common.ErrAttemptExhausted
					return
				}

				if err = waitRetryBackoff(ctx, policy); err != nil {
					errChan <- err
					return
				}
			}
		}
	}
}

// wrapPipeline 包转函数，将普通函数转化为执行管道
func wrapPipeline(f func(ctx context.Context, runLabel bool) bool) func(ctx context.Context, startChan <-chan struct{}) <-chan struct{} {
	return func(ctx context.Context, startChan <-chan struct{}) <-chan struct{} {
		finishChan := make(chan struct{})
		go func() {
			defer close(finishChan)
			select {
			case <-ctx.Done():
				return
			case _, ok := <-startChan:
				if ok {
					success := f(ctx, HandleRunLabelRun)
					if success {
						finishChan <- struct{}{}
					}
				} else {
					f(ctx, HandleRunLabelUnRun)
				}
			}
		}()

		return finishChan
	}
}

//wrapErrorMessage 包装函数，ErrorMessage专用
func wrapErrorMessage(f func(ctx context.Context, err error), logger *zap.Logger) func(ctx context.Context, err error) {
	return func(ctx context.Context, err error) {
		defer func() {
			if pErr := recover(); pErr != nil {
				var buf [1024 * 8]byte
				n := runtime.Stack(buf[:], false)
				we := common.WrapError(pErr, common.ErrorTypeAny, fmt.Sprintf("panic(%+v)", pErr))
				we.SetStack(string(buf[:n]))
				logger.Error("[Recovery]",
					zap.Any("error", we),
					zap.ByteString("stack", buf[:n]))
			}
		}()

		f(ctx, err)
	}
}

// errorMessageListenAsync 监听Error采集通道，调用ErrorMessage执行
func (p *process) errorMessageListenAsync(c <-chan error) {
	go func() {
		p.parent.cfg.Logger.Info("worker: start listening error message")
		defer p.parent.cfg.Logger.Info("worker: end listening error message")
		for err := range c {
			p.errorMessageFunc(p.ctx, err)
		}
	}()
}
