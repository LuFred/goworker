package workers

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/lufred/goworker/common"
	"github.com/lufred/goworker/common/logger"
	"github.com/lufred/goworker/config"

	"go.uber.org/zap"
)

// Worker 任务核心类
type Worker struct {
	state                 Stater
	stateChangeNotifyChan chan struct{}

	opts *workerOptions
	cfg  *config.WorkerConfig

	doneChan   chan struct{} // doneChan 程序执行结束
	signalChan chan os.Signal
	startChan  chan struct{}

	ctx             context.Context
	cancelFunc      context.CancelFunc
	cancelOnce      *sync.Once
	handler         BaseHandler
	processDoneChan chan struct{}

	workerInstance DefaultWorkerInstance

	r *Resolver

	// nolint
	currentProcess *process
}

// NewWorker 获取任务类
func NewWorker(ctx context.Context, cfg config.WorkerConfig, hd BaseHandler, opt ...WorkerOption) *Worker {
	var workerCtx, workerCancel = context.WithCancel(ctx)

	opts := defaultWorkerOptions()
	for _, o := range opt {
		o.apply(opts)
	}

	var w = &Worker{
		stateChangeNotifyChan: make(chan struct{}, 1),
		ctx:                   workerCtx,
		cfg:                   &cfg,
		opts:                  opts,
		cancelFunc:            workerCancel,
		handler:               hd,
		processDoneChan:       make(chan struct{}),
		doneChan:              make(chan struct{}),
		signalChan:            make(chan os.Signal),
		startChan:             make(chan struct{}),
		cancelOnce:            new(sync.Once),
	}

	if w.cfg.Logger == nil {
		w.cfg.Logger = logger.NewDefaultLogger()
	} else {
		logger.SetGlobalLogger(w.cfg.Logger)
	}

	var sn string
	//如果服务名称未填写。默认用id
	if w.opts.ServiceName == "" {
		sn = w.opts.ID
	} else {
		sn = w.opts.ServiceName
	}

	w.workerInstance = newDefaultWorkerInstance(w.opts.ID, sn)

	return w
}

func (w *Worker) ChangeState(state Stater) {
	w.state = state
	w.stateChangeNotifyChan <- struct{}{}
}

func (w *Worker) Validate() error {
	if err := ValidateHandler(w.handler); err != nil {
		return err
	}

	if w.opts.CronSchedule != "" {
		if err := ValidateSchedule(w.opts.CronSchedule); err != nil {
			return err
		}
	}

	return nil
}

// Run 启动服务
func (w *Worker) Run() error {
	if err := w.Validate(); err != nil {
		w.Cancel()
		return err
	}

	w.stateListenAsync()

	w.ChangeState(&WorkerStateInitialized{
		worker: w,
	})

	return nil
}

// Wait block住代码继续执行，直到发生以下几种情况：
// 1.接收到上下文超时或取消
// 2.任务执行完成
func (w *Worker) Wait() error {
	select {
	case <-w.ctx.Done():
		if err := w.ctx.Err(); err != nil {
			err = common.ContextErr(err)
			return err
		}
		return nil
	case <-w.doneChan:
		w.Cancel()
		w.cfg.Logger.Info("worker: done")
		return nil
	}
}

// waitForRunAsync 业务逻辑执行
func (w *Worker) waitForRunAsync() {
	go func() {
		w.cfg.Logger.Info("worker: start running")
		defer w.cfg.Logger.Info("worker: end of run")
		defer close(w.processDoneChan)
		select {
		case <-w.ctx.Done():
			return
		case <-w.startChan:
			goNext := true
			for goNext {
				var p *process
				next := GetBackoffForNextSchedule(w.opts.CronSchedule)
				if next == NoCronBackoff {
					goNext = false
				}

				select {
				case <-w.ctx.Done():
					return
				case <-time.After(next):
					p = w.initNewProcess()
				}

				// 立刻执行一次健康检查
				if w.opts.Model == Cluster {
					w.r.keepAlive()
				}
				if ok, err := w.waitHealthIsGreen(); ok {
					if p != nil {
						p.Run()
					}
				} else {
					logger.Error("worker: wait health green error", zap.Error(err))
				}
			}
		}
	}()
}

// Cancel 取消任务
func (w *Worker) Cancel() {
	w.cancelOnce.Do(func() {
		if w.opts.Model == Cluster && w.r != nil {
			w.r.WorkerUnregister()
		}

		if w.cancelFunc != nil {
			w.cancelFunc()
		}
	})
}

// GetWorkerInstance
func (w *Worker) UpdateInstance(ins DefaultWorkerInstance) {
	w.workerInstance = ins
}

// GetWorkerInstance
func (w *Worker) UpdateShardIndex(index int) {
	w.workerInstance.UpdateShardIndex(index)
}

// GetInstance
func (w *Worker) GetInstance() DefaultWorkerInstance {
	return w.workerInstance
}

// signalListenAsync 监听停止信号
func (w *Worker) signalListenAsync() {
	signal.Notify(w.signalChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		select {
		case <-w.ctx.Done():
			return
		case sig := <-w.signalChan:
			w.cfg.Logger.Info(fmt.Sprintf("worker: get signal %s, application will shutdown.", sig))
			w.Cancel()
		}
	}()
}

// stateListenAsync worker 状态变更监听
func (w *Worker) stateListenAsync() {
	go func() {
		w.cfg.Logger.Info("worker: start listening for state changes")
		defer w.cfg.Logger.Info("worker: stop listening for state changes")
		for {
			select {
			case <-w.ctx.Done():
				return
			case <-w.stateChangeNotifyChan:
				if w.state != nil {
					w.state.Handle()
				} else {
					//最终状态，结束worker
					close(w.doneChan)
				}
			}
		}
	}()
}

func (w *Worker) initNewProcess() *process {
	p := newProcess(w)
	return p
}

func (w *Worker) waitHealthIsGreen() (bool, error) {
	for {
		select {
		case <-w.ctx.Done():
			return false, w.ctx.Err()
		case <-time.After(200 * time.Millisecond):
			if w.workerInstance.GetHealthStatus() != Green {
				break
			}
			return true, nil
		}
	}
}
