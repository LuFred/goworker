package workers

import (
	"fmt"
	"time"

	"github.com/lufred/goworker/common/logger"
	"github.com/lufred/goworker/constant"
	"go.uber.org/zap"
)

type WorkerState int

const (
	// Initialized worker初始化完成
	Initialized WorkerState = iota
	// Pending 表示worker已创建，但未开始处理handler
	Pending
	// Running 表示worker已绑定一个process，正在处理
	Running
	// Shutdown 表示worker已结束
	Shutdown
)

type Stater interface {
	Handle()
	NextStage()
}

type WorkerStateInitialized struct {
	worker *Worker
}

func (s *WorkerStateInitialized) Handle() {
	s.worker.cfg.Logger.Debug("worker: state initialized handle...")
	defer s.NextStage()

	s.worker.signalListenAsync()

	switch s.worker.opts.Model {
	case Cluster:
		logger.Info("worker: cluster model")

		sdc := s.worker.cfg.GetWorkerDiscoveries()
		if sdc == nil {
			panic("worker: serviceDiscoveryConfig cannot be empty in cluster mode")
		}

		logger.Info(fmt.Sprintf("worker: [%s] protocol : %s", s.worker.opts.ServiceName, sdc.Protocol))
		d, err := GetDiscovery(sdc)

		if err != nil {
			panic(err)
		}

		if s.worker.opts.ShardingTotalCount <= 0 {
			s.worker.opts.ShardingTotalCount = 1
		}
		s.worker.r = NewResolver(s.worker, d, s.worker.opts.ShardingTotalCount)

		ok := false
		for !ok {
			err = s.worker.r.WorkerRegister()

			if err != nil {
				logger.Warn("worker: cluster register unsuccessful", zap.Any("warn", err))
				time.Sleep(constant.ClusterWorkerRegisterRetryInterval)
			} else {
				ok = true
			}
		}

		go func() {
			for {
				select {
				case <-s.worker.ctx.Done():
					return
				default:
					err := s.worker.r.Build()
					if err != nil {
						logger.Error("worker: cluster build error", zap.Any("error", err))
						break
					}

					return
				}

				time.Sleep(constant.ClusterWorkerBuildRetryInterval)
			}
		}()

	case Local:
		logger.Info("worker: local model")
		s.worker.workerInstance.UpdateHealthStatus(Green)
	}
	s.worker.waitForRunAsync()
}

func (s *WorkerStateInitialized) NextStage() {
	s.worker.ChangeState(&WorkerStatePending{
		worker: s.worker,
	})
}

type WorkerStatePending struct {
	worker *Worker
}

func (s *WorkerStatePending) Handle() {
	s.worker.cfg.Logger.Debug("worker: state pending handle...")
	defer s.NextStage()
}

func (s *WorkerStatePending) NextStage() {
	s.worker.ChangeState(&WorkerStateRunning{
		worker: s.worker,
	})
}

type WorkerStateRunning struct {
	worker *Worker
}

func (s *WorkerStateRunning) Handle() {
	s.worker.cfg.Logger.Debug("worker: state running handle...")
	defer s.NextStage()

	close(s.worker.startChan)

	<-s.worker.processDoneChan
}

func (s *WorkerStateRunning) NextStage() {
	s.worker.ChangeState(&WorkerStateShutdown{
		worker: s.worker,
	})
}

type WorkerStateShutdown struct {
	worker *Worker
}

func (s *WorkerStateShutdown) Handle() {
	s.worker.cfg.Logger.Debug("worker: state shutdown handle...")
	// the final state needs to be closed doneChan
	defer s.NextStage()
}

func (s *WorkerStateShutdown) NextStage() {
	defer s.worker.cfg.Logger.Debug("worker: final state...")
	s.worker.ChangeState(nil)
}
