package goworker

import (
	"context"
	"sync"
	"time"

	"github.com/lufred/goworker/common"
	"github.com/lufred/goworker/common/logger"
	"github.com/lufred/goworker/constant"
	"go.uber.org/zap"
)

type WorkerDiscovery interface {
	Register(instance DefaultWorkerInstance, w WorkerConn, shardingTotalCount int) error

	Reset(instance DefaultWorkerInstance, w WorkerConn, shardingTotalCount int) error

	Unregister(instance DefaultWorkerInstance) error

	Build(w WorkerConn) error

	Keepalive(w WorkerConn) (HealthStatus, error)
}

type State struct {
	Instances []DefaultWorkerInstance
}

type WorkerConn interface {
	UpdateState(state State)

	UpdateShardIndex(index int)

	Context() context.Context

	KeepaliveInterval() time.Duration

	Instance() DefaultWorkerInstance
}

type Resolver struct {
	ctx                context.Context
	cancelFunc         context.CancelFunc
	unregisterOnce     sync.Once
	parent             *Worker
	state              State
	mu                 sync.Mutex
	shardingTotalCount int
	discovery          WorkerDiscovery
	keepaliveInterval  time.Duration
	kaMu               sync.Mutex
}

func NewResolver(worker *Worker, discovery WorkerDiscovery, shardingTotalCount int) *Resolver {
	cc, cancel := context.WithCancel(worker.ctx)
	r := &Resolver{
		ctx:                cc,
		cancelFunc:         cancel,
		parent:             worker,
		discovery:          discovery,
		shardingTotalCount: shardingTotalCount,
		keepaliveInterval:  constant.ClusterRegisterKeepaliveInterval,
	}
	return r
}

func (r *Resolver) WorkerRegister() (err error) {
	defer func() {
		p := recover()
		if p != nil {
			logger.Error("worker: cluster unregister panic", zap.Any("panic", p))
			err = common.ErrClusterRegister
		}
	}()

	err = r.discovery.Register(r.parent.GetInstance(), r, r.shardingTotalCount)

	if err == nil {
		r.parent.workerInstance.UpdateHealthStatus(Green)
		r.keepAliveAsync()
	}
	return
}

func (r *Resolver) keepAliveAsync() {
	go func() {
		defer logger.Info("worker: discovery keepalive done")
		for {
			select {
			case <-r.ctx.Done():
				return
			case <-time.After(r.keepaliveInterval):
				r.keepAlive()
			}
		}
	}()
}

func (r *Resolver) keepAlive() {
	defer func() {
		p := recover()
		if p != nil {
			logger.Error("worker: cluster keepalive panic", zap.Any("panic", p))
			r.parent.workerInstance.UpdateHealthStatus(Red)
		}
	}()
	r.kaMu.Lock()
	defer r.kaMu.Unlock()
	hss, err := r.discovery.Keepalive(r)
	if err != nil {
		logger.Error("worker: cluster keepalive error", zap.Error(err))
		r.parent.workerInstance.UpdateHealthStatus(hss)
	} else {
		r.parent.workerInstance.UpdateHealthStatus(hss)
	}

	// 服务注册信息异常，重新注册
	if hss == Red {
		r.workerRegisterReset()
	}
}

func (r *Resolver) Build() (err error) {
	defer func() {
		p := recover()
		if p != nil {
			logger.Error("worker: cluster build panic", zap.Any("panic", p))
			err = common.ErrClusterBuild
		}
	}()
	err = r.discovery.Build(r)
	return
}

func (r *Resolver) workerRegisterReset() {
	defer func() {
		p := recover()
		if p != nil {
			logger.Error("worker: cluster register reset panic", zap.Any("panic", p))
		}
	}()

	logger.Debug("worker: cluster register reset")
	err := r.discovery.Reset(r.parent.workerInstance, r, r.shardingTotalCount)
	if err != nil {
		logger.Error("worker: cluster register reset error", zap.Error(err))
	}
}

func (r *Resolver) WorkerUnregister() {
	r.unregisterOnce.Do(func() {
		defer func() {
			p := recover()
			if p != nil {
				logger.Error("worker: cluster unregister panic", zap.Any("panic", p))
			}
		}()

		logger.Info("worker: cluster unregister")
		err := r.discovery.Unregister(r.parent.GetInstance())
		if err != nil {
			logger.Error("worker: cluster unregister error", zap.Error(err))
		}
	})
}

func (r *Resolver) UpdateState(s State) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.state = s
}

func (r *Resolver) UpdateShardIndex(i int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.parent.workerInstance.UpdateShardIndex(i)
}

func (r *Resolver) Context() context.Context {
	return r.ctx
}

func (r *Resolver) Instance() DefaultWorkerInstance {
	return r.parent.workerInstance
}

func (r *Resolver) KeepaliveInterval() time.Duration {
	return r.keepaliveInterval
}
