package etcdv3

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	workers "github.com/lufred/goworker"
	"github.com/lufred/goworker/common"
	"github.com/lufred/goworker/common/logger"
	"github.com/lufred/goworker/config"
	"github.com/lufred/goworker/constant"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"
)

const (
	ROOT = "workers/services"
	LOCK = "lock"
)

var (
	initLock sync.Mutex
)

func init() {
	workers.SetDiscovery(constant.ETCDV3Protocol, newEtcdV3ServiceDiscovery)
}

type etcdV3WorkerDiscovery struct {
	client             *clientv3.Client
	serviceGroupPath   string
	lockKey            string
	shardingTotalCount int
}

func (e *etcdV3WorkerDiscovery) Register(instance workers.DefaultWorkerInstance, w workers.WorkerConn, shardingTotalCount int) error {
	e.shardingTotalCount = shardingTotalCount
	return e.register(instance, w, e.shardingTotalCount)
}

func (e *etcdV3WorkerDiscovery) Reset(instance workers.DefaultWorkerInstance, w workers.WorkerConn, shardingTotalCount int) error {
	e.shardingTotalCount = shardingTotalCount
	return e.register(instance, w, e.shardingTotalCount)
}

func (e *etcdV3WorkerDiscovery) Unregister(instance workers.DefaultWorkerInstance) error {
	path := toPath(instance)
	if nil != e.client {
		_, err := e.client.Delete(context.Background(), path)
		if err != nil {
			return common.WithErrorMessage(err, "worker: etcdV3 delete")
		}
	}

	return nil
}

func (e *etcdV3WorkerDiscovery) Keepalive(w workers.WorkerConn) (workers.HealthStatus, error) {
	select {
	case <-w.Context().Done():
		return workers.Red, w.Context().Err()
	case <-time.After(w.KeepaliveInterval()):
		res, err := e.client.Get(w.Context(), toPath(w.Instance()))
		if err != nil {
			return workers.Yellow, err
		}

		if len(res.Kvs) == 0 {
			return workers.Red, fmt.Errorf("worker: register info not found")
		}

		ins := w.Instance()
		err = json.Unmarshal(res.Kvs[0].Value, &ins)
		if err != nil {
			return workers.Red, err
		}

		curTime := time.Now().Unix()
		if ins.GetExpiration()+10 < curTime {
			return workers.Red, err
		}

		exp := calculateInstanceExpiration(w.KeepaliveInterval())
		ins.UpdateExpiration(time.Now().Add(exp).Unix())
		valBytes, _ := json.Marshal(ins)
		leaseGrantResp, err := e.client.Grant(w.Context(), int64(exp)/1e9)
		if err != nil {
			logger.Error("worker: service register create lease error", zap.Any("error", err))
			return workers.Yellow, err
		}

		_, err = e.client.Put(w.Context(), toPath(ins), string(valBytes), clientv3.WithLease(leaseGrantResp.ID))
		if err != nil {
			return workers.Yellow, fmt.Errorf("worker: keepalive error :%+v", err)
		}
	}

	return workers.Green, nil
}

func (e *etcdV3WorkerDiscovery) Build(w workers.WorkerConn) error {
	logger.Info(fmt.Sprintf("worker: resolver build key: %s", e.serviceGroupPath))

	var insList []workers.DefaultWorkerInstance
	if res, err := e.client.Get(context.Background(), e.serviceGroupPath, clientv3.WithPrefix()); err == nil {
		for i := range res.Kvs {
			var ins workers.DefaultWorkerInstance
			err = json.Unmarshal(res.Kvs[i].Value, &ins)
			if err != nil {
				return err
			}
			insList = append(insList, ins)
		}
	} else {
		return err
	}
	w.UpdateState(workers.State{Instances: insList})
	go e.watch(w, insList)
	return nil
}

func (e *etcdV3WorkerDiscovery) watch(w workers.WorkerConn, insList []workers.DefaultWorkerInstance) {
	defer logger.Info("discovery watch done")
	insMapper := make(map[string]bool)
	for _, ins := range insList {
		insMapper[ins.GetId()] = true
	}

	ch := e.client.Watch(w.Context(), e.serviceGroupPath, clientv3.WithPrefix())
	for {
		select {
		case <-w.Context().Done():
			return
		case c := <-ch:
			for _, event := range c.Events {
				insID := strings.TrimPrefix(string(event.Kv.Key), e.serviceGroupPath)
				switch event.Type {
				case mvccpb.PUT:
					if !insMapper[insID] {
						var ins workers.DefaultWorkerInstance
						logger.Info(fmt.Sprintf("worker: watch put key(%s) val(%s)", event.Kv.Key, event.Kv.Value))
						if err := json.Unmarshal(event.Kv.Value, &ins); err != nil {
							logger.Error("worker: watch put instance json unmarshal error", zap.Any("key", string(event.Kv.Value)), zap.Any("error", err))
						} else {
							insList = append(insList, ins)
							insMapper[ins.GetId()] = true
							w.UpdateState(workers.State{Instances: insList})
						}
					}
				case mvccpb.DELETE:
					if insMapper[insID] {
						for i := range insList {
							if insList[i].GetId() == insID {
								insList[i] = insList[len(insList)-1]
								insList = insList[:len(insList)-1]
								delete(insMapper, insID)
								logger.Info(fmt.Sprintf("worker: watch delete instance %s", insID))
								break
							}
						}
					}
				}
			}

			w.UpdateState(workers.State{Instances: insList})
		}
	}
}

func (e *etcdV3WorkerDiscovery) register(curInstance workers.DefaultWorkerInstance, w workers.WorkerConn, shardingTotalCount int) error {
	ctx := context.TODO()
	e.serviceGroupPath = toGroup(curInstance)
	e.lockKey = toLock(curInstance)
	// 1.加锁
	session, err := concurrency.NewSession(e.client)
	if err != nil {
		return fmt.Errorf("worker: CompareShardIndex error(%+v)", err)
	}
	m := concurrency.NewMutex(session, e.lockKey)
	if err = m.Lock(ctx); err != nil {
		return fmt.Errorf("worker: CompareShardIndex lock(%s) error(%+v)", e.lockKey, err)
	}
	// nolint
	defer func() {
		err := m.Unlock(ctx)
		if err != nil {
			logger.Error("worker: resolver redis unlock error", zap.Error(err))
		}
	}()
	// 2.获取服务列表
	var insList []workers.DefaultWorkerInstance
	curTime := time.Now().Unix()
	if res, err := e.client.Get(ctx, e.serviceGroupPath, clientv3.WithPrefix()); err == nil {
		logger.Info(fmt.Sprintf("resolver build key: %+v", res.Kvs))
		for i := range res.Kvs {
			var ins workers.DefaultWorkerInstance
			err = json.Unmarshal(res.Kvs[i].Value, &ins)
			if err != nil {
				return err
			}

			//移除注册中心已注册的异常数据
			//移除过期项
			if ins.GetExpiration()+10 < curTime || ins.GetId() == curInstance.GetId() {
				_, err := e.client.Delete(context.Background(), toPath(ins))
				if err != nil {
					return common.WithErrorMessage(err, "worker: etcdV3 delete")
				}
				continue
			}

			insList = append(insList, ins)
		}
	} else {
		return err
	}

	if shardingTotalCount <= len(insList) {
		return fmt.Errorf("worker: resolver register limit; curCount(%d) limit(%d) group(%s)", len(insList), shardingTotalCount, e.serviceGroupPath)
	}

	// 3.计算服务分片编号
	sort.Sort(workers.DefaultWorkerInstanceSlice(insList))
	index := 0
	for _, ins := range insList {
		if ins.GetShardIndex() == index {
			index++
		} else {
			break
		}
	}

	exp := calculateInstanceExpiration(w.KeepaliveInterval())
	curInstance.UpdateShardIndex(index)
	curInstance.UpdateExpiration(time.Now().Add(exp).Unix())
	key := toPath(curInstance)
	ins, _ := json.Marshal(curInstance)
	logger.Info("worker: service register", zap.Any("key", key), zap.Any("val", ins))

	// 4.更新服务
	leaseGrantResp, err := e.client.Grant(ctx, int64(exp)/1e9)
	if err != nil {
		return err
	}
	_, err = e.client.Put(ctx, key, string(ins), clientv3.WithPrevKV(), clientv3.WithLease(leaseGrantResp.ID))
	if err != nil {
		return err
	}

	w.UpdateShardIndex(curInstance.ShardIndex)
	return nil
}

// NewEtcdV3ServiceDiscovery
func newEtcdV3ServiceDiscovery(wdc *config.WorkerDiscoveryConfig) (workers.WorkerDiscovery, error) {
	initLock.Lock()
	defer initLock.Unlock()
	logger.Info("worker: new etcdV3 client")

	if wdc == nil || len(wdc.Address) == 0 {
		return nil, errors.New("worker: could not init the etcd service instance because the config is invalid")
	}

	cli, _ := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(wdc.Address, ","),
		DialTimeout: time.Duration(wdc.TimeOut) * time.Second,
	})

	return &etcdV3WorkerDiscovery{client: cli}, nil
}

func calculateInstanceExpiration(ka time.Duration) time.Duration {
	return (ka + 10*time.Second) * 3
}

func toPath(instance workers.DefaultWorkerInstance) string {
	// like: workers/services/serviceName1/id(uuid)
	return toGroup(instance) + instance.GetId()
}

func toGroup(instance workers.DefaultWorkerInstance) string {
	// like: workers/services/serviceName1/id(uuid)
	return ROOT + constant.PATH_SEPARATOR + instance.GetServiceName() + constant.PATH_SEPARATOR
}

func toLock(instance workers.DefaultWorkerInstance) string {
	// like: workers/services/serviceName1/id(uuid)
	return ROOT + constant.PATH_SEPARATOR + LOCK + constant.PATH_SEPARATOR + instance.GetServiceName() + constant.PATH_SEPARATOR
}
