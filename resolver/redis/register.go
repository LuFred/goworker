package etcdv3

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	workers "github.com/lufred/goworker"
	"github.com/lufred/goworker/common"
	"github.com/lufred/goworker/common/logger"
	"github.com/lufred/goworker/config"
	"github.com/lufred/goworker/constant"
	"go.uber.org/zap"
)

const (
	ROOT        = "workers/services"
	LOCK        = "lock"
	EmptyString = ""
)

var (
	initLock sync.Mutex
)

func init() {
	workers.SetDiscovery(constant.RedisProtocol, newRedisServiceDiscovery)
}

type redisWorkerDiscovery struct {
	client             *redis.Client
	serviceGroupPath   string
	lockKey            string
	shardingTotalCount int
}

func (e *redisWorkerDiscovery) Register(instance workers.DefaultWorkerInstance, w workers.WorkerConn, shardingTotalCount int) error {
	e.shardingTotalCount = shardingTotalCount
	return e.register(instance, w, e.shardingTotalCount)
}

func (e *redisWorkerDiscovery) Reset(curInstance workers.DefaultWorkerInstance, w workers.WorkerConn, shardingTotalCount int) error {
	e.shardingTotalCount = shardingTotalCount
	return e.register(curInstance, w, e.shardingTotalCount)
}

func (e *redisWorkerDiscovery) Unregister(instance workers.DefaultWorkerInstance) error {
	session, _ := NewRedisSession(e.client)
	err := session.HDel(context.Background(), e.serviceGroupPath, toInstanceField(instance))
	if err != nil {
		return common.WithErrorMessage(err, "worker: etcdV3 delete")
	}

	return nil
}

func (e *redisWorkerDiscovery) Keepalive(w workers.WorkerConn) (workers.HealthStatus, error) {
	select {
	case <-w.Context().Done():
		return workers.Red, w.Context().Err()
	case <-time.After(w.KeepaliveInterval()):
		session, err := NewRedisSession(e.client)
		if err != nil {
			return workers.Yellow, fmt.Errorf("worker: resolver new redis session error(%+v)", err)
		}
		ins := w.Instance()
		field := ins.GetId()

		valStr, err := session.HGet(w.Context(), e.serviceGroupPath, field)
		if err != nil {
			return workers.Yellow, err
		}

		if valStr == EmptyString {
			return workers.Red, fmt.Errorf("worker: register info not found")
		}

		err = json.Unmarshal([]byte(valStr), &ins)
		if err != nil {
			return workers.Red, err
		}

		curTime := time.Now().Unix()
		if ins.GetExpiration()+10 < curTime {
			return workers.Red, err
		}

		ins.UpdateExpiration(time.Now().Add(calculateInstanceExpiration(w.KeepaliveInterval())).Unix())
		valBytes, _ := json.Marshal(ins)
		err = session.HSet(w.Context(), e.serviceGroupPath, field, string(valBytes))
		if err != nil {
			return workers.Yellow, fmt.Errorf("worker: keepalive error :%+v", err)
		}
	}

	return workers.Green, nil
}

func (e *redisWorkerDiscovery) Build(w workers.WorkerConn) error {
	logger.Info(fmt.Sprintf("worker: resolver build key: %s", e.serviceGroupPath))

	var insList []workers.DefaultWorkerInstance
	session, _ := NewRedisSession(e.client)
	mp, err := session.HGetAll(w.Context(), e.serviceGroupPath)
	if err != nil {
		return err
	}

	for _, v := range mp {
		var ins workers.DefaultWorkerInstance
		err = json.Unmarshal([]byte(v), &ins)
		if err != nil {
			return err
		}
		insList = append(insList, ins)
	}
	w.UpdateState(workers.State{Instances: insList})
	go e.watch(w)
	return nil
}

func (e *redisWorkerDiscovery) watch(w workers.WorkerConn) {
	defer logger.Info("discovery watch done")
	//1. 定时获取HGetAll
	session, _ := NewRedisSession(e.client)
	ch := session.HWatch(w.Context(), e.serviceGroupPath, 30*time.Second)
	for {
		select {
		case <-w.Context().Done():
			return
		case c := <-ch:
			var inss []workers.DefaultWorkerInstance
			for _, v := range c {
				var ins workers.DefaultWorkerInstance
				err := json.Unmarshal([]byte(v), &ins)
				if err != nil {
					logger.Error("worker: watch instance json unmarshal error", zap.Any("value", v), zap.Any("error", err))
				}

				// 过期时间+10 仍然小于当前时间 实例过期
				if ins.Expiration+10 < time.Now().Unix() {
					if err := session.HDel(w.Context(), e.serviceGroupPath, toInstanceField(ins)); err == nil {
						logger.Debug("worker: watch delete expired service instance")
					} else {
						logger.Error(fmt.Sprintf("worker: watch delete expired service instance error (%s)", err.Error()))
					}
				} else {
					inss = append(inss, ins)
				}
			}
			w.UpdateState(workers.State{Instances: inss})
			logger.Debug(fmt.Sprintf("group(%s) instances count : %d  %+v", e.serviceGroupPath, len(inss), inss))
		}
	}
}

func (e *redisWorkerDiscovery) register(curInstance workers.DefaultWorkerInstance, w workers.WorkerConn, shardingTotalCount int) error {
	e.serviceGroupPath = toGroup(curInstance)
	e.lockKey = toLock(curInstance)
	// 1.加锁
	session, err := NewRedisSession(e.client)
	if err != nil {
		return fmt.Errorf("worker: resolver new redis session error(%+v)", err)
	}
	m := session.NewMutex(w.Context(), e.lockKey)
	if err = m.Lock(); err != nil {
		return fmt.Errorf("worker: resolver redis lock error key(%s) error(%+v)", e.lockKey, err)
	}

	defer func() {
		err := m.UnLock()
		if err != nil {
			logger.Error("worker: resolver redis unlock error", zap.Error(err))
		}
	}()

	// 2. 获取服务列表
	var mp map[string]string
	if mp, err = session.HGetAll(w.Context(), e.serviceGroupPath); err != nil {
		fmt.Printf("worker: resolver redis get service list error(%+v)", err)
		return err
	}
	var insList []workers.DefaultWorkerInstance
	curTime := time.Now().Unix()
	for _, v := range mp {
		var ins workers.DefaultWorkerInstance
		err = json.Unmarshal([]byte(v), &ins)
		if err != nil {
			return err
		}

		//移除注册中心已注册的异常数据
		//移除过期项
		if ins.GetExpiration()+10 < curTime || ins.GetId() == curInstance.GetId() {
			err = session.HDel(w.Context(), e.serviceGroupPath, ins.GetId())
			if err != nil {
				return err
			}
			continue
		}

		insList = append(insList, ins)
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
	ins, _ := json.Marshal(curInstance)
	field := toInstanceField(curInstance)

	// 4. 更新redis
	if err = session.HSet(w.Context(), e.serviceGroupPath, field, string(ins)); err != nil {
		return fmt.Errorf("worker resolver put service error(%+v)", err)
	}

	w.UpdateShardIndex(curInstance.ShardIndex)
	return nil
}

func calculateInstanceExpiration(ka time.Duration) time.Duration {
	return (ka + 10*time.Second) * 3
}

func toInstanceField(instance workers.DefaultWorkerInstance) string {
	// like: id(uuid)
	return instance.GetId()
}

func toGroup(instance workers.DefaultWorkerInstance) string {
	// like: workers/services/serviceName1/id(uuid)
	return ROOT + constant.PATH_SEPARATOR + instance.GetServiceName() + constant.PATH_SEPARATOR
}

func toLock(instance workers.DefaultWorkerInstance) string {
	// like: workers/services/serviceName1/id(uuid)
	return ROOT + constant.PATH_SEPARATOR + LOCK + constant.PATH_SEPARATOR + instance.GetServiceName() + constant.PATH_SEPARATOR
}

// newRedisServiceDiscovery
func newRedisServiceDiscovery(wdc *config.WorkerDiscoveryConfig) (workers.WorkerDiscovery, error) {
	initLock.Lock()
	defer initLock.Unlock()
	logger.Info("worker: new redis client")

	if wdc == nil || len(wdc.Address) == 0 {
		return nil, errors.New("worker: could not init the redis service instance because the config is invalid")
	}

	cli := redis.NewClient(&redis.Options{
		Addr:     wdc.Address,
		Password: wdc.Password,
		DB:       wdc.Db,
	})

	return &redisWorkerDiscovery{client: cli}, nil
}
