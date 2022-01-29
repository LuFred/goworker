package main

import (
	"context"
	"fmt"
	"os"
	"time"

	workers "github.com/lufred/goworker"
	"github.com/lufred/goworker/config"
	"github.com/lufred/goworker/constant"
	_ "github.com/lufred/goworker/resolver/etcdv3"
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

	cfg.SetServiceDiscovery(config.WorkerDiscoveryConfig{
		Protocol: constant.ETCDV3Protocol,
		Address:  "127.0.0.1:2379",
		TimeOut:  3,
	})

	w := workers.NewWorker(
		context.Background(),
		cfg,
		&MyWorker{},
		workers.WithCronSchedule("* * * * *"),
		workers.WithModel(workers.Cluster),
		workers.WithServiceName("cluster-demo"),
		workers.WithShardingTotalCount(2),
		workers.WithParams("0=a,1=b"))

	if err := w.Run(); err != nil {
		fmt.Println(err)
		return
	}
	if err := w.Wait(); err != nil {
		fmt.Println(err.Error())
	}
	time.Sleep(1 * time.Second)
	return
}

type MyWorker struct {
	workerInfo workers.WatchInfo
	param      string
}

func (w *MyWorker) Watch(info workers.WatchInfo) {
	w.workerInfo = info
}

func (w *MyWorker) Before(ctx context.Context) error {
	defer fmt.Println("任务初始化 完成")
	fmt.Printf("workerInfo param :%s \n", w.workerInfo.Param)
	fmt.Printf("workerInfo id :%s \n", w.workerInfo.CurrentInstance.GetId())
	fmt.Printf("workerInfo serviceName :%s \n", w.workerInfo.CurrentInstance.GetServiceName())
	fmt.Printf("workerInfo createdAt :%d \n", w.workerInfo.CurrentInstance.GetCreatedAt())
	fmt.Printf("workerInfo shardIndex :%d \n", w.workerInfo.CurrentInstance.GetShardIndex())
	fmt.Printf("workerInfo clusterInstance count= %d\n", len(w.workerInfo.ClusterInstances))
	for _, v := range w.workerInfo.ClusterInstances {
		fmt.Printf("workerInfo clusterInstance: id :%s; exp:%d\n", v.GetId(), v.GetExpiration())
	}

	parMap := workers.ResolveClusterParameter(w.workerInfo.Param)
	shardIndex := w.workerInfo.CurrentInstance.GetShardIndex()
	if v, ok := parMap[shardIndex]; ok {
		w.param = v
	}
	return nil
}

func (w *MyWorker) Do(ctx context.Context) error {
	if w.param != "" {
		fmt.Printf("任务分片【%d】 参数：%s 开始执行\n", w.workerInfo.CurrentInstance.GetShardIndex(), w.param)
	} else {
		fmt.Printf("分片无相关任务参数，不执行后续逻辑。\n")
	}
	time.Sleep(3 * time.Second)
	return nil
}

func (w *MyWorker) After(ctx context.Context) error {
	fmt.Println("任务处理完成")
	return nil
}

func (w *MyWorker) ErrorMessage(ctx context.Context, err error) {
	fmt.Printf("%s\n", err.Error())
}
