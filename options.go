package workers

import "github.com/google/uuid"

const (
	NoCronExpress = ""
)

type WorkerModel int

const (
	Local   WorkerModel = 1
	Cluster WorkerModel = 2
)

type workerOptions struct {
	ID string

	ServiceName string

	// Retry 重试策略
	Retry Retrier

	Model WorkerModel

	// CronSchedule cron表达式
	//
	// https://en.wikipedia.org/wiki/Cron
	// ┌───────────── minute (0 - 59)
	// │ ┌───────────── hour (0 - 23)
	// │ │ ┌───────────── day of the month (1 - 31)
	// │ │ │ ┌───────────── month (1 - 12)
	// │ │ │ │ ┌───────────── day of the week (0 - 6) (Sunday to Saturday;
	// │ │ │ │ │
	// │ │ │ │ │
	// │ │ │ │ │
	// * * * * *
	CronSchedule string

	Params string

	ShardingTotalCount int
}

type WorkerOption interface {
	apply(*workerOptions)
}

type funcWorkerOption struct {
	applyFunc func(*workerOptions)
}

func (fdo *funcWorkerOption) apply(do *workerOptions) {
	fdo.applyFunc(do)
}

func newFuncWorkerOption(f func(*workerOptions)) *funcWorkerOption {
	return &funcWorkerOption{
		applyFunc: f,
	}
}

func defaultWorkerOptions() *workerOptions {
	wo := &workerOptions{
		ID:    uuid.New().String(),
		Retry: newRetrier(GetDefaultRetryPolicy()),
		Model: Local,
	}
	return wo
}

//WithRetryPolicy 自定义重试规则
func WithRetryPolicy(rp RetryPolicy) WorkerOption {
	return newFuncWorkerOption(func(o *workerOptions) {
		if rp != nil {
			o.Retry = newRetrier(rp)
		}
	})
}

//WithCronSchedule cron表达式
func WithCronSchedule(cron string) WorkerOption {
	return newFuncWorkerOption(func(o *workerOptions) {
		o.CronSchedule = cron
	})
}

//WithModel worker执行模式
func WithModel(model WorkerModel) WorkerOption {
	return newFuncWorkerOption(func(o *workerOptions) {
		o.Model = model
	})
}

//WithModel worker执行模式
func WithServiceName(sn string) WorkerOption {
	return newFuncWorkerOption(func(o *workerOptions) {
		o.ServiceName = sn
	})
}

//WithParams handle执行时可传递的参数
func WithParams(params string) WorkerOption {
	return newFuncWorkerOption(func(o *workerOptions) {
		o.Params = params
	})
}

//WithShardingTotalCount worker分片数
func WithShardingTotalCount(count int) WorkerOption {
	return newFuncWorkerOption(func(o *workerOptions) {
		o.ShardingTotalCount = count
	})
}
