package workers

import (
	"context"
	"reflect"

	"github.com/lufred/goworker/common"
)

// BaseHandler 任务处理程序接口
type BaseHandler interface {
	Before(ctx context.Context) error
	Do(ctx context.Context) error
	After(ctx context.Context) error
	ErrorMessage(ctx context.Context, err error)
}

func ValidateHandler(handler interface{}) error {
	hType := reflect.TypeOf(handler)
	if hType.Kind() != reflect.Ptr {
		return common.ErrHandlerMostBePtr
	}

	return nil
}

type WatchInfo struct {
	CurrentInstance  DefaultWorkerInstance
	Param            string
	ClusterInstances []DefaultWorkerInstance
}

type WatchHandler interface {
	BaseHandler
	Watch(info WatchInfo)
}
