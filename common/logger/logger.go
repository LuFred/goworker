package logger

import (
	"sync"

	"github.com/lufred/goworker/common"
	"go.uber.org/zap"
)

var (
	logger      *zap.Logger
	loggerMutex sync.Mutex
)

// NewDefaultLogger 获取默认logger对象
func NewDefaultLogger() *zap.Logger {
	lg, err := zap.NewProduction()
	if err != nil {
		common.ExitWithError(common.ExitError, err)
	}

	logger = lg
	return lg
}

func SetGlobalLogger(lg *zap.Logger) {
	loggerMutex.Lock()
	defer loggerMutex.Unlock()
	logger = lg
}
