package goworker

import (
	"math"
	"time"

	"github.com/lufred/goworker/common"
	"github.com/lufred/goworker/common/logger"
	"go.uber.org/zap"

	"github.com/robfig/cron"
)

// NoCronBackoff 不存在cron重试
const NoCronBackoff = time.Duration(-1)

// ValidateSchedule 校验cron表达式格式是否准确
func ValidateSchedule(cronSchedule string) error {
	if cronSchedule == "" {
		return nil
	}

	if _, err := cron.ParseStandard(cronSchedule); err != nil {
		return common.ErrInvalidCron
	}

	return nil
}

// GetBackoffForNextSchedule 获取下一次任务执行时间
func GetBackoffForNextSchedule(cronSchedule string) (next time.Duration) {
	if len(cronSchedule) == 0 {
		return NoCronBackoff
	}
	schedule, err := cron.ParseStandard(cronSchedule)
	if err != nil {
		logger.Error("worker: cron schedule error", zap.Any("err", err.Error()))
		return NoCronBackoff
	}
	now := time.Now()
	nextScheduleTime := schedule.Next(now)
	next = time.Duration(math.Ceil(nextScheduleTime.Sub(now).Seconds())) * time.Second
	logger.Info("worker: nextSchedule", zap.Any("duration", next))
	return next
}
