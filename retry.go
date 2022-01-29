package goworker

import (
	"context"
	"math"
	"time"

	"github.com/lufred/goworker/common"
)

const RetryDone = time.Duration(-1)

const (
	defaultInitialInterval            = time.Second
	defaultBackoffCoefficient         = 2.0
	defaultMaximumIntervalCoefficient = 10.0
	defaultMaximumAttempts            = 0
)

type RetryPolicy interface {
	ComputeNextDelay(numAttempts int) time.Duration
}

type DefaultRetryPolicy struct {
	// initialInterval 首次重启间隔，后续重启时间基于 backoffCoefficient
	initialInterval time.Duration

	// backoffCoefficient 重试补偿系数，值必须>=1, 默认为1
	backoffCoefficient float64

	// maximumInterval 最大间隔时间，默认为 initialInterval 的10倍
	maximumInterval time.Duration

	// maximumAttempts 最大尝试次数，0表示无限制
	maximumAttempts int
}

func GetDefaultRetryPolicy() *DefaultRetryPolicy {
	return &DefaultRetryPolicy{
		initialInterval:    defaultInitialInterval,
		backoffCoefficient: defaultBackoffCoefficient,
		maximumInterval:    defaultMaximumIntervalCoefficient * defaultInitialInterval,
		maximumAttempts:    defaultMaximumAttempts,
	}
}

func (d *DefaultRetryPolicy) SetInitialInterval(t time.Duration) *DefaultRetryPolicy {
	d.initialInterval = t
	d.maximumInterval = d.initialInterval * defaultMaximumIntervalCoefficient
	return d
}

func (d *DefaultRetryPolicy) SetBackoffCoefficient(b float64) *DefaultRetryPolicy {
	d.backoffCoefficient = b
	return d
}

func (d *DefaultRetryPolicy) SetMaximumInterval(t time.Duration) *DefaultRetryPolicy {
	d.maximumInterval = t
	return d
}

func (d *DefaultRetryPolicy) SetMaximumAttempts(ma int) *DefaultRetryPolicy {
	d.maximumAttempts = ma
	return d
}

func (d *DefaultRetryPolicy) ComputeNextDelay(numAttempts int) time.Duration {
	if d.maximumAttempts != 0 && d.maximumAttempts < numAttempts {
		return RetryDone
	}

	nextInterval := float64(d.initialInterval) * math.Pow(d.backoffCoefficient, float64(numAttempts-1))
	if nextInterval <= 0 {
		return RetryDone
	}

	nextDuration := time.Duration(nextInterval)

	if nextDuration < d.initialInterval {
		return RetryDone
	}

	if d.maximumInterval < nextDuration {
		nextDuration = d.maximumInterval
	}

	return nextDuration
}

type (
	retrierImpl struct {
		policy     RetryPolicy
		numAttempt int
	}

	Retrier interface {
		NextBackOff() time.Duration
		TryAgain() bool
		Reset()
	}
)

func newRetrier(policy RetryPolicy) Retrier {
	return &retrierImpl{
		policy:     policy,
		numAttempt: 1,
	}
}

func (r *retrierImpl) Reset() {
	r.numAttempt = 1
}

func (r *retrierImpl) TryAgain() bool {
	return r.policy.ComputeNextDelay(r.numAttempt) != RetryDone
}

func (r *retrierImpl) NextBackOff() time.Duration {
	nextInterval := r.policy.ComputeNextDelay(r.numAttempt)
	r.numAttempt++
	return nextInterval
}

func waitRetryBackoff(ctx context.Context, r Retrier) error {
	waitTime := time.Duration(0)

	if r != nil {
		waitTime = r.NextBackOff()
	}

	if waitTime > 0 {
		timer := time.NewTimer(waitTime)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	} else {
		return common.ErrAttemptExhausted
	}

	return nil
}
