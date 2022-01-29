package etcdv3

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"github.com/lufred/goworker/common/logger"
	"go.uber.org/zap"
)

type Session struct {
	cli *redis.Client
}

func (s *Session) HSet(ctx context.Context, k, field string, value interface{}) error {
	return s.cli.HSet(ctx, k, field, value).Err()
}

func (s *Session) HGet(ctx context.Context, k string, field string) (string, error) {
	v, err := s.cli.HGet(ctx, k, field).Result()
	if err == redis.Nil {
		return v, nil
	}
	return v, err
}

func (s *Session) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	return s.cli.HGetAll(ctx, key).Result()
}

func (s *Session) HDel(ctx context.Context, key string, fields ...string) error {
	return s.cli.HDel(ctx, key, fields...).Err()
}

func (s *Session) HWatch(ctx context.Context, key string, interval time.Duration) <-chan map[string]string {
	c := make(chan map[string]string)
	go func() {
		defer logger.Debug(fmt.Sprintf("worker: resolver watch done key(%s)", key))
		defer close(c)
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(interval):
				if r, err := s.HGetAll(ctx, key); err != nil {
					logger.Error("worker: resolver redis watch error", zap.Error(err))
				} else {
					c <- r
				}
			}
		}
	}()
	return c
}

func (s *Session) KeepAlive(ctx context.Context, key string, fieldFunc func() (f, v string), interval time.Duration) {
	go func() {
		defer logger.Warn(fmt.Sprintf("worker: resolver redis keepalive done group(%s)", key))
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(interval):
				field, val := fieldFunc()
				logger.Debug(fmt.Sprintf("worker: resolver redis keepalive group(%s) key(%s) val(%s)", key, field, val))
				err := s.HSet(ctx, key, field, val)
				if err != nil {
					logger.Error(fmt.Sprintf("worker: resolver redis keepalive error group(%s) field(%s) val(%s)", key, field, val), zap.Error(err))
				}
			}
		}
	}()
}

type Mutex struct {
	s *Session

	cancel context.CancelFunc
	ctx    context.Context
	key    string
	val    string
	exp    time.Duration
}

func NewRedisSession(redis *redis.Client) (*Session, error) {
	return &Session{
		cli: redis,
	}, nil
}

func (s *Session) NewMutex(ctx context.Context, key string) *Mutex {
	cc, cancelFun := context.WithCancel(ctx)
	m := &Mutex{
		s:      s,
		key:    key,
		val:    uuid.New().String(),
		exp:    30 * time.Second,
		ctx:    cc,
		cancel: cancelFun,
	}

	return m
}

func (m *Mutex) UnLock() error {
	defer m.cancel()
	res := m.s.cli.Get(m.ctx, m.key)
	if v, err := res.Result(); err != nil {
		return err
	} else {
		if m.val == v {
			_ = m.s.cli.Del(m.ctx, m.key)
		}
	}

	return nil
}

func (m *Mutex) Lock() error {
	for {
		select {
		case <-m.ctx.Done():
			return m.ctx.Err()
		case <-time.After(500 * time.Millisecond):
			if ok := m.s.cli.SetNX(m.ctx, m.key, m.val, m.exp); !ok.Val() {
				if ok.Err() != nil {
					return ok.Err()
				}
			} else {
				go func() {
					for {
						select {
						case <-m.ctx.Done():
							return
						case <-time.After(m.exp / 2):
							getR := m.s.cli.Get(m.ctx, m.key)
							if getR.Err() == nil && getR.Val() == m.val {
								if ok := m.s.cli.Expire(m.ctx, m.key, m.exp); ok.Err() != nil {
									logger.Error("worker: redis set expire error", zap.Error(ok.Err()))
								}
							}
						}
					}
				}()
				return nil
			}
		}
	}
}
