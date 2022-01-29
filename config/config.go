package config

import (
	"github.com/lufred/goworker/common/logger"
	"go.uber.org/zap"
)

type WorkerConfig struct {
	Logger *zap.Logger

	ServiceDiscovery *WorkerDiscoveryConfig `yaml:"service_discovery" json:"service_discovery,omitempty"`
}

//DefaultWorkerConfig 默认config
func DefaultWorkerConfig() WorkerConfig {
	return WorkerConfig{
		Logger:           logger.NewDefaultLogger(),
		ServiceDiscovery: &WorkerDiscoveryConfig{},
	}
}

func (c *WorkerConfig) SetServiceDiscovery(s WorkerDiscoveryConfig) {
	c.ServiceDiscovery = &s
}
