package config

// nolint
func (c *WorkerConfig) GetWorkerDiscoveries() *WorkerDiscoveryConfig {
	return c.ServiceDiscovery
}
