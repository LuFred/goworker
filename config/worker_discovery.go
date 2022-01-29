package config

// WorkerDiscoveryConfig will be used to create
type WorkerDiscoveryConfig struct {
	Protocol string `json:"protocol"`
	Address  string `json:"address"`
	TimeOut  int64  `json:"timeout"`
	UserName string `json:"username"`
	Password string `json:"password"`
	Db       int    `json:"db"`
}
