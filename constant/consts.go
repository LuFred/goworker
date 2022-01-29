package constant

import "time"

const (
	ETCDV3Protocol = "etcdv3"
	RedisProtocol  = "redis"
)

const (
	PATH_SEPARATOR = "/"
	PORT_SEPARATOR = ":"
)


const (
	ClusterRegisterKeepaliveInterval   = 10 * time.Second
	ClusterWorkerRegisterRetryInterval = 3 * time.Second
	ClusterWorkerBuildRetryInterval    = 3 * time.Second
)