package goworker

import (
	"time"
)

func newDefaultWorkerInstance(id string, sn string) DefaultWorkerInstance {
	return DefaultWorkerInstance{
		Id:           id,
		ServiceName:  sn,
		CreatedAt:    time.Now().Unix(),
		HealthStatus: Yellow,
	}
}

type HealthStatus int

const (
	Green  HealthStatus = 0
	Yellow HealthStatus = 1
	Red    HealthStatus = 2
)

type DefaultWorkerInstance struct {
	Id           string       `json:"id"`
	ServiceName  string       `json:"serviceName"`
	CreatedAt    int64        `json:"createdAt"`
	ShardIndex   int          `json:"shardIndex"`
	HealthStatus HealthStatus `json:"healthStatus"`
	Expiration   int64        `json:"expiration"` //秒，服务到期时间，暂只有redis集群模式下 使用
}

func (d *DefaultWorkerInstance) GetId() string {
	return d.Id
}

func (d *DefaultWorkerInstance) GetServiceName() string {
	return d.ServiceName
}

func (d *DefaultWorkerInstance) GetCreatedAt() int64 {
	return d.CreatedAt
}

func (d *DefaultWorkerInstance) GetShardIndex() int {
	return d.ShardIndex
}

func (d *DefaultWorkerInstance) GetHealthStatus() HealthStatus {
	return d.HealthStatus
}

func (d *DefaultWorkerInstance) GetExpiration() int64 {
	return d.Expiration
}

func (d *DefaultWorkerInstance) UpdateShardIndex(i int) {
	d.ShardIndex = i
}

func (d *DefaultWorkerInstance) UpdateExpiration(exp int64) {
	d.Expiration = exp
}

func (d *DefaultWorkerInstance) UpdateHealthStatus(hs HealthStatus) {
	d.HealthStatus = hs
}

type DefaultWorkerInstanceSlice []DefaultWorkerInstance

func (s DefaultWorkerInstanceSlice) Len() int { return len(s) }

func (s DefaultWorkerInstanceSlice) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

func (s DefaultWorkerInstanceSlice) Less(i, j int) bool {
	if s[i].ShardIndex != s[j].ShardIndex {
		return s[i].ShardIndex < s[j].ShardIndex
	}

	if s[i].Expiration != s[j].Expiration {
		return s[i].Expiration < s[j].Expiration
	}

	return s[i].CreatedAt < s[j].CreatedAt
}
