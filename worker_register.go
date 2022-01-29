package workers

import (
	"errors"

	"github.com/lufred/goworker/config"
)

var (
	discoveryCreatorMap = make(map[string]func(*config.WorkerDiscoveryConfig) (WorkerDiscovery, error), 4)
)

func SetDiscovery(protocol string, creator func(*config.WorkerDiscoveryConfig) (WorkerDiscovery, error)) {
	discoveryCreatorMap[protocol] = creator
}

func GetDiscovery(wdc *config.WorkerDiscoveryConfig) (WorkerDiscovery, error) {
	creator, ok := discoveryCreatorMap[wdc.Protocol]
	if !ok {
		return nil, errors.New("Could not find the worker register with register protocol: " + wdc.Protocol)
	}
	return creator(wdc)
}
