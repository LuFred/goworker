package goworker

import (
	"context"
	"strconv"
	"strings"
	"sync"
)

func FanIn(ctx context.Context, channels ...<-chan error) <-chan error {
	var wg sync.WaitGroup
	multiplexedChan := make(chan error)
	multiplex := func(c <-chan error) {
		defer wg.Done()
		for i := range c {
			select {
			case <-ctx.Done():
				return
			case multiplexedChan <- i:
			}
		}
	}

	wg.Add(len(channels))
	for _, c := range channels {
		go multiplex(c)
	}

	go func() {
		wg.Wait()
		close(multiplexedChan)
	}()
	return multiplexedChan
}

func ResolveClusterParameter(params string) map[int]string {
	var res map[int]string
	if params == "" {
		return res
	}

	ps := strings.Split(params, ",")
	if len(ps) < 1 {
		return res
	}

	res = make(map[int]string, len(ps))
	for _, item := range ps {
		kv := strings.Split(item, "=")
		if len(kv) != 2 {
			return nil
		}
		k := strings.Trim(kv[0], " ")
		v := strings.Trim(kv[1], " ")
		index, err := strconv.Atoi(k)
		if err != nil {
			return nil
		}
		res[index] = v
	}

	return res
}
