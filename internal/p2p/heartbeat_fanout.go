package p2p

import (
	"context"
	"sync"
)

const heartbeatFanoutConcurrency = 16

type heartbeatFanoutJob func(context.Context)

// runHeartbeatFanout runs context-aware heartbeat jobs with a hard concurrency
// bound. All jobs share the caller's deadline so one unreachable peer cannot
// impose its full timeout serially on every peer that follows it.
func runHeartbeatFanout(ctx context.Context, concurrency int, jobs []heartbeatFanoutJob) {
	if len(jobs) == 0 || ctx.Err() != nil {
		return
	}
	if concurrency < 1 {
		concurrency = 1
	}
	if concurrency > len(jobs) {
		concurrency = len(jobs)
	}

	work := make(chan heartbeatFanoutJob)
	var workers sync.WaitGroup
	workers.Add(concurrency)
	for range concurrency {
		go func() {
			defer workers.Done()
			for {
				if ctx.Err() != nil {
					return
				}
				select {
				case <-ctx.Done():
					return
				case job, ok := <-work:
					if !ok {
						return
					}
					if ctx.Err() != nil {
						return
					}
					job(ctx)
				}
			}
		}()
	}

sendJobs:
	for _, job := range jobs {
		select {
		case <-ctx.Done():
			break sendJobs
		case work <- job:
		}
	}
	close(work)
	workers.Wait()
}
