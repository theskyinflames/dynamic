package domain

import (
	"context"
	"sync"
)

/// Javascript https://github.com/rogchap/v8go

// Param is a worker in/out param
type Param struct {
	Err   error
	Name  string
	Value interface{}
}

// Sync is a sync flag
type Sync <-chan struct{}

// JobIn is self described
type JobIn <-chan Param

// JobOut is self described
type JobOut chan Param

// Job is a worker job
type Job func(JobIn, JobOut)

// Worker execute jobs
type Worker struct {
	out JobOut
	job Job
}

// Run runs the worker job
func (w Worker) Run(ctx context.Context, wg *sync.WaitGroup, in <-chan Param, s Sync) {
	wg.Add(1)
	go func() {
	ends:
		for {
			select {
			case <-s:
				w.job(in, w.out)
				break ends
			case <-ctx.Done():
				break ends
			}
		}
		wg.Done()
	}()
}

// Out is a Getter
func (w Worker) Out() JobOut {
	return w.out
}

// WorkerOpt is a worker option
type WorkerOpt func(w *Worker)

// OutOpt is a JobOut option
func OutOpt(out JobOut) WorkerOpt {
	return func(w *Worker) {
		w.out = out
	}
}

// NewWorker is a constructor
func NewWorker(job Job, opts ...WorkerOpt) Worker {
	w := Worker{
		out: make(chan Param),
		job: job,
	}

	for _, opt := range opts {
		opt(&w)
	}

	return w
}

// NewBlockerJoinWorker is a constructor
// See this article https://medium.com/justforfunc/two-ways-of-merging-n-channels-in-go-43c0b57cd1de
func NewBlockerJoinWorker(ws []Worker) Worker {
	out := make(chan Param)
	var job Job = func(in JobIn, _ JobOut) {
		wg := sync.WaitGroup{}
		wg.Add(len(ws))
		for _, w := range ws {
			go func(c JobOut) {
				for param := range c {
					out <- param
				}
			}(w.Out())
		}
		wg.Wait()
	}
	return NewWorker(job, OutOpt(out))
}
