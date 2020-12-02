package domain

import "sync"

/// Javascript https://github.com/rogchap/v8go

// Param is a worker in/out param
type Param struct {
	Err   error
	Name  string
	Value interface{}
}

// Sync is a sync flag
type Sync <-chan struct{}

// JobOut is self described
type JobOut <-chan Param

// Job is a worker job
type Job func(Param, Sync) JobOut

// Worker execute jobs
type Worker struct {
	out JobOut
	job Job
}

// Run runs the worker job
func (w Worker) Run(in Param, s Sync) {
	w.out = w.job(in, s)
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
func NewBlockerJoinWorker(ws []Worker, s Sync) Worker {
	out := make(chan Param)

	var job Job = func(in Param, s Sync) JobOut {
		<-s
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
		return out
	}
	return NewWorker(job, OutOpt(out))
}
