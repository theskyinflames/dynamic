package domain

import (
	"context"
	"fmt"
	"sync"
)

/// Javascript tasks ? https://github.com/rogchap/v8go

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
type Job func(context.Context, *sync.WaitGroup, JobIn, JobOut)

// Worker execute jobs
type Worker struct {
	name string
	out  JobOut
	job  Job
}

// Run runs the worker job
func (w Worker) Run(ctx context.Context, wg *sync.WaitGroup, in <-chan Param, s Sync) {
	wg.Add(1)
	go func() {
	ends:
		for {
			select {
			case <-s:
				w.job(ctx, wg, in, w.out)
				break ends
			case <-ctx.Done():
				break ends
			}
		}
		wg.Done()
		fmt.Println("finish worker", w.name)
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
func NewWorker(name string, job Job, opts ...WorkerOpt) Worker {
	w := Worker{
		name: name,
		out:  make(chan Param),
		job:  job,
	}

	for _, opt := range opts {
		opt(&w)
	}

	return w
}

// NewJoinWorker is a constructor
// See this article https://medium.com/justforfunc/two-ways-of-merging-n-channels-in-go-43c0b57cd1de
func NewJoinWorker(name string, ws []Worker) Worker {
	out := make(chan Param)
	var job Job = func(ctx context.Context, wg *sync.WaitGroup, _ JobIn, _ JobOut) {
		for i := range ws {
			w := ws[i]
			wg.Add(1)
			go func(c JobOut) {
				fmt.Println("start reading join worker", w.name)
				for {
					select {
					case <-ctx.Done():
						wg.Done()
						fmt.Println("stop reading join worker", w.name)
						return
					case v := <-c:
						out <- v
						fmt.Println("join sends: ", fmt.Sprint(v.Value))
					}
				}

			}(w.Out())
		}
		wg.Wait()
		fmt.Println("join job ends", name)
	}
	return NewWorker(name, job, OutOpt(out))
}
