package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	uuid "github.com/satori/go.uuid"
)

// Param is a worker in/out param
type Param struct {
	Err   error
	Name  string
	Value interface{}
}

// Sync is a sync flag
type Sync <-chan struct{}

// JobIn is self described
type JobIn chan Param

// JobOut is self described
type JobOut chan Param

// Postman is self described
type Postman interface {
	Receive(ctx context.Context) (*Param, error)
	Send(ctx context.Context, p Param) bool
}

// Job is a worker job
type Job func(context.Context, Postman)

// ErrHndFunc is an error handler
type ErrHndFunc func(error)

// Worker execute jobs
type Worker struct {
	uuid uuid.UUID
	name string
	out  JobOut
	in   JobIn
	job  Job

	errHndFunc ErrHndFunc
}

// WorkerOpt  is an constructor option
type WorkerOpt func(*Worker)

// NameOpt is an option
func NameOpt(name string) WorkerOpt {
	return func(w *Worker) {
		w.name = name
	}
}

// ErrHndFuncOpt is an option
func ErrHndFuncOpt(errHndFunc ErrHndFunc) WorkerOpt {
	return func(w *Worker) {
		w.errHndFunc = errHndFunc
	}
}

// NewWorker is a constructor
func NewWorker(job Job, in JobIn, opts ...WorkerOpt) Worker {
	w := Worker{
		uuid: uuid.NewV4(),
		out:  make(chan Param),
		job:  job,
		in:   in,
	}

	for _, opt := range opts {
		opt(&w)
	}

	return w
}

// Name is a getter
func (w Worker) Name() string {
	return w.name
}

// Out is a Getter
func (w Worker) Out() JobOut {
	return w.out
}

// Receive is self described
func (w Worker) Receive(ctx context.Context) (*Param, error) {
	var (
		p   *Param
		err error
	)
	ctx, cancelFunc := context.WithTimeout(ctx, time.Duration(10*time.Millisecond))
	defer cancelFunc()
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case received, ok := <-w.in:
				if !ok {
					err = fmt.Errorf("try to read from a closed in chan (%s,%s)", w.name, w.uuid.String())
					return
				}
				p = &received
				return
			}
		}
	}()
	wg.Wait()
	return p, err
}

// Send is self described
func (w Worker) Send(ctx context.Context, p Param) bool {
	var sent bool
	ctx, cancelFunc := context.WithTimeout(ctx, time.Duration(10*time.Millisecond))
	defer cancelFunc()
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if len(w.out) == 0 {
					w.out <- p
					sent = true
					return
				}
			}
		}
	}()
	wg.Wait()
	return sent
}

func (w Worker) wakeUp(ctx context.Context, s Sync) {
	go func() {
		fmt.Printf("start worker (%s)(%s)\n", w.name, w.uuid.String())
	ends:
		for {
			select {
			case <-ctx.Done():
				break ends
			case <-s:
				w.job(ctx, &w)
				break ends
			}
		}
		fmt.Printf("finish worker (%s)(%s)\n", w.name, w.uuid.String())
	}()
}

// NewJoinWorker is a constructor
// See this article https://medium.com/justforfunc/two-ways-of-merging-n-channels-in-go-43c0b57cd1de
func NewJoinWorker(ws []Worker, opts ...WorkerOpt) Worker {
	job := func(ctx context.Context, postman Postman) {
		for i := range ws {
			w := ws[i]
			go func(w Worker, postman Postman) {
				for {
					select {
					case <-ctx.Done():
						return
					default:
						v, ok := <-w.Out()
						if !ok {
							fmt.Println(fmt.Printf("join: channel worker %s is closed\n", w.name))
							return
						}
						postman.Send(ctx, v)
					}
				}
			}(w, postman)
		}
	}
	return NewWorker(job, nil, opts...)
}

// Flow is a graph of workers
type Flow struct {
	workers      map[uuid.UUID]Worker
	cf           context.CancelFunc
	ctx          context.Context
	startChan    chan struct{}
	finishedChan chan struct{}

	errHndFunc ErrHndFunc
}

// FlowOpt is Flow option
type FlowOpt func(*Flow)

// ErrHndFuncOptFlow is a error handler flow option
func ErrHndFuncOptFlow(errHndFunc ErrHndFunc) FlowOpt {
	return func(f *Flow) {
		f.errHndFunc = errHndFunc
	}
}

// NewFlow is a constructor
func NewFlow(ctx context.Context, opts ...FlowOpt) Flow {
	ctx, cf := context.WithCancel(ctx)
	f := Flow{
		workers:      make(map[uuid.UUID]Worker),
		cf:           cf,
		ctx:          ctx,
		startChan:    make(chan struct{}),
		finishedChan: make(chan struct{}),
	}

	for _, opt := range opts {
		opt(&f)
	}

	return f
}

// AddWorker is self described
func (f Flow) AddWorker(w Worker) {
	f.workers[w.uuid] = w
}

// WakeUpWorkers is self described
func (f Flow) WakeUpWorkers() {
	for uuid := range f.workers {
		f.workers[uuid].wakeUp(f.ctx, f.startChan)
	}
	return
}

// Run is self described
func (f Flow) Run() {
	f.WakeUpWorkers()
	go func() {
		defer func() {
			fmt.Println("finish flow loop")
		}()
		fmt.Println("start flow loop")
		for {
			select {
			case <-f.ctx.Done():
				close(f.finishedChan)
				return
			default:
			}
		}
	}()
	close(f.startChan)
}

// Kill ends the flow
func (f Flow) Kill() {
	f.cf()
	<-f.finishedChan
}