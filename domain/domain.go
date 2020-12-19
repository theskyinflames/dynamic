package domain

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

// Worker execute jobs
type Worker struct {
	uuid uuid.UUID
	name string
	out  JobOut
	in   JobIn
	job  Job
}

// WorkerOpt  is an constructor option
type WorkerOpt func(*Worker)

// NameOpt is a name option
func NameOpt(name string) WorkerOpt {
	return func(w *Worker) {
		w.name = name
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
				defer func() {
					fmt.Printf("join: stop reading (%s)(%s)\n", w.name, w.uuid.String())
				}()
				fmt.Printf("join: start reading (%s)(%s)\n", w.name, w.uuid.String())
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
						fmt.Println("join sends: ", fmt.Sprint(v.Value))
					}
				}
			}(w, postman)
		}
	}
	return NewWorker(job, nil, opts...)
}

// Flow is a graph of workers
type Flow struct {
	workers   map[uuid.UUID]Worker
	cf        context.CancelFunc
	ctx       context.Context
	startChan chan struct{}
	tickChan  chan uuid.UUID

	finishedChan chan struct{}
}

// NewFlow is a constructor
func NewFlow(ctx context.Context) Flow {
	ctx, cf := context.WithCancel(ctx)
	return Flow{
		workers:      make(map[uuid.UUID]Worker),
		cf:           cf,
		ctx:          ctx,
		startChan:    make(chan struct{}),
		finishedChan: make(chan struct{}),
	}
}

// AddWorker is self described
func (f Flow) AddWorker(w Worker) {
	fmt.Printf("Adding worker %s\n (%s) with in %#v\n", w.uuid.String(), w.name, w.in)
	f.workers[w.uuid] = w
}

// WakeUpWorkers is self described
func (f Flow) WakeUpWorkers() error {
	for uuid := range f.workers {
		f.workers[uuid].wakeUp(f.ctx, f.startChan)
	}
	return nil
}

// Run is self described
func (f Flow) Run() {
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
