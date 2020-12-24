package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"

	uuid "github.com/satori/go.uuid"
)

// InfoFunc is a tracing function
type InfoFunc func(format string, args ...interface{})

var (
	buf    bytes.Buffer
	logger = log.New(os.Stdout, "INFO: ", log.Lshortfile)

	// TracingChan is a tracing chan
	TracingChan = make(chan string)

	// Infof is a basic logging function for debug purposes
	Infof = func(format string, args ...interface{}) {
		TracingChan <- fmt.Sprintf(format, args...)
	}
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
	uuid       uuid.UUID
	name       string
	out        []JobOut
	in         []JobIn
	squashedIn JobIn
	job        Job

	errHndFunc ErrHndFunc
	infoFunc   InfoFunc
}

// WorkerOpt  is a constructor option
type WorkerOpt func(*Worker)

// NameWOpt is an option
func NameWOpt(name string) WorkerOpt {
	return func(w *Worker) {
		w.name = name
	}
}

// ErrHndFuncWOpt is an option
func ErrHndFuncWOpt(errHndFunc ErrHndFunc) WorkerOpt {
	return func(w *Worker) {
		w.errHndFunc = errHndFunc
	}
}

// AddOrphanInWOpt is an option
func AddOrphanInWOpt(in JobIn) WorkerOpt {
	return func(w *Worker) {
		w.in = append(w.in, in)
	}
}

// AddInfoFuncWOpt is an option
func AddInfoFuncWOpt(infoFunc InfoFunc) WorkerOpt {
	return func(w *Worker) {
		w.infoFunc = infoFunc
	}
}

// NewWorker is a constructor
func NewWorker(parents []*Worker, job Job, opts ...WorkerOpt) Worker {

	w := Worker{
		uuid:       uuid.NewV4(),
		job:        job,
		errHndFunc: func(error) {},
		infoFunc:   func(string, ...interface{}) {},
	}
	w.in = make([]JobIn, len(parents))
	for _, p := range parents {
		w.in = append(w.in, JobIn(p.plugin(w)))
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

func (w *Worker) plugin(child Worker) JobOut {
	childOut := make(JobOut)
	w.out = append(w.out, childOut)
	return childOut
}

func (w *Worker) wakeUp(ctx context.Context, finishedChan chan struct{}) {
	go func() {
		w.receivingLoop(ctx)
		w.infoFunc("woker %s up and running", w.name)
	ends:
		for {
			select {
			case <-ctx.Done():
				break ends
			default:
				w.job(ctx, w)
			}
		}
		w.infoFunc("worker %s finished", w.name)
		finishedChan <- struct{}{}
	}()
}

const errMsg = "receive: %w"

var errNoParents = errors.New("there is not any parents")

func (w *Worker) receivingLoop(ctx context.Context) {
	w.squashedIn = make(JobIn, len(w.in))
	for i := range w.in {
		go func(in, squashedIn JobIn) {
			for {
				select {
				case <-ctx.Done():
					return
				case p, ok := <-in:
					if !ok {
						return
					}
					squashedIn <- p
				}
			}
		}(w.in[i], w.squashedIn)
	}
}

// Receive is self described
func (w Worker) Receive(ctx context.Context) (*Param, error) {
	if len(w.in) == 0 {
		return nil, fmt.Errorf(errMsg, errNoParents)
	}

	var (
		received Param
		ok       bool
	)
	select {
	case <-ctx.Done():
		return nil, nil
	case received, ok = <-w.squashedIn:
		//nothing to do here
	}
	if !ok {
		err := fmt.Errorf("try to read from a closed in chan (%s,%s)", w.name, w.uuid.String())
		w.errHndFunc(err)
		return nil, err
	}
	return &received, nil
}

// Send is self described
func (w Worker) Send(ctx context.Context, p Param) bool {
	var sent int64
	wg := &sync.WaitGroup{}
	for _, out := range w.out {
		wg.Add(1)
		go func(out JobOut) {
			defer wg.Done()
			select {
			case <-ctx.Done():
				return
			default:
				out <- p
				atomic.AddInt64(&sent, 1)
				return
			}
		}(out)
	}
	wg.Wait()
	return int(sent) == len(w.out)
}

var defaultErrHndFunc = func(err error) {
	fmt.Printf("ERR: %s", err.Error())
}

// Flow is a graph of workers
type Flow struct {
	workers      map[uuid.UUID]*Worker
	cf           context.CancelFunc
	ctx          context.Context
	startChan    chan struct{}
	finishedChan chan struct{}

	errHndFunc ErrHndFunc
	infoFunc   InfoFunc
}

// FlowOpt is a constructor option
type FlowOpt func(f *Flow)

// AddInfoFuncFOpt is an option
func AddInfoFuncFOpt(infoFunc InfoFunc) FlowOpt {
	return func(f *Flow) {
		f.infoFunc = infoFunc
	}
}

// NewFlow is a constructor
func NewFlow(ctx context.Context, opts ...FlowOpt) Flow {
	ctx, cf := context.WithCancel(ctx)
	f := Flow{
		workers:      make(map[uuid.UUID]*Worker),
		cf:           cf,
		ctx:          ctx,
		finishedChan: make(chan struct{}),

		errHndFunc: func(error) {},
		infoFunc:   func(string, ...interface{}) {},
	}

	for _, opt := range opts {
		opt(&f)
	}

	return f
}

// AddWorker is self described
func (f Flow) AddWorker(w Worker) {
	f.workers[w.uuid] = &w
	f.infoFunc("added worker %s", w.name)
}

// Run is self described
func (f Flow) Run() {
	for uuid := range f.workers {
		f.workers[uuid].wakeUp(f.ctx, f.finishedChan)
	}
	f.infoFunc("flow started and running")
}

// Kill ends the flow
func (f Flow) Kill() {
	f.cf()
	// wait for all workers finish
	for range f.workers {
		<-f.finishedChan
	}
	f.infoFunc("flow stopped")
}
