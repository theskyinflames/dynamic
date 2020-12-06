package domain_test

import (
	"math"
	"sync"
	"sync/atomic"
	"testing"

	"context"

	"github.com/stretchr/testify/require"
	"github.com/theskyinflames/dynamic-go/domain"
)

// *-->JOB1()-->JOB2()-->x
func TestLinearFlow(t *testing.T) {
	require := require.New(t)

	t.Run(`Given two sequential jobs, the first one returns two numbers, and the second one select the greather of them,
	when they are added to a workflow and run it,
	then the greather number is get`, func(t *testing.T) {
		minor := float64(5)
		major := float64(10)
		job1 := func(_ domain.JobIn, out domain.JobOut) {
			out <- domain.Param{
				Value: []float64{minor, major},
			}
		}
		w1 := domain.NewWorker(job1)

		job2 := func(in domain.JobIn, out domain.JobOut) {
			values := (<-in).Value.([]float64)
			out <- domain.Param{
				Value: math.Max(values[0], values[1]),
			}
		}
		w2 := domain.NewWorker(job2)

		start := make(chan struct{})
		ctx := context.Background()
		wg := &sync.WaitGroup{}
		w1.Run(ctx, wg, nil, start)
		w2.Run(ctx, wg, w1.Out(), start)
		close(start)
		max := <-w2.Out()
		wg.Wait()

		require.Equal(major, max.Value)
	})

	t.Run(`Given two sequential jobs, 
	when they are added to a workflow, run it, and the context is cancelled,
	then the two jobs finishes`, func(t *testing.T) {
		c := make(chan struct{})
		var count int64
		job1 := func(_ domain.JobIn, out domain.JobOut) {
			<-c
			atomic.AddInt64(&count, 1)
		}
		w1 := domain.NewWorker(job1)

		job2 := func(in domain.JobIn, out domain.JobOut) {
			<-c
			atomic.AddInt64(&count, 1)
		}
		w2 := domain.NewWorker(job2)

		start := make(chan struct{})
		ctx, cancelFunc := context.WithCancel(context.Background())
		wg := &sync.WaitGroup{}
		w1.Run(ctx, wg, nil, start)
		w2.Run(ctx, wg, w1.Out(), start)
		cancelFunc()
		wg.Wait()
		close(c)
		require.Zero(count)
	})
}
