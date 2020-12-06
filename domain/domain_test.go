package domain_test

import (
	"fmt"
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
		job1 := func(_ context.Context, _ *sync.WaitGroup, _ domain.JobIn, out domain.JobOut) {
			out <- domain.Param{
				Value: []float64{minor, major},
			}
		}
		w1 := domain.NewWorker("w1", job1)

		job2 := func(_ context.Context, _ *sync.WaitGroup, in domain.JobIn, out domain.JobOut) {
			values := (<-in).Value.([]float64)
			out <- domain.Param{
				Value: math.Max(values[0], values[1]),
			}
		}
		w2 := domain.NewWorker("w2", job2)

		start := make(chan struct{})
		wg := &sync.WaitGroup{}
		ctx := context.Background()

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
		job1 := func(_ context.Context, _ *sync.WaitGroup, _ domain.JobIn, out domain.JobOut) {
			<-c
			atomic.AddInt64(&count, 1)
		}
		w1 := domain.NewWorker("w1", job1)

		job2 := func(_ context.Context, _ *sync.WaitGroup, in domain.JobIn, out domain.JobOut) {
			<-c
			atomic.AddInt64(&count, 1)
		}
		w2 := domain.NewWorker("w2", job2)

		start := make(chan struct{})
		wg := &sync.WaitGroup{}
		ctx, cancelFunc := context.WithCancel(context.Background())

		w1.Run(ctx, wg, nil, start)
		w2.Run(ctx, wg, w1.Out(), start)
		cancelFunc()
		wg.Wait()
		close(c)

		require.Zero(count)
	})
}

// * --> JOB1() -->	|
//             		| --> JOB3() --> x
// * --> JOB2() -->	|
func TestContinuousJoin(t *testing.T) {
	require := require.New(t)

	t.Run(`Given two parallel jobs that joins to a third job,
	when the workflow runs, 
	then the third job does not start until the two before start too`, func(t *testing.T) {
		job1 := func(ctx context.Context, _ *sync.WaitGroup, in domain.JobIn, out domain.JobOut) {
			for {
				select {
				case <-ctx.Done():
					return
				case v := <-in:
					out <- v
				}
			}
		}
		w1 := domain.NewWorker("w1", job1)

		job2 := func(ctx context.Context, _ *sync.WaitGroup, in domain.JobIn, out domain.JobOut) {
			for {
				select {
				case <-ctx.Done():
					return
				case v := <-in:
					out <- v
				}
			}
		}
		w2 := domain.NewWorker("w2", job2)

		job3 := func(ctx context.Context, _ *sync.WaitGroup, in domain.JobIn, out domain.JobOut) {
			max := float64(math.MinInt64)
			for {
				select {
				case <-ctx.Done():
					return
				case param := <-in:
					n := param.Value.(float64)
					if n > max {
						max = n
					}
					out <- domain.Param{Value: max}
				}
			}
		}
		w3 := domain.NewWorker("w3", job3)

		blockedJoinW := domain.NewJoinWorker("loop join", []domain.Worker{w1, w2})
		start := make(chan struct{})

		ctx, cancelFunc := context.WithCancel(context.Background())
		wg := &sync.WaitGroup{}

		w1In := make(chan domain.Param)
		w1.Run(ctx, wg, w1In, start)
		w2In := make(chan domain.Param)
		w2.Run(ctx, wg, w2In, start)

		blockedJoinW.Run(ctx, wg, nil, start)

		w3.Run(ctx, wg, blockedJoinW.Out(), start)

		close(start)

		values := []domain.Param{
			{Value: float64(10)},
			{Value: float64(32)},
			{Value: math.MaxFloat64},
			{Value: float64(999999)},
			{Value: float64(-45)},
			{Value: float64(100)},
		}

		var max domain.Param
		allReadChan := make(chan struct{})
		go func() {
			i := 0
			for {
				select {
				case <-ctx.Done():
					fmt.Println("finish test job")
					return
				case max = <-w3.Out():
					i++
					if i == 2 { // FINISH BEFORE READ ALL NUMBERS --------------->>> FAILS!!!! TODO
						fmt.Println("read all values, max:", fmt.Sprint(max))
						close(allReadChan)
					}
				}
			}
		}()

		for i := range values {
			if i%2 == 0 {
				w1In <- values[i]
			} else {
				w2In <- values[i]
			}
		}

		<-allReadChan
		cancelFunc()
		wg.Wait()

		require.Equal(math.MaxFloat64, max.Value)
	})
}
