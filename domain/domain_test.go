package domain_test

import (
	"fmt"
	"math"
	"testing"
	"time"

	"context"

	"github.com/stretchr/testify/require"
	"github.com/theskyinflames/dynamic-go/domain"
)

// *-->JOB1()-->JOB2()-->x
/*
func TestLinearFlow(t *testing.T) {
	require := require.New(t)

	t.Run(`Given two sequential jobs, the first one returns two numbers, and the second one select the greather of them,
	when they are added to a workflow and run it,
	then the greather number is get`, func(t *testing.T) {
		minor := float64(5)
		major := float64(10)
		job1 := func(_ context.Context, _ domain.JobIn, out domain.JobOut) {
			out <- domain.Param{
				Value: []float64{minor, major},
			}
		}
		w1 := domain.NewWorker("w1", job1)

		job2 := func(_ context.Context, in domain.JobIn, out domain.JobOut) {
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
		job1 := func(_ context.Context, _ domain.JobIn, _ domain.JobOut) {
			<-c
			atomic.AddInt64(&count, 1)
		}
		w1 := domain.NewWorker("w1", job1)

		job2 := func(_ context.Context, _ domain.JobIn, _ domain.JobOut) {
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

*/

// * --> JOB1() -->	|
//             		| --> JOB3() --> x
// * --> JOB2() -->	|
func TestContinuousJoin(t *testing.T) {
	require := require.New(t)

	t.Run(`Given two parallel jobs that joins to a third job,
	when the workflow runs, 
	then the third job does not start until the two before start too`, func(t *testing.T) {
		job1 := func(ctx context.Context, postman domain.Postman) {
			fmt.Printf("job %s started\n", "w1")
			for {
				select {
				case <-ctx.Done():
					return
				default:
					v, err := postman.Receive(ctx)
					fmt.Printf("job %s received %#v\n", "w1", v)
					if err != nil {
						fmt.Println(err.Error())
						return
					}
					if v != nil {
						for {
							if postman.Send(ctx, *v) {
								break
							}
						}
					}
				}
			}
		}
		w1In := make(chan domain.Param)
		w1 := domain.NewWorker(job1, w1In, domain.NameOpt("w1"))

		job2 := func(ctx context.Context, postman domain.Postman) {
			fmt.Printf("job %s started\n", "w2")
			for {
				select {
				case <-ctx.Done():
					return
				default:
					v, err := postman.Receive(ctx)
					fmt.Printf("job %s received %#v\n", "w2", v)
					if err != nil {
						fmt.Println(err.Error())
						return
					}
					if v != nil {
						for {
							if postman.Send(ctx, *v) {
								break
							}
						}
					}
				}
			}
		}
		w2In := make(chan domain.Param)
		w2 := domain.NewWorker(job2, w2In, domain.NameOpt("w2"))

		join1 := domain.NewJoinWorker([]domain.Worker{w1}, domain.NameOpt("join1"))

		job3 := func(ctx context.Context, postman domain.Postman) {
			fmt.Printf("job %s started\n", "w3")
			max := float64(math.MinInt64)
			for {
				select {
				case <-ctx.Done():
					return
				default:
					v, err := postman.Receive(ctx)
					if err != nil {
						fmt.Println(err.Error())
						return
					}
					if v != nil {
						if v.Value == nil {
							fmt.Println("w3 received value nil")
						}
						n := v.Value.(float64)
						if n > max {
							max = n
						}
						p := domain.Param{Value: max}
						for {
							if postman.Send(ctx, p) {
								break
							}
						}
					}
				}
			}
		}
		w3 := domain.NewWorker(job3, domain.JobIn(join1.Out()), domain.NameOpt("w3"))

		ctx, cancelFunc := context.WithCancel(context.Background())
		flow := domain.NewFlow(ctx)

		flow.AddWorker(w1, w1In)
		flow.AddWorker(w2, w2In)
		flow.AddWorker(join1, nil)
		flow.AddWorker(w3, join1.Out())
		require.NoError(flow.WakeUpWorkers())
		flow.Run()

		values := []domain.Param{
			{Value: float64(10)},
			{Value: float64(32)},
			{Value: math.MaxFloat64},
			{Value: float64(999999)},
			{Value: float64(-45)},
			{Value: float64(100)},
		}

		var (
			p   domain.Param
			max float64
			ok  bool
		)
		go func() {
			for {
				select {
				case <-ctx.Done():
					fmt.Println("finish test job")
					return
				case p, ok = <-w3.Out():
					if !ok {
						return
					}
					max = p.Value.(float64)
					fmt.Printf("New max: %f\n", max)
				}
			}
		}()

		fmt.Printf("w1In: %#v\n", w1In)
		fmt.Printf("w2In: %#v\n", w2In)

		for i := range values {
			if i%2 == 0 {
				w1In <- values[i]
			} else {
				w2In <- values[i]
			}
		}

		time.Sleep(time.Second)
		cancelFunc()
		flow.Kill()

		require.Equal(math.MaxFloat64, max)
	})
}
