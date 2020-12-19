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

func TestLinearFlow(t *testing.T) {
	require := require.New(t)

	t.Run(`Given a workflow with two sequential jobs,
	when the work runs,
	then all works well`, func(t *testing.T) {
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
		w1In := make(chan domain.Param, 1)
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
		w2 := domain.NewWorker(job2, domain.JobIn(w1.Out()), domain.NameOpt("w2"))

		ctx, cancelFunc := context.WithCancel(context.Background())
		flow := domain.NewFlow(ctx)
		flow.AddWorker(w1)
		flow.AddWorker(w2)
		require.NoError(flow.WakeUpWorkers())
		flow.Run()

		fmt.Printf("w1In: %#v\n", w1In)
		fmt.Printf("w2In: %#v\n", w2.Out())

		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case p := <-w2.Out():
					fmt.Printf("received out %f", p.Value.(float64))
				}
			}
		}()

		for _, value := range []domain.Param{
			{Value: float64(10)},
			{Value: float64(32)},
			{Value: math.MaxFloat64},
			{Value: float64(999999)},
			{Value: float64(-45)},
			{Value: float64(100)},
		} {
			fmt.Printf("test to send to w1 %#v\n", w1In)
			w1In <- value
			fmt.Printf("test sent to w1 %#v\n", w1In)
		}

		time.Sleep(time.Second)
		cancelFunc()
		flow.Kill()
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
		w1In := make(chan domain.Param, 1)
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
		w2In := make(chan domain.Param, 2)
		w2 := domain.NewWorker(job2, w2In, domain.NameOpt("w2"))

		join1 := domain.NewJoinWorker([]domain.Worker{w1, w2}, domain.NameOpt("join1"))

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

		flow.AddWorker(w1)
		flow.AddWorker(w2)
		flow.AddWorker(join1)
		flow.AddWorker(w3)
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
				fmt.Printf("test sent to w1 %#v\n", w1In)
				w1In <- values[i]
			} else {
				fmt.Printf("test sent to w2 %#v\n", w1In)
				w2In <- values[i]
			}
		}

		time.Sleep(time.Second)
		cancelFunc()
		flow.Kill()

		require.Equal(math.MaxFloat64, max)
	})
}
