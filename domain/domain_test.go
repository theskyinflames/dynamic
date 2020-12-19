package domain_test

import (
	"fmt"
	"math"
	"testing"

	"context"

	"github.com/stretchr/testify/require"
	"github.com/theskyinflames/dynamic-go/domain"
)

// * --> JOB1() --> JOB2() --> x --> (output consumer)
func TestLinearFlow(t *testing.T) {
	require := require.New(t)

	t.Run(`Given a workflow with two sequential jobs,
	when the work runs,
	then all works well`, func(t *testing.T) {

		// Create the workflow workers
		job1 := func(ctx context.Context, postman domain.Postman) {
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

		// Create the workflow and add the workers
		ctx, cancelFunc := context.WithCancel(context.Background())
		flow := domain.NewFlow(ctx)
		flow.AddWorker(w1)
		flow.AddWorker(w2)

		// Start workflow workers
		require.NoError(flow.WakeUpWorkers())
		flow.Run()

		// Fixture data to feed the workflow
		values := []domain.Param{
			{Value: float64(10)},
			{Value: float64(32)},
			{Value: math.MaxFloat64},
			{Value: float64(999999)},
			{Value: float64(-45)},
			{Value: float64(100)},
		}

		// This goroutine implements a consumer for the workflow output
		var (
			i                     = 0
			allValuesReceivedChan = make(chan struct{})
		)
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case <-w2.Out():
					i++
					if i == len(values) {
						close(allValuesReceivedChan)
					}
				}
			}
		}()

		// Feed the workflow
		for _, value := range values {
			w1In <- value
		}

		// Wait for all fixtures processed
		<-allValuesReceivedChan

		// Finish the workflow
		cancelFunc()
		flow.Kill()
	})
}

// * --> JOB1() -->	|
//             		| --> JOB3() --> x --> (output consumer)
// * --> JOB2() -->	|
func TestJoin(t *testing.T) {
	require := require.New(t)

	t.Run(`Given two parallel jobs that joins to a third job,
	when the workflow runs, 
	then the third job does not start until the two before start too`, func(t *testing.T) {

		// Create workers
		job1 := func(ctx context.Context, postman domain.Postman) {
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

		// This an special worker that acts like a join in the workflow
		join1 := domain.NewJoinWorker([]domain.Worker{w1, w2}, domain.NameOpt("join1"))

		job3 := func(ctx context.Context, postman domain.Postman) {
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

		// create the flow
		ctx, cancelFunc := context.WithCancel(context.Background())
		flow := domain.NewFlow(ctx)

		// Add workers to the workflow
		flow.AddWorker(w1)
		flow.AddWorker(w2)
		flow.AddWorker(join1)
		flow.AddWorker(w3)

		// Start the workflow jobs
		require.NoError(flow.WakeUpWorkers())
		flow.Run()

		// Fixtures to feed the workflow
		values := []domain.Param{
			{Value: float64(10)},
			{Value: float64(32)},
			{Value: math.MaxFloat64},
			{Value: float64(999999)},
			{Value: float64(-45)},
			{Value: float64(100)},
		}

		// Start the workflow output consumer
		var (
			p                     domain.Param
			max                   float64
			ok                    bool
			i                     = 0
			allValuesReceivedChan = make(chan struct{})
		)
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case p, ok = <-w3.Out():
					if !ok {
						return
					}
					max = p.Value.(float64)
					i++
					if i == len(values) {
						// force the end of the workflow
						close(allValuesReceivedChan)
					}
				}
			}
		}()

		// Feed the workflow
		for i := range values {
			if i%2 == 0 {
				w1In <- values[i]
			} else {
				w2In <- values[i]
			}
		}

		// Wait for all fixture data has been read
		<-allValuesReceivedChan

		// Finish the workflow
		cancelFunc()
		flow.Kill()

		require.Equal(math.MaxFloat64, max)
	})
}
