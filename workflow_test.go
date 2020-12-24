package main_test

import (
	"fmt"
	"math"
	"sync"
	"testing"

	"context"

	"github.com/stretchr/testify/require"
	main "github.com/theskyinflames/dynamic-go"
)

var (
	errHndFunc = func(err error) {
		fmt.Printf("ERR: %s\n", err.Error())
	}
)

// * --> JOB1() --> JOB2() --> x --> (output consumer)
func TestLinearFlow(t *testing.T) {
	require := require.New(t)

	t.Run(`Given a workflow with two sequential jobs,
	when the work runs,
	then all works well`, func(t *testing.T) {

		// Create the workflow workers
		job1 := func(ctx context.Context, postman main.Postman) {
			v, err := postman.Receive(ctx)
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			if v != nil {
				for !postman.Send(ctx, *v) {
				}
			}
		}
		w1In := make(chan main.Param)
		w1 := main.NewWorker(
			nil,
			job1,
			main.NameWOpt("w1"),
			main.AddOrphanInWOpt(w1In),
			main.ErrHndFuncWOpt(errHndFunc),
			main.AddInfoFuncWOpt(main.Infof),
		)

		outGraphChan := make(chan main.Param)
		job2 := func(ctx context.Context, postman main.Postman) {
			v, err := postman.Receive(ctx)
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			if v != nil {
				outGraphChan <- *v
			}
		}
		w2 := main.NewWorker(
			[]*main.Worker{&w1},
			job2,
			main.NameWOpt("w2"),
			main.ErrHndFuncWOpt(errHndFunc),
			main.AddInfoFuncWOpt(main.Infof),
		)

		// Create the workflow and add the workers
		ctx, cancelFunc := context.WithCancel(context.Background())
		flow := main.NewFlow(ctx, main.AddInfoFuncFOpt(main.Infof))
		flow.AddWorker(w1)
		flow.AddWorker(w2)

		// Start workflow workers
		flow.Run()

		// Fixture data to feed the workflow
		values := []main.Param{
			{Value: float64(10)},
			{Value: float64(32)},
			{Value: math.MaxFloat64},
			{Value: float64(999999)},
			{Value: float64(-45)},
			{Value: float64(100)},
		}

		// This goroutine implements a consumer for the workflow output
		var (
			allValuesReceivedChan = make(chan struct{})
			receivedValues        []main.Param
		)
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case p, ok := <-outGraphChan:
					if !ok {
						return
					}
					receivedValues = append(receivedValues, p)
					if len(receivedValues) == len(values) {
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

		require.Equal(values, receivedValues)
	})
}

// * --> JOB1() -->	|
//             		| --> JOB3() --> x --> (output consumer)
// * --> JOB2() -->	|
func TestFlowWithJoin(t *testing.T) {
	require := require.New(t)

	t.Run(`Given two parallel jobs that joins to a third job,
	when the workflow runs,
	then the workflow outputs the result of the third job`, func(t *testing.T) {

		// Create workers
		job1 := func(ctx context.Context, postman main.Postman) {
			v, err := postman.Receive(ctx)
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			if v != nil {
				for !postman.Send(ctx, *v) {
				}
			}
		}
		w1In := make(chan main.Param)
		w1 := main.NewWorker(
			nil,
			job1,
			main.NameWOpt("w1"),
			main.AddOrphanInWOpt(w1In),
			main.AddInfoFuncWOpt(main.Infof),
			main.ErrHndFuncWOpt(errHndFunc),
		)

		job2 := func(ctx context.Context, postman main.Postman) {
			v, err := postman.Receive(ctx)
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			if v != nil {
				for !postman.Send(ctx, *v) {
				}
			}
		}
		w2In := make(chan main.Param)
		w2 := main.NewWorker(
			nil,
			job2,
			main.NameWOpt("w2"),
			main.AddOrphanInWOpt(w2In),
			main.AddInfoFuncWOpt(main.Infof),
			main.ErrHndFuncWOpt(errHndFunc),
		)

		var job3Max = math.MinInt64
		outJob3 := make(main.JobOut)
		job3 := func(ctx context.Context, postman main.Postman) {
			v, err := postman.Receive(ctx)
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			if v != nil {
				if v.Value == nil {
					fmt.Println("w3 received value nil")
				}
				n := v.Value.(int)
				if n > job3Max {
					job3Max = n
				}
				outJob3 <- main.Param{Value: job3Max}
			}
		}
		w3 := main.NewWorker(
			[]*main.Worker{&w1, &w2},
			job3,
			main.NameWOpt("w3"),
			main.AddInfoFuncWOpt(main.Infof),
			main.ErrHndFuncWOpt(errHndFunc),
		)

		// create the flow
		ctx, cancelFunc := context.WithCancel(context.Background())
		flow := main.NewFlow(ctx, main.AddInfoFuncFOpt(main.Infof))

		// Add workers to the workflow
		flow.AddWorker(w1)
		flow.AddWorker(w2)
		flow.AddWorker(w3)

		// Start the workflow
		flow.Run()

		// Fixtures to feed the workflow
		values := []main.Param{
			{Value: 10},
			{Value: 32},
			{Value: math.MaxInt64},
			{Value: 999999},
			{Value: -45},
			{Value: 100},
		}

		// Start the workflow output consumer
		var (
			p                     main.Param
			max                   int
			ok                    bool
			i                     = 0
			allValuesReceivedChan = make(chan struct{})
		)
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case p, ok = <-outJob3:
					if !ok {
						return
					}
					max = p.Value.(int)
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

		require.Equal(math.MaxInt64, max)
	})
}

// * --> JOB1() -->	|
//             		 >--> JOB3() --> x --> (output consumer)
// * --> JOB2() -->	|
//             |
//             ---------> JOB4() --> x --> (output consumer2)
func TestFlowWithJoinAndSplit(t *testing.T) {
	require := require.New(t)

	// TODO remove this tracing from workfow package
	go func() {
		for s := range main.TracingChan {
			fmt.Println(s)
		}
	}()

	t.Run(`Given two parallel jobs that joins to a third job,
	when the workflow runs,
	then the workflow outputs the result of the third job`, func(t *testing.T) {

		// Create workers
		job1 := func(ctx context.Context, postman main.Postman) {
			v, err := postman.Receive(ctx)
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			if v != nil {
				for !postman.Send(ctx, *v) {
					// retry sending
				}
			}
		}
		w1In := make(chan main.Param)
		w1 := main.NewWorker(
			nil,
			job1,
			main.NameWOpt("w1"),
			main.AddOrphanInWOpt(w1In),
			main.AddInfoFuncWOpt(main.Infof),
			main.ErrHndFuncWOpt(errHndFunc),
		)

		job2 := func(ctx context.Context, postman main.Postman) {
			v, err := postman.Receive(ctx)
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			if v != nil {
				for !postman.Send(ctx, *v) {
					// retry sending
				}
			}
		}
		w2In := make(chan main.Param)
		w2 := main.NewWorker(
			nil,
			job2,
			main.NameWOpt("w2"),
			main.AddOrphanInWOpt(w2In),
			main.AddInfoFuncWOpt(main.Infof),
			main.ErrHndFuncWOpt(errHndFunc),
		)

		var job3Max = math.MinInt64
		outJob3 := make(main.JobOut)
		job3 := func(ctx context.Context, postman main.Postman) {
			v, err := postman.Receive(ctx)
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			if v != nil {
				n := v.Value.(int)
				if n > job3Max {
					job3Max = n
				}
				outJob3 <- main.Param{Value: job3Max}
			}
		}
		w3 := main.NewWorker(
			[]*main.Worker{&w1, &w2},
			job3,
			main.NameWOpt("w3"),
			main.AddInfoFuncWOpt(main.Infof),
			main.ErrHndFuncWOpt(errHndFunc),
		)

		var job4Min = math.MaxInt64
		outJob4 := make(main.JobOut)
		job4 := func(ctx context.Context, postman main.Postman) {
			v, err := postman.Receive(ctx)
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			if v != nil {
				n := v.Value.(int)
				if n < job4Min {
					job4Min = n
				}
				outJob4 <- main.Param{Value: job4Min}
			}
		}
		w4 := main.NewWorker(
			[]*main.Worker{&w2},
			job4,
			main.NameWOpt("w4"),
			main.AddInfoFuncWOpt(main.Infof),
			main.ErrHndFuncWOpt(errHndFunc),
		)

		// create the flow
		ctx, cancelFunc := context.WithCancel(context.Background())
		flow := main.NewFlow(ctx, main.AddInfoFuncFOpt(main.Infof))

		// Add workers to the workflow
		flow.AddWorker(w1)
		flow.AddWorker(w2)
		flow.AddWorker(w3)
		flow.AddWorker(w4)

		// Start the workflow
		flow.Run()

		// Fixtures to feed the workflow
		values := []main.Param{
			{Value: 10},
			{Value: 32},
			{Value: math.MaxInt64},
			{Value: math.MinInt64},
			{Value: 999999},
			{Value: -45},
			{Value: 100},
		}

		// Start the workflow output MAX consumer
		var (
			i   int
			max int
			wg  = sync.WaitGroup{}
		)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				// finishing the consumer when all values have been received
				// this step is only necessary if we do know when finish the flow
				if i == len(values) {
					return
				}
				select {
				case <-ctx.Done():
					return
				case p, ok := <-outJob3:
					if !ok {
						return
					}
					max = p.Value.(int)
					i++
				}
			}
		}()

		// Start the workflow output consumer
		var (
			min  int
			odds = countOdds(len(values))
			i2   = 0
		)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				// finishing the consumer when all values have been received
				// this step is only necessary if we do know when finish the flow
				if i2 == odds {
					// all odd values positions have been readed
					// take account that this job only receives odd values
					return
				}
				select {
				case <-ctx.Done():
					return
				case p, ok := <-outJob4:
					if !ok {
						return
					}
					min = p.Value.(int)
					i2++
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
		wg.Wait()

		// Finish the workflow
		cancelFunc()
		flow.Kill()

		require.Equal(math.MaxInt64, max)
		require.Equal(math.MinInt64, min)
	})
}

func countOdds(n int) int {
	odds := int(n / 2)
	// if either n is odd
	if n%2 == 0 {
		odds++
	}
	return odds
}
