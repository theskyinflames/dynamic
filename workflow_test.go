package main_test

import (
	"errors"
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

// * --> JOB1() -->	|
//             		|--> JOB3() --> x --> (output consumer)
// * --> JOB2() -->	|
//             |
//             ---------> JOB4() --> x --> (output consumer2)
func TestFlowWithJoinAndSplit(t *testing.T) {
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

// In this example, JOB1 takes a list of numbers and send them to JOB1 and JOB2.
// But, with a constraint. JOB2 only accumulates odd numbers, and JOB3 only even ones.
// These two jobs add all receiving numbers and output their totals every time they
// receive a new number and hence, their total changes.
// The JOB4 receives the totals from JOB2 and JOB3, adds them and says if the
// addition is equal to addition of all numbers of the list.
// The flow ends when JOB4 receives all numbers of the list.
//
//               --- odd  --> JOB2() --|
// * --> JOB1() -|                     |--> JOB4() --> (output consumer)
//               --- even --> JOB3() --|
//
func TestJobInputFilter(t *testing.T) {
	require := require.New(t)

	job1 := func(ctx context.Context, postman main.Postman) {
		var (
			p   *main.Param
			err error
		)
		// blocking receive
		for p == nil {
			p, err = postman.Receive(ctx)
			if err != nil {
				require.True(errors.Is(err, main.ErrFlowKilled))
				return
			}
		}

		for !postman.Send(ctx, *p) {
			// blocking until the value is sent
		}
	}

	job2 := func(ctx context.Context, postman main.Postman) {
		var (
			p     *main.Param
			err   error
			total int
		)
		// blocking receive
		for p == nil {
			p, err = postman.Receive(ctx)
			if err != nil {
				require.True(errors.Is(err, main.ErrFlowKilled))
				return
			}
		}
		require.NotZero(p.Value.(int) % 2)

		total += p.Value.(int)

		for !postman.Send(ctx, main.Param{Value: total}) {
			// blocking until the total is sent
		}
	}

	job3 := func(ctx context.Context, postman main.Postman) {
		var (
			p     *main.Param
			err   error
			total int
		)
		// blocking receive
		for p == nil {
			p, err = postman.Receive(ctx)
			if err != nil {
				require.True(errors.Is(err, main.ErrFlowKilled))
				return
			}
		}
		require.Zero(p.Value.(int) % 2)

		total += p.Value.(int)

		for !postman.Send(ctx, main.Param{Value: total}) {
			// blocking until the total is sent
		}
	}

	// This job receives the accumulators from jobs 2 and 3
	// and adds them. When it has read all the numbers of the list,
	// outputs the total
	var (
		outputChan    = make(chan bool)
		values        = []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}
		allValuesRead = make(chan struct{})
		readValues    int
		total         int
	)
	job4 := func(ctx context.Context, postman main.Postman) {
		var (
			received *main.Param
			err      error
		)
		const expectedAmount = 210

		// blocking receive
		for received == nil {
			received, err = postman.Receive(ctx)
			if err != nil {
				require.True(errors.Is(err, main.ErrFlowKilled))
				return
			}
		}
		total += received.Value.(int)
		readValues++
		if readValues == len(values) {
			outputChan <- (total == expectedAmount)
			close(allValuesRead)
		}
	}

	w1InChan := make(chan main.Param)
	worker1 := main.NewWorker(
		nil,
		job1,
		main.NameWOpt("w1"),
		main.AddOrphanInWOpt(w1InChan),
		main.AddInfoFuncWOpt(main.Infof),
		main.ErrHndFuncWOpt(errHndFunc),
	)

	worker2 := main.NewWorker(
		[]*main.Worker{&worker1},
		job2,
		main.NameWOpt("w2"),
		main.AddInFilterFuncWOpt(func(p main.Param) bool {
			return p.Value.(int)%2 != 0
		}),
		main.AddInfoFuncWOpt(main.Infof),
		main.ErrHndFuncWOpt(errHndFunc),
	)

	worker3 := main.NewWorker(
		[]*main.Worker{&worker1},
		job3,
		main.NameWOpt("w3"),
		main.AddInFilterFuncWOpt(func(p main.Param) bool {
			return p.Value.(int)%2 == 0
		}),
		main.AddInfoFuncWOpt(main.Infof),
		main.ErrHndFuncWOpt(errHndFunc),
	)

	worker4 := main.NewWorker(
		[]*main.Worker{&worker2, &worker3},
		job4,
		main.NameWOpt("w4"),
		main.AddInfoFuncWOpt(main.Infof),
		main.ErrHndFuncWOpt(errHndFunc),
	)

	// create the flow
	ctx, cancelFunc := context.WithCancel(context.Background())
	flow := main.NewFlow(ctx, main.AddInfoFuncFOpt(main.Infof))

	// Add workers to the workflow
	flow.AddWorker(worker1)
	flow.AddWorker(worker2)
	flow.AddWorker(worker3)
	flow.AddWorker(worker4)

	// Start the workflow
	flow.Run()

	// Start the flow output consumer
	var amountOK bool
	consumerFinished := make(chan struct{})
	go func() {
		for {
			select {
			case <-ctx.Done():
				close(consumerFinished)
				return
			case amountOK = <-outputChan:
			}
		}
	}()

	// Start to feed the flow
	for _, v := range values {
		w1InChan <- main.Param{Value: v}
	}

	<-allValuesRead
	flow.Kill()
	cancelFunc()
	<-consumerFinished

	require.True(amountOK)
}
