package dynamic_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	main "github.com/theskyinflames/dynamic"
)

var errHndFunc = func(err error) {
	fmt.Printf("ERR: %s\n", err.Error())
}

// In this example, JOB1 takes a list of numbers and send them to JOB1 and JOB2.
// But, with a constraint. JOB2 only accumulates odd numbers, and JOB3 only even ones.
// These two jobs add all receiving numbers and output their totals every time they
// receive a new number and hence, their local total changes.
// The JOB4 receives the totals from JOB2 and JOB3, adds them and checks if the
// addition is equal to addition of all numbers of the list. If so, the flow finishes.
//
//               --- odd  --> JOB2() --|
// * --> JOB1() -|                     |--> JOB4() --> (output consumer)
//               --- even --> JOB3() --|
//
func TestFlow(t *testing.T) {
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
