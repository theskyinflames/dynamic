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
// The JOB4 receives the totals from JOB2 and JOB3, adds them and says if the addition is a prime number.
// The flow ends when JOB1 sends all numbers.
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

	// This job receives the accumulators from jobs 2 and 3,
	// and outputs true if the addition of the two ones are a prime number,
	// and outputs false in the opposite
	var (
		outputChan    = make(chan bool)
		values        = []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}
		allValuesRead = make(chan struct{})
		readValues    int
	)
	job4 := func(ctx context.Context, postman main.Postman) {
		var (
			received   *main.Param
			err        error
			primes     = []int{2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71, 73, 79, 83, 89, 97, 101, 103, 107, 109, 113, 127, 131, 137, 139, 149, 151, 157, 163, 167, 173, 179, 181, 191, 193, 197, 199, 211, 223, 227, 229, 233, 239, 241, 251, 257, 263, 269, 271, 277, 281, 283, 293, 307, 311, 313, 317, 331, 337, 347, 349, 353, 359, 367, 373, 379, 383, 389, 397, 401, 409, 419, 421, 431, 433, 439, 443, 449, 457, 461, 463, 467, 479, 487, 491, 499, 503, 509, 521, 523, 541, 547, 557, 563, 569, 571, 577, 587, 593, 599, 601, 607, 613, 617, 619, 631, 641, 643, 647, 653, 659, 661, 673, 677, 683, 691, 701, 709, 719, 727, 733, 739, 743, 751, 757, 761, 769, 773, 787, 797, 809, 811, 821, 823, 827, 829, 839, 853, 857, 859, 863, 877, 881, 883, 887, 907, 911, 919, 929, 937, 941, 947, 953, 967, 971, 977, 983, 991, 997, 1009, 1013, 1019, 1021, 1031, 1033, 1039, 1049, 1051, 1061, 1063, 1069, 1087, 1091, 1093, 1097, 1103, 1109, 1117, 1123, 1129, 1151, 1153, 1163, 1171, 1181, 1187, 1193, 1201, 1213, 1217, 1223, 1229, 1231, 1237, 1249, 1259, 1277, 1279, 1283, 1289, 1291, 1297, 1301, 1303, 1307, 1319, 1321, 1327, 1361, 1367, 1373, 1381, 1399, 1409, 1423, 1427, 1429, 1433, 1439, 1447, 1451, 1453, 1459, 1471, 1481, 1483, 1487, 1489, 1493, 1499, 1511, 1523, 1531, 1543, 1549, 1553, 1559, 1567, 1571, 1579, 1583, 1597, 1601, 1607, 1609, 1613, 1619, 1621, 1627, 1637, 1657, 1663, 1667, 1669, 1693, 1697, 1699, 1709, 1721, 1723, 1733, 1741, 1747, 1753, 1759, 1777, 1783, 1787, 1789, 1801, 1811, 1823, 1831, 1847, 1861, 1867, 1871, 1873, 1877, 1879, 1889, 1901, 1907, 1913, 1931, 1933, 1949, 1951, 1973, 1979, 1987}
			foundPrime bool
			total      int
		)
		// blocking receive
		for received == nil {
			received, err = postman.Receive(ctx)
			if err != nil {
				require.True(errors.Is(err, main.ErrFlowKilled))
				return
			}
		}
		total += received.Value.(int)
		foundPrime = false
		for _, prime := range primes {
			if prime == total {
				foundPrime = true
				break
			}
		}
		outputChan <- foundPrime
		readValues++
		if readValues == len(values) {
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
	var parity bool
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case parity = <-outputChan:
				fmt.Printf("pairty: %t\n", parity)
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

	require.False(parity)
}
