package main

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strconv"

	dynamic "github.com/theskyinflames/dynamic"
)

const allNumbersSent = -1

func numbersGeneratorJob() dynamic.Job {
	var allSent bool
	return func(ctx context.Context, postman dynamic.Postman) {
		if allSent {
			return
		}
		const numbers = 100
		used := make(map[int]struct{})
		for i := 0; i < numbers; i++ {
			random := rand.Intn(numbers)
			if _, ok := used[random]; ok {
				i-- // avoid repeated values
				continue
			}
			used[random] = struct{}{}
			select {
			case <-ctx.Done():
				return
			default:
				p := dynamic.Param{
					Value: random,
				}
				// blocking until the value is sent
				for !postman.Send(ctx, p) {
				}
			}
		}
		for !postman.Send(ctx, dynamic.Param{Value: allNumbersSent}) {
		}
		allSent = true
	}
}

func sorterJob(order int) dynamic.Job {
	sorted := []int{}
	return func(ctx context.Context, postman dynamic.Postman) {
		var (
			p   *dynamic.Param
			err error
		)
		// blocking receive
		for p == nil {
			p, err = postman.Receive(ctx)
			if err != nil {
				if err != dynamic.ErrFlowKilled {
					panic(err)
				}
				return
			}
		}
		if (*p).Value.(int) == allNumbersSent {
			for !postman.Send(ctx, dynamic.Param{
				Name:  fmt.Sprint(order),
				Value: sorted,
			}) {
			}
		} else {
			sorted = append(sorted, (*p).Value.(int))
			sort.Ints(sorted)
		}
	}
}

func consolidateJob(numSorters int, output chan []int) dynamic.Job {
	sortedByOrder := make(map[int][]int)
	return func(ctx context.Context, postman dynamic.Postman) {
		var (
			p   *dynamic.Param
			err error
		)
		// blocking receive
		for p == nil {
			p, err = postman.Receive(ctx)
			if err != nil {
				if err != dynamic.ErrFlowKilled {
					panic(err)
				}
				return
			}
		}

		name, _ := strconv.Atoi((*p).Name)
		sorted := (*p).Value.([]int)
		sortedByOrder[name] = sorted
		numSorters--
		if numSorters == 0 {
			output <- buildSortedSliceFromMap(sortedByOrder)
			return
		}
	}
}

func buildSortedSliceFromMap(sortedByOrder map[int][]int) []int {
	sorters := []int{}
	for sorter := range sortedByOrder {
		sorters = append(sorters, sorter)
	}
	sort.Ints(sorters)

	allSorted := []int{}
	for sorter := range sorters {
		allSorted = append(allSorted, sortedByOrder[sorter]...)
	}
	return allSorted
}

func main() {
	worker1 := dynamic.NewWorker(
		nil,
		numbersGeneratorJob(),
		dynamic.NameWOpt("w1"),
		dynamic.AddInfoFuncWOpt(dynamic.Infof),
		dynamic.ErrHndFuncWOpt(dynamic.Errorf),
	)

	worker2 := dynamic.NewWorker(
		[]*dynamic.Worker{&worker1},
		sorterJob(0),
		dynamic.NameWOpt("w2"),
		dynamic.AddInFilterFuncWOpt(func(p dynamic.Param) bool {
			return p.Value.(int) <= 10 || p.Value.(int) == allNumbersSent
		}),
		dynamic.AddInfoFuncWOpt(dynamic.Infof),
		dynamic.ErrHndFuncWOpt(dynamic.Errorf),
	)

	worker3 := dynamic.NewWorker(
		[]*dynamic.Worker{&worker1},
		sorterJob(1),
		dynamic.NameWOpt("w3"),
		dynamic.AddInFilterFuncWOpt(func(p dynamic.Param) bool {
			return (p.Value.(int) > 10 && p.Value.(int) <= 50) || p.Value.(int) == allNumbersSent
		}),
		dynamic.AddInfoFuncWOpt(dynamic.Infof),
		dynamic.ErrHndFuncWOpt(dynamic.Errorf),
	)

	worker4 := dynamic.NewWorker(
		[]*dynamic.Worker{&worker1},
		sorterJob(2),
		dynamic.NameWOpt("w4"),
		dynamic.AddInFilterFuncWOpt(func(p dynamic.Param) bool {
			return p.Value.(int) > 50 || p.Value.(int) == allNumbersSent
		}),
		dynamic.AddInfoFuncWOpt(dynamic.Infof),
		dynamic.ErrHndFuncWOpt(dynamic.Errorf),
	)

	outputChan := make(chan []int)
	worker5 := dynamic.NewWorker(
		[]*dynamic.Worker{&worker2, &worker3, &worker4},
		consolidateJob(3, outputChan),
		dynamic.NameWOpt("w5"),
		dynamic.AddInfoFuncWOpt(dynamic.Infof),
		dynamic.ErrHndFuncWOpt(dynamic.Errorf),
	)

	// create the flow
	ctx := context.Background()
	flow := dynamic.NewFlow(ctx, dynamic.AddInfoFuncFOpt(dynamic.Infof))

	// add workers to the workflow
	flow.AddWorker(worker1)
	flow.AddWorker(worker2)
	flow.AddWorker(worker3)
	flow.AddWorker(worker4)
	flow.AddWorker(worker5)

	// start the workflow
	flow.Run()

	// wait for the flow ends
	sorted := <-outputChan

	// kill flow workers
	flow.Kill()

	// output the flow result
	dynamic.Infof("sorted: %#v\n", sorted)
}
