package mapreduce

import "container/list"
import "fmt"


type WorkerInfo struct {
	address string
	// You can add definitions here.
}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here
	mapChan := make(chan int, mr.nMap)
	defer close(mapChan)
	mapFailedChan := make(chan int, mr.nMap)
	defer close(mapFailedChan)

	var map_func func(int)
	map_func = func(index int) {
		args := DoJobArgs{
			File: mr.file,
			Operation: Map,
			JobNumber: index,
			NumOtherPhase: mr.nReduce,
		}
		var reply DoJobReply
		var worker string
		var ok bool = false
		select {
		case worker = <-mr.registerChannel:
			DPrintf("map worker: %v\n", worker)
			ok = call(worker, "Worker.DoJob", args, &reply)
		case worker = <-mr.idleChannel:
			DPrintf("map worker: %v\n", worker)
			ok = call(worker, "Worker.DoJob", args, &reply)
		}

		if ok {
			mapChan <- index
		} else {
			mapFailedChan <- index
			DPrintf("Failed worker: %v, retrying\n", worker)
		}
		// worker归还到idle中
		mr.idleChannel <- worker
	}

	for i := 0; i < mr.nMap; i++ {
		go map_func(i)
	}

	mapSuccess := 0
	for mapSuccess < mr.nMap {
		var index int
		select {
		case index = <-mapChan:
			DPrintf("map index: %v, success: %v\n", index, mapSuccess)
			mapSuccess++
		case index = <-mapFailedChan:
			go map_func(index)
		}
	}

	DPrintf("Map is done.")

	reduceChan := make(chan int, mr.nReduce)
	defer close(reduceChan)
	reduceFailedChan := make(chan int, mr.nReduce)
	defer close(reduceFailedChan)

	var reduce_func func(int)
	reduce_func = func(index int) {
		args := DoJobArgs{
			File: mr.file,
			Operation: Reduce,
			JobNumber: index,
			NumOtherPhase: mr.nMap,
		}
		var reply DoJobReply
		var worker string
		var ok bool = false
		select {
		case worker = <-mr.registerChannel:
			DPrintf("reduce worker: %v\n", worker)
			ok = call(worker, "Worker.DoJob", args, &reply)
		case worker = <-mr.idleChannel:
			DPrintf("reduce worker: %v\n", worker)
			ok = call(worker, "Worker.DoJob", args, &reply)
		}

		if ok {
			reduceChan <- index
		} else {
			reduceFailedChan <- index
			DPrintf("Failed worker: %v, retrying", worker)
		}
		mr.idleChannel <- worker
	}

	for i := 0; i < mr.nReduce; i++ {
		go reduce_func(i)
	}

	reduceSuccess := 0
	for reduceSuccess < mr.nReduce {
		var index int
		select {
		case index = <-reduceChan:
			DPrintf("reduce index: %v, success: %v\n", index, mapSuccess)
			reduceSuccess++
		case index = <-reduceFailedChan:
			go reduce_func(index)
		}
	}

	DPrintf("reduce done\n")
	return mr.KillWorkers()
}
