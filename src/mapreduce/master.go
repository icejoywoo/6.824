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

	// map
	for i := 0; i < mr.nMap; i++ {
		go func(index int) {
			// 运行失败会直接重试，死循环直到成功
			for {
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
					mr.idleChannel <- worker
					return
				}
			}
		} (i)
	}

	for i := 0; i < mr.nMap; i++ {
		<- mapChan
	}

	DPrintf("Map is done.")

	reduceChan := make(chan int, mr.nReduce)
	defer close(reduceChan)

	for i := 0; i < mr.nReduce; i++ {
		go func(index int) {
			// 运行失败会直接重试，死循环直到成功
			for {
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
					mr.idleChannel <- worker
					return
				}
			}
		} (i)
	}

	for i := 0; i < mr.nReduce; i++ {
		<- reduceChan
	}

	DPrintf("reduce done\n")
	return mr.KillWorkers()
}
