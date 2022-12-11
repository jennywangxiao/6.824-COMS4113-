package mapreduce

import (
	"container/list"
	"fmt"
	"log"
	"sync"
)

type WorkerInfo struct {
	address string
}

/*
Distribute Map and Reduce Jobs to Workers (Channels)
*/
func (mr *MapReduce) DistributeJobsChannels(job_type JobType, n_jobs int, n_other_jobs int) {
	done_channel := make(chan int)
	for i := 0; i < n_jobs; i++ {
		go func(job_num int) {
			for {
				worker := <-mr.availableChannel
				var reply DoJobReply
				args := &DoJobArgs{mr.file, job_type, job_num, n_other_jobs}
				if call(worker, "Worker.DoJob", args, &reply) {
					done_channel <- job_num
					mr.availableChannel <- worker
					break
				}
				DPrintf("DoWork: RPC %s do job error\n", worker)
			}
		}(i)
	}
	for i := 0; i < n_jobs; i++ {
		<-done_channel
	}
}

/*
Distribute Map and Reduce Jobs to Workers (WaitGroups)
*/
func (mr *MapReduce) DistributeJobsWaitGroups(job_type JobType, n_jobs int, n_other_jobs int) {
	var wg sync.WaitGroup
	wg.Add(n_jobs)
	for i := 0; i < n_jobs; i++ {
		go func(job_num int) {
			// defer wg.Done() // !! Does Not Work unless buffered !!
			for {
				worker := <-mr.availableChannel
				var reply DoJobReply
				args := &DoJobArgs{mr.file, job_type, job_num, n_other_jobs}
				if call(worker, "Worker.DoJob", args, &reply) {
					wg.Done()
					mr.availableChannel <- worker
					return
				}
				DPrintf("DoWork: RPC %s do job error\n", worker)
			}
		}(i)
	}
	wg.Wait()
}

func (mr *MapReduce) DistributeJobsLocking(job_type JobType, n_jobs int, n_other_jobs int) {
	c := 0
	mu := &sync.Mutex{}
	cu := sync.NewCond(mu)
	mu.Lock()
	defer mu.Unlock()
	for i := 0; i < n_jobs; i++ {
		go func(job_num int) {
			for {
				worker := <-mr.availableChannel
				var reply DoJobReply
				args := &DoJobArgs{mr.file, job_type, job_num, n_other_jobs}
				if call(worker, "Worker.DoJob", args, &reply) {
					mu.Lock()
					c++
					if c >= n_jobs {
						cu.Broadcast()
					}
					mu.Unlock()
					mr.availableChannel <- worker
					return
				}
				DPrintf("DoWork: RPC %s do job error\n", worker)
			}
		}(i)
	}
	cu.Wait()
}

/*
Register Incoming Workers
*/
func (mr *MapReduce) RegisterWorkers() {
	for {
		worker := <-mr.registerChannel
		mr.Workers[worker] = &WorkerInfo{worker}
		mr.availableChannel <- worker
	}
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	log.Println(len(mr.Workers))
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
	go mr.RegisterWorkers()
	mr.DistributeJobsChannels(Map, mr.nMap, mr.nReduce)
	mr.DistributeJobsChannels(Reduce, mr.nReduce, mr.nMap)
	return mr.KillWorkers()
}
