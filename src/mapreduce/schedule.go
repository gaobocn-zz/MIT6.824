package mapreduce

import (
    "fmt"
    "container/list"
    "sync"
)

var responise chan int

var wg sync.WaitGroup

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//

    // in fact you can use slice to achieve the same complexity
    var itask int
    taskList := list.New()
    for i := 0; i < ntasks; i++ {
        taskList.PushBack(i)
    }

    nfinished := 0
    // this doesn't work, my guess is taskList is not mutexed, so it may not be up-to-date
    for nfinished < ntasks {
        fmt.Println(nfinished, ntasks, taskList.Len())
        if taskList.Len() > 0 {
            select {
            case wname := <-mr.registerChannel:
                itask = taskList.Remove(taskList.Front()).(int)
                doTaskArgs := &DoTaskArgs{JobName: mr.jobName, File: mr.files[itask],
                    Phase: phase, TaskNumber: itask, NumOtherPhase: nios}
                //fmt.Println("map task: ", itask)
                wg.Add(1)
                go func() { // in fact main can exit before this, so we should probabily use waitgroup
                    ok := call(wname, "Worker.DoTask", doTaskArgs, new(struct{}))
                    if ok {
                        nfinished++
                        mr.Register(&RegisterArgs{wname}, new(struct{}))
                    } else {
                        taskList.PushFront(itask)
                    }
                    wg.Done()
                }()
            }
        } else {
            fmt.Println("waiting")
            wg.Wait()
            fmt.Println("waiting end")
        }
    }
	fmt.Printf("Schedule: %v phase done\n", phase)
}

func HandleResponse(r int, nfinished *int, l *list.List) {
    if r < 0 {
        *nfinished++
    } else {
        l.PushFront(r)
    }
}
