package mapreduce

import "fmt"

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
    nfinished := 0
    for nfinished < ntasks {
        select {
        case wname := <-mr.registerChannel:
            doTaskArgs := &DoTaskArgs{JobName: mr.jobName, File: mr.files[nfinished],
                Phase: phase, TaskNumber: nfinished, NumOtherPhase: nios}
            nfinished++
            go func() { // in fact main can exit before this, so we should probabily use waitgroup
                ok := call(wname, "Worker.DoTask", doTaskArgs, new(struct{}))
                if ok {
                    mr.registerChannel <- wname
                }
            }()
        }
    }
	fmt.Printf("Schedule: %v phase done\n", phase)
}
