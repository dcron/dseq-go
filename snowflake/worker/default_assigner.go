package worker

type DefaultWorkerIdAssigner struct {
	workerId int64
}

func (this *DefaultWorkerIdAssigner) Init(workerId int64) {
	this.workerId = workerId
}

func (this *DefaultWorkerIdAssigner) AssignWorkerId(maxWorkerId int64) int64 {
	return this.workerId
}

func (this *DefaultWorkerIdAssigner) IsHealth() bool {
	return true
}
