package runners

import (
	"github.com/dcron/dseq-go/snowflake/worker/store"
	"time"
)

const deadSeconds uint = 300

type WorkerNodeHeartbeet struct {
	workerNodeStore          store.WorkerNodeStore
	workerId                 int64
	lastSuccessHeartbeetTime int64
	stopFlag                 bool
}

func (this *WorkerNodeHeartbeet) Init(workerNodeStore store.WorkerNodeStore, workerId int64) {
	this.workerNodeStore = workerNodeStore
	this.workerId = workerId
	this.stopFlag = false

	go this.workerNodeHeartbeet()
}

func (this *WorkerNodeHeartbeet) workerNodeHeartbeet() {
	for {
		if this.stopFlag {
			return
		}

		// 清除所有失效的数据
		_, err := this.workerNodeStore.DeleteDeadWorkerNodes()

		// 基于workerId心跳
		result, err := this.workerNodeStore.HearbeetWorkerNode(this.workerId, deadSeconds)
		if err == nil && result {
			this.lastSuccessHeartbeetTime = time.Now().Unix()
		}

		time.Sleep(time.Second * 10)
	}
}

func (this *WorkerNodeHeartbeet) StopWorkerNodeHeartbeet() {
	this.stopFlag = true
}

func (this *WorkerNodeHeartbeet) IsSuccess() bool {
	// 长时间未心跳成功判断
	return (time.Now().Unix() - this.lastSuccessHeartbeetTime) <= int64(deadSeconds-100)
}
