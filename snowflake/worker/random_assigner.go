package worker

import (
	"database/sql"
	"github.com/dcron/dseq-go/snowflake/worker/runners"
	"github.com/dcron/dseq-go/snowflake/worker/store"
	"math/rand"
	"time"
)

type RandomWorkerIdAssigner struct {
	name                string
	db                  sql.DB
	workerNodeStore     store.WorkerNodeStore
	workerNodeHeartbeet *runners.WorkerNodeHeartbeet
}

func (this *RandomWorkerIdAssigner) Init(name string, db sql.DB) {
	this.name = name
	this.workerNodeStore = store.WorkerNodeStore{db}
}

// 生成一个1～2^workerBits内的随机值，去数据库申请，当值存在时，重新随机取值，重新申请。
func (this *RandomWorkerIdAssigner) AssignWorkerId(maxWorkerId int64) int64 {
	if this.workerNodeHeartbeet != nil {
		this.workerNodeHeartbeet.StopWorkerNodeHeartbeet()
	}

	var workerId int64 = -1
	var retry int64 = 0

	for {
		if workerId <= -1 {
			tempWorkerId := rand.Int63n(maxWorkerId)

			_, err := this.workerNodeStore.InitWorkerNode(store.WorkerNode{tempWorkerId})

			if err != nil {
				retry += 10
				time.Sleep(time.Millisecond * time.Duration(retry))
			}
		}
	}

	this.workerNodeHeartbeet = &runners.WorkerNodeHeartbeet{}
	this.workerNodeHeartbeet.Init(this.workerNodeStore, workerId)

	return workerId
}

func (this *RandomWorkerIdAssigner) IsHealth() bool {
	return this.workerNodeHeartbeet.IsSuccess()
}
