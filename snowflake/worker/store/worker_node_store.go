package store

import (
	"database/sql"
)

type WorkerNode struct {
	workerId int64
}

type WorkerNodeStore struct {
	db sql.DB
}

func (this *WorkerNodeStore) InitWorkerNode(workerNode WorkerNode) (int64, error) {
	result, err := this.db.Exec("INSERT INTO CMF_SEQUENCE_WORKER_NODE(ID, LAUNCH_DATE, LIFE) VALUES (?, NOW(), DATE_ADD(NOW(),INTERVAL 300 SECOND))", workerNode.workerId)
	if err != nil {
		return int64(nil), err
	}

	return result.LastInsertId()
}

func (this *WorkerNodeStore) HearbeetWorkerNode(workerId int64, deadSeconds uint) (bool, error) {
	result, err := this.db.Exec("UPDATE CMF_SEQUENCE_WORKER_NODE SET LIFE = DATE_ADD(NOW(),INTERVAL ? SECOND) WHERE ID=?", deadSeconds, workerId)
	if err != nil {
		return false, err
	}

	rows, err := result.RowsAffected()

	return rows >= 1, err
}

func (this *WorkerNodeStore) DeleteDeadWorkerNodes() (bool, error) {
	_, err := this.db.Exec("DELETE FROM CMF_SEQUENCE_WORKER_NODE WHERE LIFE < NOW()")
	if err != nil {
		return false, err
	}

	return true, nil
}
