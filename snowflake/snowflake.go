package snowflake

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

type WorkerIdAssigner interface {
	AssignWorkerId(maxWorkerId int64) int64
	IsHealth() bool
}

type SnowflakeUidGenerator struct {
	sequenceBits       uint
	workerIdBits       uint
	timestampBits      uint
	sequenceMask       int64
	workerIdLeftShift  uint
	workerId           int64
	workerIdAssigner   WorkerIdAssigner
	maxWorkerId        int64
	timestampLeftShift uint
	epochTimestamp     int64
	maxDeltaTimes      int64
	sequence           int64
	lastTimestamp      int64
	idLock             *sync.Mutex
}

func (this *SnowflakeUidGenerator) Init(workerIdAssigner WorkerIdAssigner, timestampBits uint, workerIdBits uint) error {

	this.timestampBits = timestampBits
	this.workerIdBits = workerIdBits
	this.sequenceBits = 64 - 1 - this.timestampBits - this.workerIdBits

	this.sequenceMask = -1 ^ (-1 << this.sequenceBits)
	this.sequence = 0

	this.workerIdLeftShift = this.sequenceBits
	this.maxWorkerId = -1 ^ (-1 << this.workerIdBits)
	this.workerIdAssigner = workerIdAssigner
	this.workerId = this.workerIdAssigner.AssignWorkerId(this.maxWorkerId)
	if this.workerId < 0 || this.workerId > this.maxWorkerId {
		return errors.New(fmt.Sprintf("workerId[%v] is less than 0 or greater than maxWorkerId[%v].", this.workerId, this.maxWorkerId))
	}

	this.timestampLeftShift = this.workerIdBits + this.workerIdLeftShift
	this.maxDeltaTimes = -1 ^ (-1 << this.timestampBits)
	epochDateStr := "2019-10-01"
	epochDate, _ := time.Parse("2006-01-02", epochDateStr)
	this.epochTimestamp = epochDate.UnixNano() / int64(time.Millisecond)
	this.lastTimestamp = -1

	this.idLock = &sync.Mutex{}
	return nil
}

func (this *SnowflakeUidGenerator) GetUniqueID() (int64, error) {
	this.idLock.Lock()
	if !this.workerIdAssigner.IsHealth() {
		this.workerId = this.workerIdAssigner.AssignWorkerId(this.maxWorkerId)
	}
	timestamp := this.timeGen()
	if timestamp < this.lastTimestamp {
		return -1, errors.New(fmt.Sprintf("Clock moved backwards.  Refusing to generate id for %d milliseconds", this.lastTimestamp-timestamp))
	}

	if timestamp == this.lastTimestamp {
		this.sequence = (this.sequence + 1) & this.sequenceMask
		if this.sequence == 0 {
			timestamp = this.tilNextMillis()
			this.sequence = 0
		}
	} else {
		this.sequence = 0
	}

	if timestamp-this.epochTimestamp > this.maxDeltaTimes {
		return -1, errors.New(fmt.Sprintf("Timestamp bits is exhausted. Refusing UID generate. Now:  %d", timestamp))
	}

	this.lastTimestamp = timestamp

	this.idLock.Unlock()

	id := ((timestamp - this.epochTimestamp) << this.timestampLeftShift) |
		(this.workerId << this.workerIdLeftShift) |
		this.sequence

	return id, nil
}

func (this *SnowflakeUidGenerator) tilNextMillis() int64 {
	timestamp := this.timeGen()

	for {
		if timestamp > this.lastTimestamp {
			break
		}

		timestamp = this.timeGen()
	}

	return timestamp
}

func (this *SnowflakeUidGenerator) timeGen() int64 {
	timestamp := time.Now().UnixNano() / int64(time.Millisecond)

	return timestamp
}
