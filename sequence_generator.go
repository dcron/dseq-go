package dseq_go

type SequenceGenerator interface {
	GetUniqueID() (int64, error)
}