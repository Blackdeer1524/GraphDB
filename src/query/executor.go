package query

type StorageEngine interface {
}

type Executor struct {
	se StorageEngine
}

func New(se StorageEngine) *Executor {
	return &Executor{
		se: se,
	}
}
