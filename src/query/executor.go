package query

import (
	"github.com/Blackdeer1524/GraphDB/src/storage"
	"github.com/Blackdeer1524/GraphDB/src/txns"
)

type Executor struct {
	se     storage.Engine
	locker txns.ILockManager
}

func New(
	se storage.Engine,
	locker txns.ILockManager,
) *Executor {
	return &Executor{
		se:     se,
		locker: locker,
	}
}
