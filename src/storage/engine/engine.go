package engine

import "github.com/Blackdeer1524/GraphDB/src/storage/systemcatalog"

type Locker interface {
}

type StorageEngine struct {
	lock    Locker
	catalog *systemcatalog.Manager
}

func New(s *systemcatalog.Manager, l Locker) *StorageEngine {
	return &StorageEngine{
		catalog: s,
	}
}

func (s *StorageEngine) CreateVertexTable() {
	panic("unimplemented")
}

func (s *StorageEngine) DropVertexTable() {
	panic("unimplemented")
}

func (s *StorageEngine) CreateEdgesTable() {
	panic("unimplemented")
}

func (s *StorageEngine) DropEdgesTable() {
	panic("unimplemented")
}
