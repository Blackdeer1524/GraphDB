package fuzz

import "github.com/Blackdeer1524/GraphDB/src/storage"

type Model struct {
	VertexTables map[string]storage.Schema
	EdgeTables   map[string]storage.Schema
	Indexes      map[string]storage.Index
}

func newModel() *Model {
	return &Model{
		VertexTables: make(map[string]storage.Schema),
		EdgeTables:   make(map[string]storage.Schema),
		Indexes:      make(map[string]storage.Index),
	}
}

func (m *Model) apply(op Operation, res OpResult) {
	if !res.Success {
		return
	}

	switch op.Type {
	case OpCreateVertexTable:
		if m.VertexTables == nil {
			m.VertexTables = make(map[string]storage.Schema)
		}
	case OpDropVertexTable:
		delete(m.VertexTables, op.Name)

		for idx, meta := range m.Indexes {
			if meta.TableName == op.Name && meta.TableKind == "vertex" {
				delete(m.Indexes, idx)
			}
		}
	case OpCreateEdgeTable:
		if m.EdgeTables == nil {
			m.EdgeTables = make(map[string]storage.Schema)
		}
	case OpDropEdgeTable:
		delete(m.EdgeTables, op.Name)
		for idx, meta := range m.Indexes {
			if meta.TableName == op.Name && meta.TableKind == "edge" {
				delete(m.Indexes, idx)
			}
		}
	case OpCreateIndex:
		m.Indexes[op.Name] = storage.Index{
			TableName: op.Table,
			TableKind: op.TableKind,
			Columns:   append([]string(nil), op.Columns...),
		}
	case OpDropIndex:
		delete(m.Indexes, op.Name)
	default:
		panic("unhandled op type")
	}
}
