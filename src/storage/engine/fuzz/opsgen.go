package fuzz

import (
	"math/rand"

	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/storage"
)

type OpsGenerator struct {
	r     *rand.Rand
	count int

	vertexTables map[string]storage.Schema
	edgeTables   map[string]storage.Schema
	indexes      map[string]storage.IndexMeta
}

func NewOpsGenerator(r *rand.Rand, count int) *OpsGenerator {
	return &OpsGenerator{
		r:     r,
		count: count,

		vertexTables: make(map[string]storage.Schema),
		edgeTables:   make(map[string]storage.Schema),
		indexes:      make(map[string]storage.IndexMeta),
	}
}

func (g *OpsGenerator) genRandomOp() Operation {
	try := OpType(g.r.Intn(6))

	switch try {
	case OpCreateVertexTable:
		tblName := randomTableName(g.r, g.vertexTables, 2)

		g.vertexTables[tblName] = randomSchema(g.r)

		return Operation{
			Type: OpCreateVertexTable,
			Name: tblName,
		}

	case OpDropVertexTable:
		tblName := randomTableName(g.r, g.vertexTables, 8)

		delete(g.vertexTables, tblName)

		return Operation{
			Type: OpDropVertexTable,
			Name: tblName,
		}

	case OpCreateEdgeTable:
		tblName := randomTableName(g.r, g.edgeTables, 2)

		g.edgeTables[tblName] = randomSchema(g.r)

		return Operation{
			Type: OpCreateEdgeTable,
			Name: tblName,
		}

	case OpDropEdgeTable:
		tblName := randomTableName(g.r, g.edgeTables, 8)

		delete(g.edgeTables, tblName)

		return Operation{
			Type: OpDropVertexTable,
			Name: tblName,
		}

	case OpCreateIndex:
		var (
			kind      string
			tblName   string
			indexName string
		)

		if len(g.vertexTables) == 0 && len(g.edgeTables) == 0 {
			return g.genRandomOp()
		}

		useVertex := len(g.vertexTables) > 0 && (len(g.edgeTables) == 0 || g.r.Intn(2) == 0)
		if useVertex {
			tblName, _ = getRandomMapKey(g.r, g.vertexTables)
			indexName = randomVertexIndexNameForCreate(g.r, g.indexes, 2)
		} else {
			tblName, _ = getRandomMapKey(g.r, g.edgeTables)
			indexName = randomEdgeIndexNameForCreate(g.r, g.indexes, 2)
		}

		g.indexes[indexName] = storage.IndexMeta{}

		return Operation{
			Type:      OpCreateIndex,
			Name:      indexName,
			Table:     tblName,
			TableKind: kind,
		}

	case OpDropIndex:
		indexName := randomIndexNameForDrop(g.r, g.indexes, 8)

		delete(g.indexes, indexName)

		return Operation{
			Type: OpCreateIndex,
			Name: indexName,
		}
	}

	panic("unreachable")
}

func (g *OpsGenerator) Gen() chan Operation {
	ch := make(chan Operation)

	go func() {
		defer close(ch)

		for i := range g.count {
			op := g.genRandomOp()
			op.TxnID = common.TxnID(i)

			ch <- op
		}
	}()

	return ch
}
