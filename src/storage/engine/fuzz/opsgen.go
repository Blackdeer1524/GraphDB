package engine

// import (
// 	"math/rand"
//
// 	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
// 	"github.com/Blackdeer1524/GraphDB/src/storage"
// )
//
// type OpsGenerator struct {
// 	r     *rand.Rand
// 	count int
//
// 	vertexTables  map[string]storage.Schema
// 	edgeTables    map[string]storage.Schema
// 	vertexIndexes map[string]storage.IndexMeta
// 	edgeIndexes   map[string]storage.IndexMeta
// }
//
// func NewOpsGenerator(r *rand.Rand, count int) *OpsGenerator {
// 	return &OpsGenerator{
// 		r:     r,
// 		count: count,
//
// 		vertexTables:  make(map[string]storage.Schema),
// 		edgeTables:    make(map[string]storage.Schema),
// 		vertexIndexes: make(map[string]storage.IndexMeta),
// 		edgeIndexes:   make(map[string]storage.IndexMeta),
// 	}
// }
//
// func (g *OpsGenerator) genRandomOp() Operation {
// 	try := OpType(g.r.Intn(6))
//
// 	switch try {
// 	case OpCreateVertexTable:
// 		tblName := randomTableName(g.r, g.vertexTables, 2)
// 		g.vertexTables[tblName] = randomSchema(g.r)
// 		return Operation{
// 			Type: OpCreateVertexTable,
// 			Name: tblName,
// 		}
//
// 	case OpDropVertexTable:
// 		tblName := randomTableName(g.r, g.vertexTables, 8)
// 		delete(g.vertexTables, tblName)
// 		return Operation{
// 			Type: OpDropVertexTable,
// 			Name: tblName,
// 		}
//
// 	case OpCreateEdgeTable:
// 		tblName := randomTableName(g.r, g.edgeTables, 2)
// 		g.edgeTables[tblName] = randomSchema(g.r)
// 		return Operation{
// 			Type: OpCreateEdgeTable,
// 			Name: tblName,
// 		}
//
// 	case OpDropEdgeTable:
// 		tblName := randomTableName(g.r, g.edgeTables, 8)
// 		delete(g.edgeTables, tblName)
// 		return Operation{
// 			Type: OpDropVertexTable,
// 			Name: tblName,
// 		}
//
// 	case OpCreateVertexIndex:
// 		var (
// 			tblName   string
// 			indexName string
// 		)
//
// 		if len(g.vertexTables) == 0 {
// 			return g.genRandomOp()
// 		}
//
// 		tblName, _ = getRandomMapKey(g.r, g.vertexTables)
// 		indexName = randomVertexIndexNameForCreate(g.r, g.vertexIndexes, 2)
//
// 		g.vertexIndexes[indexName] = storage.IndexMeta{}
//
// 		return Operation{
// 			Type:  OpCreateVertexIndex,
// 			Name:  indexName,
// 			Table: tblName,
// 		}
//
// 	case OpCreateEdgeIndex:
// 		var (
// 			tblName   string
// 			indexName string
// 		)
//
// 		if len(g.vertexTables) == 0 {
// 			return g.genRandomOp()
// 		}
//
// 		tblName, _ = getRandomMapKey(g.r, g.edgeTables)
// 		indexName = randomVertexIndexNameForCreate(g.r, g.edgeIndexes, 2)
//
// 		g.edgeIndexes[indexName] = storage.IndexMeta{}
//
// 		return Operation{
// 			Type:  OpCreateEdgeIndex,
// 			Name:  indexName,
// 			Table: tblName,
// 		}
//
// 	case OpDropVertexIndex:
// 		indexName := randomVertexIndexNameForDrop(g.r, g.vertexIndexes, 8)
// 		delete(g.vertexIndexes, indexName)
// 		return Operation{
// 			Type: OpDropVertexIndex,
// 			Name: indexName,
// 		}
//
// 	case OpDropEdgeIndex:
// 		indexName := randomEdgeIndexNameForDrop(g.r, g.edgeIndexes, 8)
// 		delete(g.edgeIndexes, indexName)
// 		return Operation{
// 			Type: OpDropEdgeIndex,
// 			Name: indexName,
// 		}
// 	}
//
// 	panic("unreachable")
// }
//
// func (g *OpsGenerator) Gen() chan Operation {
// 	ch := make(chan Operation)
//
// 	go func() {
// 		defer close(ch)
//
// 		for range g.count {
// 			op := g.genRandomOp()
// 			op.TxnID = common.TxnID(g.r.Intn(g.count))
//
// 			ch <- op
// 		}
// 	}()
//
// 	return ch
// }
//
