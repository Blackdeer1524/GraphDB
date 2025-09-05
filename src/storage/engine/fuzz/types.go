package engine

// import (
// 	"fmt"
//
// 	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
// )
//
// type OpType int
//
// const (
// 	OpCreateVertexTable OpType = iota
// 	OpDropVertexTable
// 	OpCreateEdgeTable
// 	OpDropEdgeTable
// 	OpCreateVertexIndex
// 	OpCreateEdgeIndex
// 	OpDropVertexIndex
// 	OpDropEdgeIndex
// )
//
// type Operation struct {
// 	Type    OpType
// 	Name    string
// 	Table   string
// 	Columns []string
// 	TxnID   common.TxnID
// }
//
// func (op Operation) String() string {
// 	switch op.Type {
// 	case OpCreateVertexTable:
// 		return fmt.Sprintf("CreateVertexTable(name=%s, txn=%d)", op.Name, op.TxnID)
// 	case OpDropVertexTable:
// 		return fmt.Sprintf("DropVertexTable(name=%s, txn=%d)", op.Name, op.TxnID)
// 	case OpCreateEdgeTable:
// 		return fmt.Sprintf("CreateEdgeTable(name=%s, txn=%d)", op.Name, op.TxnID)
// 	case OpDropEdgeTable:
// 		return fmt.Sprintf("DropEdgeTable(name=%s, txn=%d)", op.Name, op.TxnID)
// 	case OpCreateVertexIndex:
// 		return fmt.Sprintf(
// 			"CreateVertexIndex(name=%s, table=%s, cols=%v, txn=%d)",
// 			op.Name,
// 			op.Table,
// 			op.Columns,
// 			op.TxnID,
// 		)
// 	case OpCreateEdgeIndex:
// 		return fmt.Sprintf(
// 			"CreateEdgeIndex(name=%s, table=%s, cols=%v, txn=%d)",
// 			op.Name,
// 			op.Table,
// 			op.Columns,
// 			op.TxnID,
// 		)
// 	case OpDropVertexIndex:
// 		return fmt.Sprintf("DropVertexIndex(name=%s, txn=%d)", op.Name, op.TxnID)
// 	case OpDropEdgeIndex:
// 		return fmt.Sprintf("DropEdgeIndex(name=%s, txn=%d)", op.Name, op.TxnID)
// 	default:
// 		return "unknown-op"
// 	}
// }
//
// type OpResult struct {
// 	Op      Operation
// 	Success bool
// 	ErrText string
// }
//
