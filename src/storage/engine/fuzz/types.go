package fuzz

import (
	"fmt"

	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
)

type OpType int

const (
	OpCreateVertexTable OpType = iota
	OpDropVertexTable
	OpCreateEdgeTable
	OpDropEdgeTable
	OpCreateIndex
	OpDropIndex
)

type Operation struct {
	Type      OpType
	Name      string
	Table     string
	TableKind string
	Columns   []string
	TxnID     common.TxnID
}

func (op Operation) String() string {
	switch op.Type {
	case OpCreateVertexTable:
		return fmt.Sprintf("CreateVertexTable(name=%s, txn=%d)", op.Name, op.TxnID)
	case OpDropVertexTable:
		return fmt.Sprintf("DropVertexTable(name=%s, txn=%d)", op.Name, op.TxnID)
	case OpCreateEdgeTable:
		return fmt.Sprintf("CreateEdgeTable(name=%s, txn=%d)", op.Name, op.TxnID)
	case OpDropEdgeTable:
		return fmt.Sprintf("DropEdgeTable(name=%s, txn=%d)", op.Name, op.TxnID)
	case OpCreateIndex:
		return fmt.Sprintf(
			"CreateIndex(name=%s, table=%s/%s, cols=%v, txn=%d)",
			op.Name,
			op.TableKind,
			op.Table,
			op.Columns,
			op.TxnID,
		)
	case OpDropIndex:
		return fmt.Sprintf("DropIndex(name=%s, txn=%d)", op.Name, op.TxnID)
	default:
		return "unknown-op"
	}
}

type OpResult struct {
	Op      Operation
	Success bool
	ErrText string
}
