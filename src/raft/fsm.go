package raft

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Blackdeer1524/GraphDB/src"
	"io"
	"strconv"
	"strings"

	hraft "github.com/hashicorp/raft"
	"go.uber.org/zap"

	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/storage"
)

type fsm struct {
	nodeID    string
	log       src.Logger
	executor  Inserter
	txnLogger common.ITxnLogger
}

var _ hraft.FSM = &fsm{}

func (f *fsm) Apply(l *hraft.Log) any {
	// [action]\n[txnID]\n[tableName or edgeTableName]\n[payload JSON]
	fields := strings.Split(string(l.Data), "\n")
	if len(fields) < 4 {
		return errors.New("invalid log data format")
	}

	actionStr := fields[0]
	action, err := queryActionFromString(actionStr)
	if err != nil {
		return fmt.Errorf("can't apply unknown action: %s", actionStr)
	}

	txnIDVal, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return fmt.Errorf("invalid txnID: %v", err)
	}

	txnID := common.TxnID(txnIDVal)
	txnLogger := f.txnLogger.WithContext(txnID)

	nodeID := zap.String("node_id", f.nodeID)

	switch action {
	case InsertVertex:
		f.log.Infow("processing insert vertex action", nodeID)
		// fields[2] table name, fields[3] JSON record
		tableName := fields[2]
		var record storage.VertexInfo
		if err := json.Unmarshal([]byte(fields[3]), &record); err != nil {
			f.log.Errorw("failed to parse vertex record JSON", zap.Error(err), nodeID)
			return fmt.Errorf("failed to parse vertex record JSON: %w", err)
		}
		err := f.executor.InsertVertex(txnID, tableName, record, txnLogger)
		if err != nil {
			f.log.Errorw("failed to insert vertex", zap.Error(err), nodeID)
			return fmt.Errorf("InsertVertex failed: %w", err)
		}
		f.log.Infow("vertex inserted", nodeID, "vertex_id", record.SystemID)
		return record.SystemID

	case InsertVertices:
		f.log.Infow("processing insert vertices action", nodeID)
		// fields[2] table name, fields[3] JSON array of records
		tableName := fields[2]
		var records []storage.VertexInfo
		if err := json.Unmarshal([]byte(fields[3]), &records); err != nil {
			f.log.Errorw("failed to parse vertices records JSON", zap.Error(err), nodeID)
			return fmt.Errorf("failed to parse vertices records JSON: %w", err)
		}
		err := f.executor.InsertVertices(txnID, tableName, records, txnLogger)
		if err != nil {
			f.log.Errorw("failed to insert vertices", zap.Error(err), nodeID)
			return fmt.Errorf("InsertVertices failed: %w", err)
		}
		f.log.Infow("vertices inserted", nodeID)
		ids := make([]storage.VertexSystemID, 0, len(records))
		for _, record := range records {
			ids = append(ids, record.SystemID)
		}
		return ids

	case InsertEdge:
		f.log.Infow("processing insert edge action", nodeID)
		// fields[2] table name, fields[3] JSON for a single EdgeInfo
		edgeTable := fields[2]
		var edgeInfo storage.EdgeInfo
		if err := json.Unmarshal([]byte(fields[3]), &edgeInfo); err != nil {
			f.log.Errorw("failed to parse edge record JSON", zap.Error(err), nodeID)
			return fmt.Errorf("failed to parse edge record JSON: %w", err)
		}
		err := f.executor.InsertEdge(txnID, edgeTable, edgeInfo, txnLogger)
		if err != nil {
			f.log.Errorw("failed to insert edge", zap.Error(err), nodeID)
			return fmt.Errorf("InsertEdge failed: %w", err)
		}
		f.log.Infow("edge inserted", nodeID, "edge_id", edgeInfo.SystemID)
		return edgeInfo.SystemID

	case InsertEdges:
		f.log.Infow("processing insert edges action", nodeID)
		// fields[2] table name, fields[3] JSON array of EdgeInfo
		edgeTable := fields[2]
		var edges []storage.EdgeInfo
		if err := json.Unmarshal([]byte(fields[3]), &edges); err != nil {
			f.log.Errorw("failed to parse edges records JSON", zap.Error(err), nodeID)
			return fmt.Errorf("failed to parse edges records JSON: %w", err)
		}
		err := f.executor.InsertEdges(txnID, edgeTable, edges, txnLogger)
		if err != nil {
			f.log.Errorw("failed to insert edges", zap.Error(err), nodeID)
			return fmt.Errorf("InsertEdges failed: %w", err)
		}
		f.log.Infow("edges inserted", nodeID)
		ids := make([]storage.EdgeSystemID, 0, len(edges))
		for _, edge := range edges {
			ids = append(ids, edge.SystemID)
		}
		return ids

	default:
		return fmt.Errorf("unhandled action: %s", actionStr)
	}
}

func (f *fsm) Snapshot() (hraft.FSMSnapshot, error) {
	//TODO implement me
	panic("implement me")
}

func (f *fsm) Restore(snapshot io.ReadCloser) error {
	//TODO implement me
	panic("implement me")
}
