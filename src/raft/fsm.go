package raft

import (
	"errors"
	"fmt"
	"github.com/Blackdeer1524/GraphDB/src"
	hraft "github.com/hashicorp/raft"
	"go.uber.org/zap"
	"io"
	"strings"
)

var _ hraft.FSM = &fsm{}

type fsm struct {
	nodeID string
	log    src.Logger
}

func (f *fsm) Apply(l *hraft.Log) any {
	fields := strings.Split(string(l.Data), "\n")
	if len(fields) < 2 {
		return errors.New("incorrect data received")
	}

	action, err := queryActionFromString(fields[0])
	if err != nil {
		return fmt.Errorf("can't apply unknown action: %s", fields[0])
	}

	switch action {
	case InsertVertex:
		f.log.Infow("processing insert vertex action", zap.String("node_id", f.nodeID))
	case InsertVertices:
		f.log.Infow("processing insert vertices action", zap.String("node_id", f.nodeID))
	case InsertEdge:
		f.log.Infow("processing insert edge action", zap.String("node_id", f.nodeID))
	case InsertEdges:
		f.log.Infow("processing insert edges action", zap.String("node_id", f.nodeID))
	}

	return nil
}

func (f *fsm) Snapshot() (hraft.FSMSnapshot, error) {
	//TODO implement me
	panic("implement me")
}

func (f *fsm) Restore(snapshot io.ReadCloser) error {
	//TODO implement me
	panic("implement me")
}
