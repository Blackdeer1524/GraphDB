package storage

import (
	"encoding"
	"errors"
	"fmt"
	"iter"

	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/pkg/utils"
)

type VertexID uint64

type VertexWithDepthAndRID struct {
	V VertexID
	D uint32
	R common.RecordID
}

type VertexIDWithRID struct {
	V VertexID
	R common.RecordID
}

func (v *VertexIDWithRID) MarshalBinary() ([]byte, error) {
	return nil, nil
}

func (v *VertexIDWithRID) UnmarshalBinary(data []byte) error {
	return nil
}

var ErrQueueEmpty = errors.New("queue is empty")

type NeighborIter interface {
	Seq() iter.Seq[utils.Pair[[]VertexIDWithRID, error]]
	Close() error
}

type Queue interface {
	Enqueue(v VertexWithDepthAndRID) error
	Dequeue() (VertexWithDepthAndRID, error)

	Close() error
}

type RawQueue interface {
	Enqueue(v []byte) error
	Dequeue() ([]byte, error)
	Close() error
}

type BitMap interface {
	Get(v VertexID) (bool, error)
	Set(v VertexID, b bool) error
	Close() error
}

type TransactionManager interface {
	Begin() (common.TxnID, error)

	RollbackTx(common.TxnID) error
	CommitTx(common.TxnID) error
}

type TypedQueue[T interface {
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
}] struct {
	q RawQueue
}

func (tq *TypedQueue[T]) Enqueue(v T) error {
	b, err := v.MarshalBinary()
	if err != nil {
		return err
	}

	return tq.q.Enqueue(b)
}

func (tq *TypedQueue[T]) Dequeue() (T, error) {
	var zero T

	b, err := tq.q.Dequeue()
	if err != nil {
		return zero, fmt.Errorf("failed to dequeue: %w", err)
	}

	var obj T

	err = obj.UnmarshalBinary(b)
	if err != nil {
		return zero, fmt.Errorf("failed to unmarshal: %w", err)
	}

	return obj, nil
}

func (tq *TypedQueue[T]) Close() error {
	return tq.q.Close()
}

var ErrNoVertexesInGraph = errors.New("no vertexes")

type Vertex struct {
	ID   VertexID
	Data map[string]any
}

type VerticesIter interface {
	Seq() iter.Seq[*Vertex]
	Close() error
}

type Edge struct {
	Src  VertexID
	Dst  VertexID
	Data map[string]any
}

type EdgeFilter func(*Edge) bool

type SumNeighborAttributesFilter func(float64) bool

type AssociativeArray[K comparable, V any] interface {
	Get(k K) (V, bool)
	Set(k K, v V) error
	Seq(yield func(K, V) bool)
}
