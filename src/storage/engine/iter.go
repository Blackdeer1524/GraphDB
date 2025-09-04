package engine

import (
	"fmt"
	"iter"

	"github.com/Blackdeer1524/GraphDB/src/bufferpool"
	"github.com/Blackdeer1524/GraphDB/src/pkg/assert"
	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/pkg/utils"
	"github.com/Blackdeer1524/GraphDB/src/storage"
	"github.com/Blackdeer1524/GraphDB/src/txns"
)

func yieldErrorPair[T any](err error, yield func(utils.Pair[T, error]) bool) bool {
	var zero T
	errItem := utils.Pair[T, error]{
		First:  zero,
		Second: err,
	}
	return yield(errItem)
}

func yieldErrorTripple[T, K any](err error, yield func(utils.Triple[T, K, error]) bool) bool {
	var zeroT T
	var zeroK K
	errItem := utils.Triple[T, K, error]{
		First:  zeroT,
		Second: zeroK,
		Third:  err,
	}
	return yield(errItem)
}

type edgesIter struct {
	se            *StorageEngine
	curEdgeID     storage.EdgeID
	schema        storage.Schema
	edgeFileToken *txns.FileLockToken
	edgesIndex    storage.Index
}

func newEdgesIter(
	se *StorageEngine,
	startEdgeID storage.EdgeID,
	schema storage.Schema,
	edgeFileToken *txns.FileLockToken,
	edgesIndex storage.Index,
) (*edgesIter, error) {
	assert.Assert(!startEdgeID.IsNil(), "start edge ID shouldn't be nil")

	if !se.locker.UpgradeFileLock(edgeFileToken, txns.GranularLockShared) {
		return nil, fmt.Errorf("failed to upgrade file lock")
	}

	iter := &edgesIter{
		curEdgeID:     startEdgeID,
		se:            se,
		schema:        schema,
		edgeFileToken: edgeFileToken,
		edgesIndex:    edgesIndex,
	}
	return iter, nil
}

func (e *edgesIter) getAndMoveForward() (bool, utils.Pair[common.RecordID, storage.Edge], error) {
	assert.Assert(!e.curEdgeID.IsNil(), "current edge ID shouldn't be nil")

	rid, err := GetEdgeRID(e.edgeFileToken.GetTxnID(), e.curEdgeID, e.edgesIndex)
	if err != nil {
		nilEdgeInfo := utils.Pair[common.RecordID, storage.Edge]{
			First:  common.RecordID{},
			Second: storage.Edge{},
		}
		return false, nilEdgeInfo, err
	}

	pageIdent := rid.R.PageIdentity()
	pg, err := e.se.pool.GetPageNoCreate(pageIdent)
	if err != nil {
		nilEdgeInfo := utils.Pair[common.RecordID, storage.Edge]{
			First:  common.RecordID{},
			Second: storage.Edge{},
		}
		return false, nilEdgeInfo, err
	}
	edgeData := pg.LockedRead(rid.R.SlotNum)
	e.se.pool.Unpin(pageIdent)

	edgeInternalFields, edgeFields, err := parseEdgeRecord(edgeData, e.schema)
	if err != nil {
		nilEdgeInfo := utils.Pair[common.RecordID, storage.Edge]{
			First:  common.RecordID{},
			Second: storage.Edge{},
		}
		return false, nilEdgeInfo, err
	}
	edgeInfo := utils.Pair[common.RecordID, storage.Edge]{
		First: rid.R,
		Second: storage.Edge{
			EdgeInternalFields: edgeInternalFields,
			Data:               edgeFields,
		},
	}

	e.curEdgeID = edgeInternalFields.NextEdgeID
	if e.curEdgeID.IsNil() {
		return false, edgeInfo, nil
	}
	return true, edgeInfo, nil
}

func (e *edgesIter) Seq() iter.Seq[utils.Triple[common.RecordID, storage.Edge, error]] {
	return func(yield func(utils.Triple[common.RecordID, storage.Edge, error]) bool) {
		for {
			hasMore, edge, err := e.getAndMoveForward()
			if err != nil {
				yield(utils.Triple[common.RecordID, storage.Edge, error]{
					First:  common.RecordID{},
					Second: storage.Edge{},
					Third:  err,
				})
				return
			}

			item := utils.Triple[common.RecordID, storage.Edge, error]{
				First:  edge.First,
				Second: edge.Second,
				Third:  nil,
			}
			if !hasMore {
				yield(item)
				return
			}

			if !yield(item) {
				break
			}
		}
	}
}

type dirItemsIter struct {
	se           *StorageEngine
	curDirItemID storage.DirItemID
	dirFileToken *txns.FileLockToken
	dirIndex     storage.Index
}

func newDirItemsIter(
	se *StorageEngine,
	startDirItemID storage.DirItemID,
	dirFileToken *txns.FileLockToken,
	dirIndex storage.Index,
) (*dirItemsIter, error) {
	assert.Assert(!startDirItemID.IsNil(), "start directory item ID shouldn't be nil")
	if !se.locker.UpgradeFileLock(dirFileToken, txns.GranularLockShared) {
		return nil, fmt.Errorf("failed to upgrade file lock")
	}

	iter := &dirItemsIter{
		curDirItemID: startDirItemID,
		se:           se,
		dirFileToken: dirFileToken,
		dirIndex:     dirIndex,
	}
	return iter, nil
}

func (d *dirItemsIter) getAndMoveForward() (bool, storage.DirectoryItem, error) {
	assert.Assert(!d.curDirItemID.IsNil(), "current directory item ID shouldn't be nil")

	rid, err := GetDirectoryRID(d.dirFileToken.GetTxnID(), d.curDirItemID, d.dirIndex)
	if err != nil {
		return false, storage.DirectoryItem{}, err
	}

	pageIdent := rid.R.PageIdentity()
	pg, err := d.se.pool.GetPageNoCreate(pageIdent)
	if err != nil {
		return false, storage.DirectoryItem{}, err
	}
	dirItemData := pg.LockedRead(rid.R.SlotNum)
	d.se.pool.Unpin(pageIdent)

	dirItem, err := parseDirectoryRecord(dirItemData)
	if err != nil {
		return false, storage.DirectoryItem{}, err
	}

	d.curDirItemID = dirItem.NextItemID
	if d.curDirItemID.IsNil() {
		return false, dirItem, nil
	}
	return true, dirItem, nil
}

func (d *dirItemsIter) Seq() iter.Seq[utils.Pair[storage.DirectoryItem, error]] {
	return func(yield func(utils.Pair[storage.DirectoryItem, error]) bool) {
		for {
			hasMore, dirItem, err := d.getAndMoveForward()
			if err != nil {
				yieldErrorPair(err, yield)
				return
			}

			if !hasMore {
				yield(utils.Pair[storage.DirectoryItem, error]{First: dirItem, Second: nil})
				return
			}

			if !yield(utils.Pair[storage.DirectoryItem, error]{First: dirItem, Second: nil}) {
				break
			}
		}

	}
}

type neighboursEdgesIter struct {
	se             *StorageEngine
	vID            storage.VertexID
	vertTableToken *txns.FileLockToken
	vertIndex      storage.Index
	logger         common.ITxnLoggerWithContext
}

var _ storage.NeighborEdgesIter = &neighboursEdgesIter{}

func newNeighboursEdgesIter(
	se *StorageEngine,
	vID storage.VertexID,
	vertTableToken *txns.FileLockToken,
	vertIndex storage.Index,
	logger common.ITxnLoggerWithContext,
) *neighboursEdgesIter {
	iter := &neighboursEdgesIter{
		se:             se,
		vID:            vID,
		vertTableToken: vertTableToken,
		vertIndex:      vertIndex,
		logger:         logger,
	}
	return iter
}

func iterWithErrorTriple[T, K any](err error) func(yield func(utils.Triple[T, K, error]) bool) {
	return func(yield func(utils.Triple[T, K, error]) bool) {
		var zeroT T
		var zeroK K
		yield(utils.Triple[T, K, error]{First: zeroT, Second: zeroK, Third: err})
	}
}

func (i *neighboursEdgesIter) Seq() iter.Seq[utils.Triple[common.RecordID, storage.Edge, error]] {
	if !i.se.locker.UpgradeFileLock(i.vertTableToken, txns.GranularLockIntentionShared) {
		err := fmt.Errorf("failed to upgrade file lock")
		return iterWithErrorTriple[common.RecordID, storage.Edge](err)
	}

	vertRID, err := GetVertexRID(i.vertTableToken.GetTxnID(), i.vID, i.vertIndex)
	if err != nil {
		return iterWithErrorTriple[common.RecordID, storage.Edge](err)
	}
	pToken := i.se.locker.LockPage(i.vertTableToken, vertRID.R.PageID, txns.PageLockShared)
	if pToken == nil {
		return iterWithErrorTriple[common.RecordID, storage.Edge](fmt.Errorf("failed to lock page"))
	}

	pg, err := i.se.pool.GetPageNoCreate(vertRID.R.PageIdentity())
	if err != nil {
		return iterWithErrorTriple[common.RecordID, storage.Edge](err)
	}
	vertData := pg.LockedRead(vertRID.R.SlotNum)
	i.se.pool.Unpin(vertRID.R.PageIdentity())

	vertInternalFields, _, err := parseVertexRecordHeader(vertData)
	if err != nil {
		return iterWithErrorTriple[common.RecordID, storage.Edge](err)
	}
	if vertInternalFields.DirItemID.IsNil() {
		return func(yield func(utils.Triple[common.RecordID, storage.Edge, error]) bool) {
			return
		}
	}

	cToken := txns.NewNilCatalogLockToken(i.vertTableToken.GetTxnID())
	dirTableMeta, err := i.se.GetDirTableMeta(cToken, i.vertTableToken.GetFileID())
	if err != nil {
		return iterWithErrorTriple[common.RecordID, storage.Edge](err)
	}

	dirIndex, err := i.se.GetDirTableInternalIndex(
		i.vertTableToken.GetTxnID(),
		dirTableMeta.FileID,
		cToken,
		i.logger,
	)
	if err != nil {
		return iterWithErrorTriple[common.RecordID, storage.Edge](err)
	}

	dirFileToken := txns.NewNilFileLockToken(cToken, dirTableMeta.FileID)
	return func(yield func(utils.Triple[common.RecordID, storage.Edge, error]) bool) {
		dirItemsIter, err := newDirItemsIter(
			i.se,
			vertInternalFields.DirItemID,
			dirFileToken,
			dirIndex,
		)
		if err != nil {
			yieldErrorTripple(err, yield)
			return
		}

		for dirItemErr := range dirItemsIter.Seq() {
			dirItem, err := dirItemErr.Destruct()
			if err != nil {
				yieldErrorTripple(err, yield)
				return
			}

			if dirItem.EdgeID.IsNil() {
				continue
			}

			edgesFileToken := txns.NewNilFileLockToken(cToken, dirItem.EdgeFileID)
			edgesIndex, err := i.se.GetEdgeTableInternalIndex(
				i.vertTableToken.GetTxnID(),
				dirItem.EdgeFileID,
				cToken,
				i.logger,
			)
			if err != nil {
				yieldErrorTripple(err, yield)
				return
			}

			edgesMeta, err := i.se.GetEdgeTableMetaByFileID(dirItem.EdgeFileID, cToken)
			if err != nil {
				yieldErrorTripple(err, yield)
				return
			}

			edgesIter, err := newEdgesIter(
				i.se,
				dirItem.EdgeID,
				edgesMeta.Schema,
				edgesFileToken,
				edgesIndex,
			)
			if err != nil {
				yieldErrorTripple(err, yield)
				return
			}

			for ridEdgesErr := range edgesIter.Seq() {
				_, _, err := ridEdgesErr.Destruct()
				if err != nil {
					yieldErrorTripple(err, yield)
					return
				}

				if !yield(ridEdgesErr) {
					return
				}
			}
		}
	}
}

func (i *neighboursEdgesIter) Close() error {
	return nil
}

type neighboursIter struct {
	se             *StorageEngine
	vID            storage.VertexID
	vertTableToken *txns.FileLockToken
	vertIndex      storage.Index
	locker         *txns.LockManager
	logger         common.ITxnLoggerWithContext
	pool           bufferpool.BufferPool
}

var _ storage.NeighborIter = &neighboursIter{}

func newNeighboursIter(
	se *StorageEngine,
	pool bufferpool.BufferPool,
	vID storage.VertexID,
	vertTableToken *txns.FileLockToken,
	vertIndex storage.Index,
	locker *txns.LockManager,
	logger common.ITxnLoggerWithContext,
) *neighboursIter {
	iter := &neighboursIter{
		se:             se,
		pool:           pool,
		vID:            vID,
		vertTableToken: vertTableToken,
		vertIndex:      vertIndex,
		locker:         locker,
		logger:         logger,
	}
	return iter
}

func (i *neighboursIter) Seq() iter.Seq[utils.Pair[storage.VertexIDWithRID, error]] {
	return func(yield func(utils.Pair[storage.VertexIDWithRID, error]) bool) {
		edgesIter := newNeighboursEdgesIter(i.se, i.vID, i.vertTableToken, i.vertIndex, i.logger)

		cToken := i.vertTableToken.GetCatalogLockToken()
		lastEdgeFileID := common.NilFileID
		var vertexIndex storage.Index
		for ridEdgeErr := range edgesIter.Seq() {
			edgeRID, edge, err := ridEdgeErr.Destruct()
			if err != nil {
				yieldErrorPair(err, yield)
				return
			}

			if edgeRID.FileID != lastEdgeFileID {
				lastEdgeFileID = edgeRID.FileID
				edgeMeta, err := i.se.GetEdgeTableMetaByFileID(edgeRID.FileID, cToken)
				if err != nil {
					yieldErrorPair(err, yield)
					return
				}

				vertexIndex, err = i.se.GetVertexTableInternalIndex(
					cToken.GetTxnID(),
					edgeMeta.DstVertexFileID,
					cToken,
					i.logger,
				)
				if err != nil {
					yieldErrorPair(err, yield)
					return
				}
			}

			vertexRID, err := GetVertexRID(
				cToken.GetTxnID(),
				edge.DstVertexID,
				vertexIndex,
			)
			if err != nil {
				yieldErrorPair(err, yield)
				return
			}

			vertexInfo := utils.Pair[storage.VertexIDWithRID, error]{
				First:  vertexRID,
				Second: nil,
			}
			if !yield(vertexInfo) {
				return
			}
		}
	}
}

func (i *neighboursIter) Close() error {
	return nil
}
