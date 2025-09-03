package engine

import (
	"fmt"
	"iter"

	"github.com/Blackdeer1524/GraphDB/src/pkg/assert"
	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/pkg/utils"
	"github.com/Blackdeer1524/GraphDB/src/storage"
	"github.com/Blackdeer1524/GraphDB/src/txns"
)

type edgesIter struct {
	se            *StorageEngine
	curEdgeID     storage.EdgeID
	edgeFileToken *txns.FileLockToken
	edgeSchema    storage.Schema
	edgesIndex    storage.Index
}

func newEdgesIter(
	se *StorageEngine,
	startEdgeID storage.EdgeID,
	edgeFileToken *txns.FileLockToken,
	edgeSchema storage.Schema,
	edgesIndex storage.Index,
) (*edgesIter, error) {
	if !se.locker.UpgradeFileLock(edgeFileToken, txns.GranularLockShared) {
		return nil, fmt.Errorf("failed to upgrade file lock")
	}

	iter := &edgesIter{
		curEdgeID:     startEdgeID,
		se:            se,
		edgeFileToken: edgeFileToken,
		edgeSchema:    edgeSchema,
		edgesIndex:    edgesIndex,
	}
	return iter, nil
}

func (e *edgesIter) getAndMoveForward() (bool, storage.Edge, error) {
	rid, err := e.se.GetEdgeRID(e.edgeFileToken.GetTxnID(), e.curEdgeID, e.edgesIndex)
	if err != nil {
		return false, storage.Edge{}, err
	}

	pageIdent := rid.R.PageIdentity()
	pg, err := e.se.pool.GetPageNoCreate(pageIdent)
	if err != nil {
		return false, storage.Edge{}, err
	}
	defer e.se.pool.Unpin(pageIdent)

	edgeData := pg.LockedRead(rid.R.SlotNum)
	edgeInternalFields, edgeFields, err := parseEdgeRecord(edgeData, e.edgeSchema)
	if err != nil {
		return false, storage.Edge{}, err
	}

	edge := storage.Edge{
		EdgeInternalFields: edgeInternalFields,
		Data:               edgeFields,
	}
	if edgeInternalFields.NextEdgeID.IsNil() {
		return false, edge, nil
	}
	e.curEdgeID = edgeInternalFields.NextEdgeID
	return true, edge, nil
}

func (e *edgesIter) Seq() iter.Seq[utils.Pair[storage.Edge, error]] {
	return func(yield func(utils.Pair[storage.Edge, error]) bool) {
		for {
			hasMore, edge, err := e.getAndMoveForward()
			if err != nil {
				yield(utils.Pair[storage.Edge, error]{First: storage.Edge{}, Second: err})
				return
			}

			if !hasMore {
				yield(utils.Pair[storage.Edge, error]{First: edge, Second: nil})
				return
			}

			if !yield(utils.Pair[storage.Edge, error]{First: edge, Second: nil}) {
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
	assert.Assert(!d.curDirItemID.IsNil(), "current directory item ID is nil")
	rid, err := d.se.GetDirectoryRID(d.dirFileToken.GetTxnID(), d.curDirItemID, d.dirIndex)
	if err != nil {
		return false, storage.DirectoryItem{}, err
	}

	pageIdent := rid.R.PageIdentity()
	pg, err := d.se.pool.GetPageNoCreate(pageIdent)
	if err != nil {
		return false, storage.DirectoryItem{}, err
	}
	defer d.se.pool.Unpin(pageIdent)

	dirItemData := pg.LockedRead(rid.R.SlotNum)
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
				yield(
					utils.Pair[storage.DirectoryItem, error]{
						First:  storage.DirectoryItem{},
						Second: err,
					},
				)
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

type vertexNeighboursIter struct {
	se             *StorageEngine
	vertTableToken *txns.FileLockToken
	vID            storage.VertexID
	vertIndex      storage.Index
	logger         common.ITxnLoggerWithContext
}

// func newVertexNeighboursIter(
// 	se *StorageEngine,
// 	txnID common.TxnID,
// 	vertTableID common.FileID,
// 	vertRID storage.VertexIDWithRID,
// ) (*vertexNeighboursIter, error) {
// }

func iterWithError(err error) func(yield func(utils.Pair[storage.VertexIDWithRID, error]) bool) {
	return func(yield func(utils.Pair[storage.VertexIDWithRID, error]) bool) {
		yield(
			utils.Pair[storage.VertexIDWithRID, error]{
				First:  storage.VertexIDWithRID{},
				Second: err,
			},
		)
	}
}

func (i *vertexNeighboursIter) Seq() iter.Seq[utils.Pair[storage.VertexIDWithRID, error]] {
	if !i.se.locker.UpgradeFileLock(i.vertTableToken, txns.GranularLockIntentionShared) {
		err := fmt.Errorf("failed to upgrade file lock")
		return iterWithError(err)
	}

	vertRID, err := i.se.GetVertexRID(i.vertTableToken.GetTxnID(), i.vID, i.vertIndex)
	if err != nil {
		return iterWithError(err)
	}
	pToken := i.se.locker.LockPage(
		i.vertTableToken,
		vertRID.R.PageIdentity().PageID,
		txns.PageLockShared,
	)
	if pToken == nil {
		return iterWithError(fmt.Errorf("failed to lock page"))
	}

	pg, err := i.se.pool.GetPageNoCreate(vertRID.R.PageIdentity())
	if err != nil {
		return iterWithError(err)
	}
	defer i.se.pool.Unpin(vertRID.R.PageIdentity())

	vertData := pg.LockedRead(vertRID.R.SlotNum)
	vertInternalFields, _, err := parseVertexRecordHeader(vertData)
	if err != nil {
		return iterWithError(err)
	}
	if vertInternalFields.DirItemID.IsNil() {
		return func(yield func(utils.Pair[storage.VertexIDWithRID, error]) bool) {
			return
		}
	}

	dirTableMeta, err := i.se.catalog.GetDirectoryTableMeta(i.vertTableToken.GetFileID())
	if err != nil {
		return iterWithError(err)
	}
	

	dirIndex, err := i.se.GetDirectoryIndex(
		i.vertTableToken.GetTxnID(),
		dirTableMeta.FileID,
		i.logger,
	)
	if err != nil {
		return iterWithError(err)
	}

	return func(yield func(utils.Pair[storage.VertexIDWithRID, error]) bool) {
		newDirItemsIter(i.se, vertInternalFields.DirItemID, i.dirFileToken, i.dirIndexI)

	}
}
