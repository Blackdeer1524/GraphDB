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
	edgesIndex    storage.Index
}

func newEdgesIter(
	se *StorageEngine,
	startEdgeID storage.EdgeID,
	edgeFileToken *txns.FileLockToken,
	edgesIndex storage.Index,
) (*edgesIter, error) {
	if !se.locker.UpgradeFileLock(edgeFileToken, txns.GranularLockShared) {
		return nil, fmt.Errorf("failed to upgrade file lock")
	}

	iter := &edgesIter{
		curEdgeID:     startEdgeID,
		se:            se,
		edgeFileToken: edgeFileToken,
		edgesIndex:    edgesIndex,
	}
	return iter, nil
}

func (e *edgesIter) getAndMoveForward() (bool, storage.EdgeInternalFields, error) {
	rid, err := e.se.GetEdgeRID(e.edgeFileToken.GetTxnID(), e.curEdgeID, e.edgesIndex)
	if err != nil {
		return false, storage.EdgeInternalFields{}, err
	}

	pageIdent := rid.R.PageIdentity()
	pg, err := e.se.pool.GetPageNoCreate(pageIdent)
	if err != nil {
		return false, storage.EdgeInternalFields{}, err
	}
	defer e.se.pool.Unpin(pageIdent)

	edgeData := pg.LockedRead(rid.R.SlotNum)
	edgeInternalFields, _, err := parseEdgeRecordHeader(edgeData)
	if err != nil {
		return false, storage.EdgeInternalFields{}, err
	}

	if edgeInternalFields.NextEdgeID.IsNil() {
		return false, edgeInternalFields, nil
	}
	e.curEdgeID = edgeInternalFields.NextEdgeID
	return true, edgeInternalFields, nil
}

func (e *edgesIter) Seq() iter.Seq[utils.Pair[storage.EdgeInternalFields, error]] {
	return func(yield func(utils.Pair[storage.EdgeInternalFields, error]) bool) {
		for {
			hasMore, edgeInternals, err := e.getAndMoveForward()
			if err != nil {
				yield(
					utils.Pair[storage.EdgeInternalFields, error]{
						First:  storage.EdgeInternalFields{},
						Second: err,
					},
				)
				return
			}

			if !hasMore {
				yield(
					utils.Pair[storage.EdgeInternalFields, error]{
						First:  edgeInternals,
						Second: nil,
					},
				)
				return
			}

			if !yield(
				utils.Pair[storage.EdgeInternalFields, error]{First: edgeInternals, Second: nil},
			) {
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
	vID            storage.VertexID
	vertTableToken *txns.FileLockToken
	vertIndex      storage.Index
	logger         common.ITxnLoggerWithContext
}

func newVertexNeighboursIter(
	se *StorageEngine,
	vID storage.VertexID,
	vertTableToken *txns.FileLockToken,
	vertIndex storage.Index,
	logger common.ITxnLoggerWithContext,
) *vertexNeighboursIter {
	iter := &vertexNeighboursIter{
		se:             se,
		vID:            vID,
		vertTableToken: vertTableToken,
		vertIndex:      vertIndex,
		logger:         logger,
	}
	return iter
}

func iterWithError[T any](err error) func(yield func(utils.Pair[T, error]) bool) {
	return func(yield func(utils.Pair[T, error]) bool) {
		var zero T
		yield(utils.Pair[T, error]{First: zero, Second: err})
	}
}

func (i *vertexNeighboursIter) Seq() iter.Seq[utils.Pair[storage.VertexIDWithRID, error]] {
	if !i.se.locker.UpgradeFileLock(i.vertTableToken, txns.GranularLockIntentionShared) {
		err := fmt.Errorf("failed to upgrade file lock")
		return iterWithError[storage.VertexIDWithRID](err)
	}

	vertRID, err := GetVertexRID(i.vertTableToken.GetTxnID(), i.vID, i.vertIndex)
	if err != nil {
		return iterWithError[storage.VertexIDWithRID](err)
	}
	pToken := i.se.locker.LockPage(i.vertTableToken, vertRID.R.PageID, txns.PageLockShared)
	if pToken == nil {
		return iterWithError[storage.VertexIDWithRID](fmt.Errorf("failed to lock page"))
	}

	pg, err := i.se.pool.GetPageNoCreate(vertRID.R.PageIdentity())
	if err != nil {
		return iterWithError[storage.VertexIDWithRID](err)
	}
	vertData := pg.LockedRead(vertRID.R.SlotNum)
	i.se.pool.Unpin(vertRID.R.PageIdentity())

	vertInternalFields, _, err := parseVertexRecordHeader(vertData)
	if err != nil {
		return iterWithError[storage.VertexIDWithRID](err)
	}
	if vertInternalFields.DirItemID.IsNil() {
		return func(yield func(utils.Pair[storage.VertexIDWithRID, error]) bool) {
			return
		}
	}

	dirTableMeta, err := i.se.catalog.GetDirectoryTableMeta(i.vertTableToken.GetFileID())
	if err != nil {
		return iterWithError[storage.VertexIDWithRID](err)
	}

	dirIndex, err := i.se.GetDirectoryInternalIndex(
		i.vertTableToken.GetTxnID(),
		dirTableMeta.FileID,
		i.logger,
	)
	if err != nil {
		return iterWithError[storage.VertexIDWithRID](err)
	}

	cToken := txns.NewNilCatalogLockToken(i.vertTableToken.GetTxnID())
	dirFileToken := txns.NewNilFileLockToken(cToken, dirTableMeta.FileID)
	return func(yield func(utils.Pair[storage.VertexIDWithRID, error]) bool) {
		dirItemsIter, err := newDirItemsIter(
			i.se,
			vertInternalFields.DirItemID,
			dirFileToken,
			dirIndex,
		)
		if err != nil {
			yield(
				utils.Pair[storage.VertexIDWithRID, error]{
					First:  storage.VertexIDWithRID{},
					Second: err,
				},
			)
			return
		}

		for dirItemErr := range dirItemsIter.Seq() {
			dirItem, err := dirItemErr.Destruct()
			if err != nil {
				errItem := utils.Pair[storage.VertexIDWithRID, error]{
					First:  storage.VertexIDWithRID{},
					Second: err,
				}
				yield(errItem)
				return
			}

			if dirItem.EdgeID.IsNil() {
				continue
			}

			edgesFileToken := txns.NewNilFileLockToken(cToken, dirItem.EdgeFileID)
			edgesIndex, err := i.se.GetEdgeInternalIndex(
				i.vertTableToken.GetTxnID(),
				dirItem.EdgeFileID,
				i.logger,
			)
			if err != nil {
				errItem := utils.Pair[storage.VertexIDWithRID, error]{
					First:  storage.VertexIDWithRID{},
					Second: err,
				}
				yield(errItem)
				return
			}
			
			edgeTableName, err := i.se.catalog.GetEdgeTableNameByFileID(dirItem.EdgeFileID)
			if err != nil {
				errItem := utils.Pair[storage.VertexIDWithRID, error]{
					First:  storage.VertexIDWithRID{},
					Second: err,
				}
				yield(errItem)
				return
			}
			
			panic("TODO")

			edgesIter, err := newEdgesIter(i.se, dirItem.EdgeID, edgesFileToken, edgesIndex)
			if err != nil {
				errItem := utils.Pair[storage.VertexIDWithRID, error]{
					First:  storage.VertexIDWithRID{},
					Second: err,
				}
				yield(errItem)
				return
			}

			for edgesInternalsErr := range edgesIter.Seq() {
				edgesInternals, err := edgesInternalsErr.Destruct()
				if err != nil {
					errItem := utils.Pair[storage.VertexIDWithRID, error]{
						First:  storage.VertexIDWithRID{},
						Second: err,
					}
					yield(errItem)
					return
				}

				dstVertexRID, err := GetVertexRID(
					i.vertTableToken.GetTxnID(),
					edgesInternals.DstVertexID,
					,
				)
				if err != nil {
					errItem := utils.Pair[storage.VertexIDWithRID, error]{
						First:  storage.VertexIDWithRID{},
						Second: err,
					}
					yield(errItem)
					return
				}

				item := utils.Pair[storage.VertexIDWithRID, error]{
					First:  dstVertexRID,
					Second: nil,
				}
				if !yield(item) {
					return
				}
			}

		}
	}
}
