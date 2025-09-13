package engine

import (
	"errors"
	"fmt"
	"iter"

	"github.com/Blackdeer1524/GraphDB/src/bufferpool"
	"github.com/Blackdeer1524/GraphDB/src/pkg/assert"
	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/pkg/utils"
	"github.com/Blackdeer1524/GraphDB/src/storage"
	"github.com/Blackdeer1524/GraphDB/src/storage/disk"
	"github.com/Blackdeer1524/GraphDB/src/storage/page"
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
	curEdgeID     storage.EdgeSystemID
	schema        storage.Schema
	edgeFilter    storage.EdgeFilter
	edgeFileToken *txns.FileLockToken
	edgesIndex    storage.Index
}

func newEdgesIter(
	se *StorageEngine,
	startEdgeID storage.EdgeSystemID,
	schema storage.Schema,
	edgeFilter storage.EdgeFilter,
	edgeFileToken *txns.FileLockToken,
	edgesIndex storage.Index,
) (*edgesIter, error) {
	assert.Assert(!startEdgeID.IsNil(), "start edge ID shouldn't be nil")

	if !se.locker.UpgradeFileLock(edgeFileToken, txns.GranularLockShared) {
		err := fmt.Errorf("failed to upgrade file lock: %w", txns.ErrDeadlockPrevention)
		return nil, err
	}

	iter := &edgesIter{
		curEdgeID:     startEdgeID,
		se:            se,
		schema:        schema,
		edgeFilter:    edgeFilter,
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

	edgeSystemFields, edgeFields, err := parseEdgeRecord(edgeData, e.schema)
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
			EdgeSystemFields: edgeSystemFields,
			Data:             edgeFields,
		},
	}

	e.curEdgeID = edgeSystemFields.NextEdgeID
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
				yieldErrorTripple(err, yield)
				return
			}

			if !e.edgeFilter(&edge.Second) {
				if hasMore {
					continue
				}
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
	curDirItemID storage.DirItemSystemID
	dirFileToken *txns.FileLockToken
	dirIndex     storage.Index
}

func newDirItemsIter(
	se *StorageEngine,
	startDirItemID storage.DirItemSystemID,
	dirFileToken *txns.FileLockToken,
	dirIndex storage.Index,
) (*dirItemsIter, error) {
	assert.Assert(!startDirItemID.IsNil(), "start directory item ID shouldn't be nil")
	if !se.locker.UpgradeFileLock(dirFileToken, txns.GranularLockShared) {
		err := fmt.Errorf("failed to upgrade file lock: %w", txns.ErrDeadlockPrevention)
		return nil, err
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
	se              *StorageEngine
	startVertID     storage.VertexSystemID
	edgeFilter      storage.EdgeFilter
	vertTableToken  *txns.FileLockToken
	vertSystemIndex storage.Index
	logger          common.ITxnLoggerWithContext
}

var _ storage.NeighborEdgesIter = &neighboursEdgesIter{}

func newNeighboursEdgesIter(
	se *StorageEngine,
	startVertID storage.VertexSystemID,
	edgeFilter storage.EdgeFilter,
	vertTableToken *txns.FileLockToken,
	vertSystemIndex storage.Index,
	logger common.ITxnLoggerWithContext,
) *neighboursEdgesIter {
	iter := &neighboursEdgesIter{
		se:              se,
		logger:          logger,
		startVertID:     startVertID,
		edgeFilter:      edgeFilter,
		vertTableToken:  vertTableToken,
		vertSystemIndex: vertSystemIndex,
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
	if !i.se.locker.UpgradeFileLock(i.vertTableToken, txns.GranularLockShared) {
		err := fmt.Errorf("failed to upgrade file lock: %w", txns.ErrDeadlockPrevention)
		return iterWithErrorTriple[common.RecordID, storage.Edge](err)
	}
	txnID := i.vertTableToken.GetTxnID()

	vertRID, err := GetVertexRID(txnID, i.startVertID, i.vertSystemIndex)
	if err != nil {
		return iterWithErrorTriple[common.RecordID, storage.Edge](err)
	}
	pToken := i.se.locker.LockPage(i.vertTableToken, vertRID.R.PageID, txns.PageLockShared)
	if pToken == nil {
		err := fmt.Errorf("failed to lock page: %w", txns.ErrDeadlockPrevention)
		return iterWithErrorTriple[common.RecordID, storage.Edge](err)
	}

	pg, err := i.se.pool.GetPageNoCreate(vertRID.R.PageIdentity())
	if err != nil {
		return iterWithErrorTriple[common.RecordID, storage.Edge](err)
	}
	vertData := pg.LockedRead(vertRID.R.SlotNum)
	i.se.pool.Unpin(vertRID.R.PageIdentity())

	vertSystemFields, _, err := parseVertexRecordHeader(vertData)
	if err != nil {
		return iterWithErrorTriple[common.RecordID, storage.Edge](err)
	}
	if vertSystemFields.DirItemID.IsNil() {
		return func(yield func(utils.Triple[common.RecordID, storage.Edge, error]) bool) {
		}
	}

	cToken := txns.NewNilCatalogLockToken(txnID)
	dirTableMeta, err := i.se.GetDirTableMeta(cToken, i.vertTableToken.GetFileID())
	if err != nil {
		return iterWithErrorTriple[common.RecordID, storage.Edge](err)
	}

	dirIndex, err := i.se.GetDirTableSystemIndex(
		txnID,
		dirTableMeta.FileID,
		cToken,
		i.logger,
	)
	if err != nil {
		return iterWithErrorTriple[common.RecordID, storage.Edge](err)
	}

	dirFileToken := txns.NewNilFileLockToken(cToken, dirTableMeta.FileID)
	return func(yield func(utils.Triple[common.RecordID, storage.Edge, error]) bool) {
		defer dirIndex.Close()

		dirItemsIter, err := newDirItemsIter(
			i.se,
			vertSystemFields.DirItemID,
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
			continueFlag, err := func() (bool, error) {
				edgesIndex, err := i.se.GetEdgeTableSystemIndex(
					txnID,
					dirItem.EdgeFileID,
					cToken,
					i.logger,
				)
				if err != nil {
					return false, err
				}
				defer edgesIndex.Close()

				edgesMeta, err := i.se.GetEdgeTableMetaByFileID(dirItem.EdgeFileID, cToken)
				if err != nil {
					return false, err
				}

				edgesIter, err := newEdgesIter(
					i.se,
					dirItem.EdgeID,
					edgesMeta.Schema,
					i.edgeFilter,
					edgesFileToken,
					edgesIndex,
				)
				if err != nil {
					return false, err
				}

				for ridEdgesErr := range edgesIter.Seq() {
					_, _, err := ridEdgesErr.Destruct()
					if err != nil {
						return false, err
					}

					if !yield(ridEdgesErr) {
						return false, nil
					}
				}
				return true, nil
			}()

			if err != nil {
				yieldErrorTripple(err, yield)
				return
			}
			if !continueFlag {
				return
			}
		}
	}
}

func (i *neighboursEdgesIter) Close() error {
	return nil
}

type neighbourVertexIDsIter struct {
	se             *StorageEngine
	vID            storage.VertexSystemID
	vertTableToken *txns.FileLockToken
	vertIndex      storage.Index
	edgeFilter     storage.EdgeFilter
	logger         common.ITxnLoggerWithContext
}

var _ storage.NeighborIDIter = &neighbourVertexIDsIter{}

func newNeighbourVertexIDsIter(
	se *StorageEngine,
	vID storage.VertexSystemID,
	vertTableToken *txns.FileLockToken,
	vertIndex storage.Index,
	edgeFilter storage.EdgeFilter,
	logger common.ITxnLoggerWithContext,
) *neighbourVertexIDsIter {
	iter := &neighbourVertexIDsIter{
		se:             se,
		logger:         logger,
		vID:            vID,
		vertTableToken: vertTableToken,
		vertIndex:      vertIndex,
		edgeFilter:     edgeFilter,
	}
	return iter
}

func (i *neighbourVertexIDsIter) Seq() iter.Seq[utils.Pair[storage.VertexSystemIDWithRID, error]] {
	cToken := i.vertTableToken.GetCatalogLockToken()
	edgesIter := newNeighboursEdgesIter(
		i.se,
		i.vID,
		i.edgeFilter,
		i.vertTableToken,
		i.vertIndex,
		i.logger,
	)

	return func(yield func(utils.Pair[storage.VertexSystemIDWithRID, error]) bool) {
		lastEdgeFileID := common.NilFileID
		var dstVertIndex storage.Index
		defer func() {
			if dstVertIndex != nil {
				dstVertIndex.Close()
			}
		}()

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

				if dstVertIndex != nil {
					dstVertIndex.Close()
				}
				dstVertIndex, err = i.se.GetVertexTableSystemIndex(
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

			dstVertRID, err := GetVertexRID(
				cToken.GetTxnID(),
				edge.DstVertexID,
				dstVertIndex,
			)
			if err != nil {
				yieldErrorPair(err, yield)
				return
			}

			dstVertInfo := utils.Pair[storage.VertexSystemIDWithRID, error]{
				First:  dstVertRID,
				Second: nil,
			}
			if !yield(dstVertInfo) {
				return
			}
		}
	}
}

func (i *neighbourVertexIDsIter) Close() error {
	return nil
}

type neighbourVertexIter struct {
	se                 *StorageEngine
	vID                storage.VertexSystemID
	initVertTableToken *txns.FileLockToken
	vertIndex          storage.Index
	vertexFilter       storage.VertexFilter
	edgeFilter         storage.EdgeFilter
	locker             *txns.LockManager
	logger             common.ITxnLoggerWithContext

	debugAsserts bool
}

var _ storage.VerticesIter = &neighbourVertexIter{}

func newNeighbourVertexIter(
	se *StorageEngine,
	vID storage.VertexSystemID,
	vertTableToken *txns.FileLockToken,
	vertIndex storage.Index,
	vertexFilter storage.VertexFilter,
	edgeFilter storage.EdgeFilter,
	locker *txns.LockManager,
	logger common.ITxnLoggerWithContext,
	debugAsserts bool,
) *neighbourVertexIter {
	iter := &neighbourVertexIter{
		se:                 se,
		logger:             logger,
		vID:                vID,
		initVertTableToken: vertTableToken,
		vertIndex:          vertIndex,
		vertexFilter:       vertexFilter,
		edgeFilter:         edgeFilter,
		locker:             locker,
		debugAsserts:       debugAsserts,
	}
	return iter
}

func (i *neighbourVertexIter) Seq() iter.Seq[utils.Triple[common.RecordID, storage.Vertex, error]] {
	return func(yield func(utils.Triple[common.RecordID, storage.Vertex, error]) bool) {
		neighbourVertexIDsIter := newNeighbourVertexIDsIter(
			i.se,
			i.vID,
			i.initVertTableToken,
			i.vertIndex,
			i.edgeFilter,
			i.logger,
		)
		cToken := i.initVertTableToken.GetCatalogLockToken()

		var dstVertexFileToken *txns.FileLockToken
		lastVertexFileID := common.NilFileID
		for vertexIDWithRIDErr := range neighbourVertexIDsIter.Seq() {
			vertexIDWithRID, err := vertexIDWithRIDErr.Destruct()
			if err != nil {
				yieldErrorTripple(err, yield)
				return
			}
			if vertexIDWithRID.R.FileID != lastVertexFileID {
				lastVertexFileID = vertexIDWithRID.R.FileID
				dstVertexFileToken = i.locker.LockFile(
					cToken,
					vertexIDWithRID.R.FileID,
					txns.GranularLockShared,
				)
				if dstVertexFileToken == nil {
					err := fmt.Errorf("failed to lock file: %w", txns.ErrDeadlockPrevention)
					yieldErrorTripple(err, yield)
					return
				}
			}

			if i.debugAsserts {
				pToken := i.locker.LockPage(
					dstVertexFileToken,
					vertexIDWithRID.R.PageID,
					txns.PageLockShared,
				)
				assert.Assert(
					pToken != nil,
					"should have locked the page (the file is locked in the shared mode)",
				)
			}

			vertexPageIdent := vertexIDWithRID.R.PageIdentity()
			pg, err := i.se.pool.GetPageNoCreate(vertexPageIdent)
			if err != nil {
				yieldErrorTripple(err, yield)
				return
			}
			vertexData := pg.LockedRead(vertexIDWithRID.R.SlotNum)
			i.se.pool.Unpin(vertexPageIdent)

			vertexMeta, err := i.se.GetVertexTableMetaByFileID(
				vertexIDWithRID.R.FileID,
				cToken,
			)
			if err != nil {
				yieldErrorTripple(err, yield)
				return
			}

			vertexSystemFields, vertexFields, err := parseVertexRecord(
				vertexData,
				vertexMeta.Schema,
			)
			if err != nil {
				yieldErrorTripple(err, yield)
				return
			}

			vertex := storage.Vertex{
				VertexSystemFields: vertexSystemFields,
				Data:               vertexFields,
			}

			if !i.vertexFilter(&vertex) {
				continue
			}

			vertexInfo := utils.Triple[common.RecordID, storage.Vertex, error]{
				First:  vertexIDWithRID.R,
				Second: vertex,
				Third:  nil,
			}

			if !yield(vertexInfo) {
				return
			}
		}
	}
}

func (i *neighbourVertexIter) Close() error {
	return nil
}

type vertexTableScanIter struct {
	se           storage.Engine
	pool         bufferpool.BufferPool
	vertexFilter storage.VertexFilter
	tableToken   *txns.FileLockToken
	tableSchema  storage.Schema
	locker       *txns.LockManager

	debugAsserts bool
}

var _ storage.VerticesIter = &vertexTableScanIter{}

func newVertexTableScanIter(
	se *StorageEngine,
	pool bufferpool.BufferPool,
	vertexFilter storage.VertexFilter,
	vertTableToken *txns.FileLockToken,
	vartTableSchema storage.Schema,
	locker *txns.LockManager,

	debugAsserts bool,
) *vertexTableScanIter {
	return &vertexTableScanIter{
		se:           se,
		pool:         pool,
		vertexFilter: vertexFilter,
		tableToken:   vertTableToken,
		tableSchema:  vartTableSchema,
		locker:       locker,

		debugAsserts: debugAsserts,
	}
}

func (iter *vertexTableScanIter) Seq() iter.Seq[utils.Triple[common.RecordID, storage.Vertex, error]] {
	return func(yield func(utils.Triple[common.RecordID, storage.Vertex, error]) bool) {
		if !iter.locker.UpgradeFileLock(iter.tableToken, txns.GranularLockShared) {
			err := fmt.Errorf("failed to upgrade file lock: %w", txns.ErrDeadlockPrevention)
			yieldErrorTripple(err, yield)
			return
		}

		pageID := uint64(0)
		for {
			pageIdent := common.PageIdentity{
				FileID: iter.tableToken.GetFileID(),
				PageID: common.PageID(pageID),
			}

			if iter.debugAsserts {
				pToken := iter.locker.LockPage(
					iter.tableToken,
					common.PageID(pageID),
					txns.PageLockShared,
				)
				assert.Assert(
					pToken != nil,
					"should have locked the page (the table is locked in the shared mode)",
				)
			}

			pg, err := iter.pool.GetPageNoCreate(pageIdent)
			if err != nil {
				if !errors.Is(err, disk.ErrNoSuchPage) {
					yieldErrorTripple(err, yield)
				}
				return
			}
			continueFlag, err := func() (bool, error) {
				defer iter.pool.Unpin(pageIdent)
				pg.RLock()
				defer pg.RUnlock()
				recordID := common.RecordID{
					FileID:  pageIdent.FileID,
					PageID:  pageIdent.PageID,
					SlotNum: 0,
				}
				for i := uint16(0); i < pg.NumSlots(); i++ {
					if pg.SlotInfo(i) != page.SlotStatusInserted {
						continue
					}

					data := pg.UnsafeRead(i)
					vertexSystemFields, vertexFields, err := parseVertexRecord(
						data,
						iter.tableSchema,
					)
					if err != nil {
						return false, err
					}
					vertex := storage.Vertex{
						VertexSystemFields: vertexSystemFields,
						Data:               vertexFields,
					}
					if !iter.vertexFilter(&vertex) {
						continue
					}

					recordID.SlotNum = i
					item := utils.Triple[common.RecordID, storage.Vertex, error]{
						First:  recordID,
						Second: vertex,
						Third:  nil,
					}
					if !yield(item) {
						return false, nil
					}
				}
				return true, nil
			}()
			if err != nil {
				yieldErrorTripple(err, yield)
				return
			}
			if !continueFlag {
				return
			}
			pageID++
		}
	}
}

func (iter *vertexTableScanIter) Close() error {
	return nil
}

/// ================================

type edgeTableScanIter struct {
	se          storage.Engine
	pool        bufferpool.BufferPool
	edgeFilter  storage.EdgeFilter
	tableToken  *txns.FileLockToken
	tableSchema storage.Schema
	locker      *txns.LockManager

	debugAsserts bool
}

var _ storage.EdgesIter = &edgeTableScanIter{}

func newEdgeTableScanIter(
	se *StorageEngine,
	pool bufferpool.BufferPool,
	edgeFilter storage.EdgeFilter,
	edgeTableToken *txns.FileLockToken,
	edgeTableSchema storage.Schema,
	locker *txns.LockManager,

	debugAsserts bool,
) *edgeTableScanIter {
	return &edgeTableScanIter{
		se:          se,
		pool:        pool,
		edgeFilter:  edgeFilter,
		tableToken:  edgeTableToken,
		tableSchema: edgeTableSchema,
		locker:      locker,

		debugAsserts: debugAsserts,
	}
}

func (iter *edgeTableScanIter) Seq() iter.Seq[utils.Triple[common.RecordID, storage.Edge, error]] {
	return func(yield func(utils.Triple[common.RecordID, storage.Edge, error]) bool) {
		if !iter.locker.UpgradeFileLock(iter.tableToken, txns.GranularLockShared) {
			err := fmt.Errorf("failed to upgrade file lock: %w", txns.ErrDeadlockPrevention)
			yieldErrorTripple(err, yield)
			return
		}

		pageID := uint64(0)
		for {
			pageIdent := common.PageIdentity{
				FileID: iter.tableToken.GetFileID(),
				PageID: common.PageID(pageID),
			}

			if iter.debugAsserts {
				pToken := iter.locker.LockPage(
					iter.tableToken,
					common.PageID(pageID),
					txns.PageLockShared,
				)
				assert.Assert(
					pToken != nil,
					"should have locked the page (the table is locked in the shared mode)",
				)
			}

			pg, err := iter.pool.GetPageNoCreate(pageIdent)
			if err != nil {
				if !errors.Is(err, disk.ErrNoSuchPage) {
					yieldErrorTripple(err, yield)
				}
				return
			}
			continueFlag, err := func() (bool, error) {
				defer iter.pool.Unpin(pageIdent)

				pg.RLock()
				defer pg.RUnlock()
				recordID := common.RecordID{
					FileID:  pageIdent.FileID,
					PageID:  pageIdent.PageID,
					SlotNum: 0,
				}
				for i := uint16(0); i < pg.NumSlots(); i++ {
					if pg.SlotInfo(i) != page.SlotStatusInserted {
						continue
					}

					data := pg.UnsafeRead(i)
					edgeSystemFields, edgeFields, err := parseEdgeRecord(
						data,
						iter.tableSchema,
					)
					if err != nil {
						return false, err
					}
					edge := storage.Edge{
						EdgeSystemFields: edgeSystemFields,
						Data:             edgeFields,
					}
					if !iter.edgeFilter(&edge) {
						continue
					}

					recordID.SlotNum = i
					item := utils.Triple[common.RecordID, storage.Edge, error]{
						First:  recordID,
						Second: edge,
						Third:  nil,
					}
					if !yield(item) {
						return false, nil
					}
				}
				return true, nil
			}()
			if err != nil {
				yieldErrorTripple(err, yield)
				return
			}
			if !continueFlag {
				return
			}
			pageID++
		}
	}
}

func (iter *edgeTableScanIter) Close() error {
	return nil
}

type dirItemsScanIter struct {
	se            storage.Engine
	pool          bufferpool.BufferPool
	dirTableToken *txns.FileLockToken
	locker        *txns.LockManager

	debugAsserts bool
}

var _ storage.DirItemsIter = &dirItemsScanIter{}

func newDirItemsScanIter(
	se *StorageEngine,
	pool bufferpool.BufferPool,
	dirTableToken *txns.FileLockToken,
	locker *txns.LockManager,
	debugAsserts bool,
) *dirItemsScanIter {
	return &dirItemsScanIter{
		se:            se,
		pool:          pool,
		dirTableToken: dirTableToken,
		locker:        locker,
		debugAsserts:  debugAsserts,
	}
}

func (iter *dirItemsScanIter) Seq() iter.Seq[utils.Triple[common.RecordID, storage.DirectoryItem, error]] {
	return func(yield func(utils.Triple[common.RecordID, storage.DirectoryItem, error]) bool) {
		if !iter.locker.UpgradeFileLock(iter.dirTableToken, txns.GranularLockShared) {
			err := fmt.Errorf("failed to upgrade file lock: %w", txns.ErrDeadlockPrevention)
			yieldErrorTripple(err, yield)
			return
		}

		pageID := uint64(0)
		for {
			pageIdent := common.PageIdentity{
				FileID: iter.dirTableToken.GetFileID(),
				PageID: common.PageID(pageID),
			}

			if iter.debugAsserts {
				pToken := iter.locker.LockPage(
					iter.dirTableToken,
					common.PageID(pageID),
					txns.PageLockShared,
				)
				assert.Assert(
					pToken != nil,
					"should have locked the page (the table is locked in the shared mode)",
				)
			}

			pg, err := iter.pool.GetPageNoCreate(pageIdent)
			if errors.Is(err, disk.ErrNoSuchPage) {
				return
			} else if err != nil {
				yieldErrorTripple(err, yield)
				return
			}
			continueFlag, err := func() (bool, error) {
				defer iter.pool.Unpin(pageIdent)
				pg.RLock()
				defer pg.RUnlock()

				for i := uint16(0); i < pg.NumSlots(); i++ {
					if pg.SlotInfo(i) != page.SlotStatusInserted {
						continue
					}

					data := pg.UnsafeRead(i)
					dirItem, err := parseDirectoryRecord(data)
					if err != nil {
						return false, err
					}

					item := utils.Triple[common.RecordID, storage.DirectoryItem, error]{
						First: common.RecordID{
							FileID:  pageIdent.FileID,
							PageID:  pageIdent.PageID,
							SlotNum: i,
						},
						Second: dirItem,
						Third:  nil,
					}

					if !yield(item) {
						return false, nil
					}
				}
				return true, nil
			}()
			if err != nil {
				yieldErrorTripple(err, yield)
				return
			}
			if !continueFlag {
				return
			}
		}
	}
}

func (iter *dirItemsScanIter) Close() error {
	return nil
}
