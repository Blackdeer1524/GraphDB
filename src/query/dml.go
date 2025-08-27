package query

import (
	"fmt"

	"github.com/google/uuid"

	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/storage"
	"github.com/Blackdeer1524/GraphDB/src/storage/engine"
	"github.com/Blackdeer1524/GraphDB/src/storage/page"
	"github.com/Blackdeer1524/GraphDB/src/txns"
)

type Index interface {
	Get(key []byte) (common.RecordID, error)
	Delete(key []byte) error
	Insert(key []byte, rid common.RecordID) (common.RecordID, error)
}

func (e *Executor) getSerializedVertex(
	txnID common.TxnID,
	fToken *txns.FileLockToken,
	vertexID storage.VertexID,
) ([]byte, error) {
	vertexRID, err := e.se.GetVertexRID(txnID, fToken.GetFileID(), vertexID)
	if err != nil {
		return nil, fmt.Errorf("failed to get vertex RID: %w", err)
	}

	pageIdent := vertexRID.R.PageIdentity()
	pg, err := e.pool.GetPageNoCreate(pageIdent)
	if err != nil {
		return nil, fmt.Errorf("failed to get page: %w", err)
	}
	defer e.pool.Unpin(pageIdent)

	data := pg.LockedRead(vertexRID.R.SlotNum)
	return data, nil
}

func (e *Executor) SelectVertex(
	txnID common.TxnID,
	fToken *txns.FileLockToken,
	vertexID storage.VertexID,
	schema storage.Schema,
) (engine.VertexInternalFields, map[string]any, error) {
	data, err := e.getSerializedVertex(txnID, fToken, vertexID)
	if err != nil {
		return engine.VertexInternalFields{}, nil, fmt.Errorf(
			"failed to get serialized vertex: %w",
			err,
		)
	}

	vertexInternalFields, record, err := parseVertexRecord(data, schema)
	if err != nil {
		return engine.VertexInternalFields{}, nil, fmt.Errorf(
			"failed to parse vertex record: %w",
			err,
		)
	}

	return vertexInternalFields, record, nil
}

func (e *Executor) getPageWithFreeSpace(fileID common.FileID) (common.PageID, error) {
	panic("not implemented")
}

func (e *Executor) InsertVertex(
	txnID common.TxnID,
	fToken *txns.FileLockToken,
	data map[string]any,
	schema storage.Schema,
	ctxLogger common.ITxnLoggerWithContext,
) error {
	pageID, err := e.getPageWithFreeSpace(fToken.GetFileID())
	if err != nil {
		return fmt.Errorf("failed to get free page: %w", err)
	}
	if e.locker.LockPage(fToken, pageID, txns.PAGE_LOCK_EXCLUSIVE) == nil {
		return fmt.Errorf("failed to lock page: %w", err)
	}

	pageIdent := common.PageIdentity{
		FileID: fToken.GetFileID(),
		PageID: pageID,
	}
	pg, err := e.pool.GetPageNoCreate(pageIdent)
	if err != nil {
		return fmt.Errorf("failed to get page: %w", err)
	}
	defer e.pool.Unpin(pageIdent)

	internalFields := engine.VertexInternalFields{
		ID:          storage.VertexID(uuid.New()),
		DirectoryID: storage.DirItemID(uuid.Nil),
	}

	serializedData, err := serializeVertexRecord(
		internalFields,
		data,
		schema,
	)

	if err != nil {
		return fmt.Errorf("failed to serialize vertex record: %w", err)
	}

	return e.pool.WithMarkDirty(
		txnID,
		pageIdent,
		pg,
		func(lockedPage *page.SlottedPage) (common.LogRecordLocInfo, error) {
			_, loc, err := lockedPage.InsertWithLogs(serializedData, pageIdent, ctxLogger)
			return loc, err
		},
	)
}

func (e *Executor) DeleteVertex(
	txnID common.TxnID,
	fToken *txns.FileLockToken,
	vertexID storage.VertexID,
	ctxLogger common.ITxnLoggerWithContext,
) error {
	vertexRID, err := e.se.GetVertexRID(txnID, fToken.GetFileID(), vertexID)
	if err != nil {
		return fmt.Errorf("failed to get vertex RID: %w", err)
	}

	pageIdent := vertexRID.R.PageIdentity()
	pToken := e.locker.LockPage(fToken, pageIdent.PageID, txns.PAGE_LOCK_EXCLUSIVE)
	if pToken == nil {
		return fmt.Errorf("failed to lock page: %v", pageIdent)
	}

	pg, err := e.pool.GetPageNoCreate(pageIdent)
	if err != nil {
		return fmt.Errorf("failed to get page: %w", err)
	}
	defer e.pool.Unpin(pageIdent)

	return e.pool.WithMarkDirty(
		txnID,
		pageIdent,
		pg,
		func(lockedPage *page.SlottedPage) (common.LogRecordLocInfo, error) {
			return lockedPage.DeleteWithLogs(vertexRID.R, ctxLogger)
		},
	)
}

func (e *Executor) UpdateVertex(
	txnID common.TxnID,
	fToken *txns.FileLockToken,
	vertexID storage.VertexID,
	newData map[string]any,
	schema storage.Schema,
	ctxLogger common.ITxnLoggerWithContext,
) error {
	vertexRID, err := e.se.GetVertexRID(txnID, fToken.GetFileID(), vertexID)
	if err != nil {
		return fmt.Errorf("failed to get vertex RID: %w", err)
	}

	pageIdent := vertexRID.R.PageIdentity()
	pToken := e.locker.LockPage(fToken, pageIdent.PageID, txns.PAGE_LOCK_EXCLUSIVE)
	if pToken == nil {
		return fmt.Errorf("failed to lock page: %v", pageIdent)
	}

	pg, err := e.pool.GetPageNoCreate(pageIdent)
	if err != nil {
		return fmt.Errorf("failed to get page: %w", err)
	}
	defer e.pool.Unpin(pageIdent)

	return e.pool.WithMarkDirty(
		txnID,
		pageIdent,
		pg,
		func(lockedPage *page.SlottedPage) (common.LogRecordLocInfo, error) {
			storedVertex := lockedPage.UnsafeRead(vertexRID.R.SlotNum)
			vertexInternalFields, _, err := parseVertexRecord(storedVertex, schema)
			if err != nil {
				return common.NewNilLogRecordLocation(), fmt.Errorf(
					"failed to parse vertex record: %w",
					err,
				)
			}

			serializedData, err := serializeVertexRecord(
				vertexInternalFields,
				newData,
				schema,
			)

			if err != nil {
				return common.NewNilLogRecordLocation(), fmt.Errorf(
					"failed to serialize vertex record: %w",
					err,
				)
			}

			return lockedPage.UpdateWithLogs(serializedData, vertexRID.R, ctxLogger)
		},
	)
}

func (e *Executor) updateVertexDirItemID(
	txnID common.TxnID,
	srcVertToken *txns.FileLockToken,
	srcVertexID storage.VertexID,
	dirItemID storage.DirItemID,
	ctxLogger common.ITxnLoggerWithContext,
) error {
	vertexRID, err := e.se.GetVertexRID(txnID, srcVertToken.GetFileID(), srcVertexID)
	if err != nil {
		return fmt.Errorf("failed to get vertex RID: %w", err)
	}

	vToken := e.locker.LockPage(
		srcVertToken,
		vertexRID.R.PageIdentity().PageID,
		txns.PAGE_LOCK_EXCLUSIVE,
	)
	if vToken == nil {
		return fmt.Errorf("failed to lock vertex page: %v", vertexRID.R.PageIdentity())
	}

	pg, err := e.pool.GetPageNoCreate(vertexRID.R.PageIdentity())
	if err != nil {
		return fmt.Errorf("failed to get vertex page: %w", err)
	}
	defer e.pool.Unpin(vertexRID.R.PageIdentity())

	err = e.pool.WithMarkDirty(
		txnID,
		vertexRID.R.PageIdentity(),
		pg,
		func(lockedPage *page.SlottedPage) (common.LogRecordLocInfo, error) {
			vertexRecordBytes := lockedPage.UnsafeRead(vertexRID.R.SlotNum)
			vertexInternalFields, tail, err := parseVertexRecordHeader(vertexRecordBytes)
			if err != nil {
				return common.NewNilLogRecordLocation(), fmt.Errorf(
					"failed to parse vertex record header: %w",
					err,
				)
			}

			vertexInternalFields.DirectoryID = dirItemID
			udpatedVertexData, err := serializeVertexRecordHeader(vertexInternalFields, tail)
			if err != nil {
				return common.NewNilLogRecordLocation(), fmt.Errorf(
					"failed to serialize vertex record header: %w",
					err,
				)
			}

			return lockedPage.UpdateWithLogs(udpatedVertexData, vertexRID.R, ctxLogger)
		},
	)
	if err != nil {
		return fmt.Errorf("failed to update vertex: %w", err)
	}

	return nil
}

func (e *Executor) SelectEdge(
	txnID common.TxnID,
	edgeID storage.EdgeID,
	fToken *txns.FileLockToken,
	schema storage.Schema,
) (engine.EdgeInternalFields, map[string]any, error) {
	edgeRID, err := e.se.GetEdgeRID(txnID, fToken.GetFileID(), edgeID)
	if err != nil {
		return engine.EdgeInternalFields{}, nil, fmt.Errorf("failed to get edge RID: %w", err)
	}

	pToken := e.locker.LockPage(fToken, edgeRID.R.PageIdentity().PageID, txns.PAGE_LOCK_SHARED)
	if pToken == nil {
		return engine.EdgeInternalFields{}, nil, fmt.Errorf(
			"failed to lock page: %v",
			edgeRID.R.PageIdentity(),
		)
	}

	pageIdent := edgeRID.R.PageIdentity()
	pg, err := e.pool.GetPageNoCreate(pageIdent)
	if err != nil {
		return engine.EdgeInternalFields{}, nil, fmt.Errorf("failed to get page: %w", err)
	}
	defer e.pool.Unpin(pageIdent)

	data := pg.LockedRead(edgeRID.R.SlotNum)
	return parseEdgeRecord(data, schema)
}

func (e *Executor) insertEdgeHelper(
	txnID common.TxnID,
	srcVertexID storage.VertexID,
	dstVertexID storage.VertexID,
	dirItemID storage.DirItemID,
	edgeFields map[string]any,
	schema storage.Schema,
	nextEdgeID storage.EdgeID,
	edgesFileToken *txns.FileLockToken,
	ctxLogger common.ITxnLoggerWithContext,
) (storage.EdgeID, error) {
	edgePageID, err := e.getPageWithFreeSpace(edgesFileToken.GetFileID())
	if err != nil {
		return storage.EdgeID(uuid.Nil), fmt.Errorf("failed to get free page: %w", err)
	}
	if e.locker.LockPage(edgesFileToken, edgePageID, txns.PAGE_LOCK_EXCLUSIVE) == nil {
		return storage.EdgeID(uuid.Nil), fmt.Errorf("failed to lock page: %w", err)
	}

	edgeInternalFields := engine.NewEdgeInternalFields(
		storage.EdgeID(uuid.New()),
		dirItemID,
		srcVertexID,
		dstVertexID,
		nextEdgeID,
		storage.EdgeID(uuid.Nil),
	)

	edgePageIdent := common.PageIdentity{
		FileID: edgesFileToken.GetFileID(),
		PageID: edgePageID,
	}
	edgePg, err := e.pool.GetPageNoCreate(edgePageIdent)
	if err != nil {
		return storage.EdgeID(uuid.Nil), fmt.Errorf("failed to get page: %w", err)
	}
	defer e.pool.Unpin(edgePageIdent)

	edgeRecordBytes, err := serializeEdgeRecord(edgeInternalFields, edgeFields, schema)
	if err != nil {
		return storage.EdgeID(
				uuid.Nil,
			), fmt.Errorf(
				"failed to serialize edge record: %w",
				err,
			)
	}
	err = e.pool.WithMarkDirty(
		txnID,
		edgePageIdent,
		edgePg,
		func(lockedPage *page.SlottedPage) (common.LogRecordLocInfo, error) {
			_, loc, err := lockedPage.InsertWithLogs(
				edgeRecordBytes,
				edgePageIdent,
				ctxLogger,
			)
			return loc, err
		},
	)
	if err != nil {
		return storage.EdgeID(uuid.Nil), fmt.Errorf("failed to insert edge record: %w", err)
	}

	return edgeInternalFields.ID, nil

}

func (e *Executor) insertEdgeWithDirItem(
	txnID common.TxnID,
	srcVertexID storage.VertexID,
	dstVertexID storage.VertexID,
	edgesFileToken *txns.FileLockToken,
	srcVertDirToken *txns.FileLockToken,
	edgeFields map[string]any,
	schema storage.Schema,
	prevDirItemID storage.DirItemID,
	ctxLogger common.ITxnLoggerWithContext,
) (storage.EdgeID, storage.DirItemID, error) {
	newEdgeID, err := e.insertEdgeHelper(
		txnID,
		srcVertexID,
		dstVertexID,
		storage.NilDirItemID,
		edgeFields,
		schema,
		storage.NilEdgeID,
		edgesFileToken,
		ctxLogger,
	)
	if err != nil {
		err = fmt.Errorf("failed to insert very first edge: %w", err)
		return storage.NilEdgeID, storage.NilDirItemID, err
	}

	dirItemDataFields := engine.DirectoryItemDataFields{
		VertexID:   srcVertexID,
		EdgeFileID: edgesFileToken.GetFileID(),
		EdgeID:     newEdgeID,
	}
	dirItemID, err := e.insertDirectoryItem(
		txnID,
		srcVertDirToken,
		dirItemDataFields,
		prevDirItemID,
		ctxLogger,
	)
	if err != nil {
		err = fmt.Errorf("failed to create directory: %w", err)
		return storage.NilEdgeID, storage.NilDirItemID, err
	}

	err = e.updateEdgeDirItemID(txnID, edgesFileToken, newEdgeID, dirItemID, ctxLogger)
	if err != nil {
		err = fmt.Errorf("failed to update edge directory item ID: %w", err)
		return storage.NilEdgeID, storage.NilDirItemID, err
	}

	return newEdgeID, dirItemID, nil
}

func (e *Executor) InsertEdge(
	txnID common.TxnID,
	srcVertexID storage.VertexID,
	dstVertexID storage.VertexID,
	edgeFields map[string]any,
	schema storage.Schema,
	srcVertToken *txns.FileLockToken,
	srcVertDirToken *txns.FileLockToken,
	edgesFileToken *txns.FileLockToken,
	ctxLogger common.ITxnLoggerWithContext,
) error {
	srcVertInternalFields, _, err := e.SelectVertex(
		txnID,
		srcVertToken,
		srcVertexID,
		schema,
	)
	if err != nil {
		return fmt.Errorf("failed to get serialized src vertex: %w", err)
	}

	if srcVertInternalFields.DirectoryID.IsNil() {
		_, dirItemID, err := e.insertEdgeWithDirItem(
			txnID,
			srcVertexID,
			dstVertexID,
			edgesFileToken,
			srcVertDirToken,
			edgeFields,
			schema,
			storage.NilDirItemID,
			ctxLogger,
		)
		if err != nil {
			return fmt.Errorf("failed to insert edge with directory item: %w", err)
		}
		err = e.updateVertexDirItemID(txnID, srcVertToken, srcVertexID, dirItemID, ctxLogger)
		if err != nil {
			return fmt.Errorf("failed to update vertex directory item ID: %w", err)
		}
		return nil
	}

	dirItem, err := e.selectDirectoryItem(
		txnID,
		srcVertDirToken,
		srcVertInternalFields.DirectoryID,
	)
	if err != nil {
		return fmt.Errorf("failed to select directory item: %w", err)
	}

	for {
		if dirItem.EdgeFileID == edgesFileToken.GetFileID() {
			insertedEdgeID, err := e.insertEdgeHelper(
				txnID,
				srcVertexID,
				dstVertexID,
				dirItem.ID,
				edgeFields,
				schema,
				dirItem.EdgeID,
				edgesFileToken,
				ctxLogger,
			)
			if err != nil {
				return fmt.Errorf("failed to insert edge: %w", err)
			}

			if !dirItem.EdgeID.IsNil() {
				err = e.updateEdgePrevID(
					txnID,
					edgesFileToken,
					dirItem.EdgeID,
					insertedEdgeID,
					ctxLogger,
				)
				if err != nil {
					return fmt.Errorf("failed to update prev edge ID: %w", err)
				}
			}

			dirItem.EdgeID = insertedEdgeID
			err = e.updateDirectoryItem(txnID, srcVertDirToken, dirItem.ID, dirItem, ctxLogger)
			if err != nil {
				return fmt.Errorf("failed to update directory item: %w", err)
			}
			return nil
		}

		if dirItem.NextItemID.IsNil() {
			break
		}

		dirItem, err = e.selectDirectoryItem(
			txnID,
			srcVertDirToken,
			dirItem.NextItemID,
		)
		if err != nil {
			return fmt.Errorf("failed to select directory item: %w", err)
		}
	}

	_, _, err = e.insertEdgeWithDirItem(
		txnID,
		srcVertexID,
		dstVertexID,
		edgesFileToken,
		srcVertDirToken,
		edgeFields,
		schema,
		dirItem.ID,
		ctxLogger,
	)
	if err != nil {
		return fmt.Errorf("failed to insert edge with directory item: %w", err)
	}
	return nil
}

func (e *Executor) selectDirectoryItem(
	txnID common.TxnID,
	dirToken *txns.FileLockToken,
	dirItemID storage.DirItemID,
) (engine.DirectoryItem, error) {
	dirRID, err := e.se.GetDirectoryRID(txnID, dirToken.GetFileID(), dirItemID)
	if err != nil {
		return engine.DirectoryItem{}, fmt.Errorf("failed to get directory RID: %w", err)
	}

	pageIdent := dirRID.R.PageIdentity()
	pToken := e.locker.LockPage(dirToken, pageIdent.PageID, txns.PAGE_LOCK_SHARED)
	if pToken == nil {
		return engine.DirectoryItem{}, fmt.Errorf("failed to lock page: %v", pageIdent)
	}

	pg, err := e.pool.GetPageNoCreate(pageIdent)
	if err != nil {
		return engine.DirectoryItem{}, fmt.Errorf("failed to get page: %w", err)
	}
	defer e.pool.Unpin(pageIdent)
	data := pg.LockedRead(dirRID.R.SlotNum)
	return parseDirectoryRecord(data)
}

func (e *Executor) insertDirectoryItem(
	txnID common.TxnID,
	dirToken *txns.FileLockToken,
	dirItem engine.DirectoryItemDataFields,
	prevDirItemID storage.DirItemID,
	ctxLogger common.ITxnLoggerWithContext,
) (storage.DirItemID, error) {
	pageID, err := e.getPageWithFreeSpace(dirToken.GetFileID())
	if err != nil {
		return storage.DirItemID(uuid.Nil), fmt.Errorf("failed to get free page: %w", err)
	}
	if e.locker.LockPage(dirToken, pageID, txns.PAGE_LOCK_EXCLUSIVE) == nil {
		return storage.DirItemID(uuid.Nil), fmt.Errorf("failed to lock page: %w", err)
	}

	pageIdent := common.PageIdentity{
		FileID: dirToken.GetFileID(),
		PageID: pageID,
	}

	pToken := e.locker.LockPage(dirToken, pageID, txns.PAGE_LOCK_EXCLUSIVE)
	if pToken == nil {
		return storage.DirItemID(uuid.Nil), fmt.Errorf("failed to lock page: %v", pageIdent)
	}

	pg, err := e.pool.GetPageNoCreate(pageIdent)
	if err != nil {
		return storage.DirItemID(uuid.Nil), fmt.Errorf("failed to get page: %w", err)
	}
	defer e.pool.Unpin(pageIdent)

	newDirItemID := storage.DirItemID(uuid.New())
	err = e.pool.WithMarkDirty(
		txnID,
		pageIdent,
		pg,
		func(lockedPage *page.SlottedPage) (common.LogRecordLocInfo, error) {
			fullDirItem := engine.DirectoryItem{
				DirectoryItemInternalFields: engine.DirectoryItemInternalFields{
					ID:         newDirItemID,
					NextItemID: storage.DirItemID(uuid.Nil),
					PrevItemID: prevDirItemID,
				},
				DirectoryItemDataFields: dirItem,
			}

			directoryRecordBytes, err := serializeDirectoryRecord(fullDirItem)
			if err != nil {
				return common.NewNilLogRecordLocation(), fmt.Errorf(
					"failed to serialize directory record: %w",
					err,
				)
			}

			_, loc, err := lockedPage.InsertWithLogs(directoryRecordBytes, pageIdent, ctxLogger)
			return loc, err
		},
	)
	if err != nil {
		return storage.DirItemID(uuid.Nil), fmt.Errorf("failed to insert directory item: %w", err)
	}
	return newDirItemID, nil
}

func (e *Executor) updateDirectoryItem(
	txnID common.TxnID,
	dirToken *txns.FileLockToken,
	dirItemID storage.DirItemID,
	dirItem engine.DirectoryItem,
	ctxLogger common.ITxnLoggerWithContext,
) error {
	dirRID, err := e.se.GetDirectoryRID(txnID, dirToken.GetFileID(), dirItemID)
	if err != nil {
		return fmt.Errorf("failed to get directory RID: %w", err)
	}

	pageIdent := dirRID.R.PageIdentity()
	pToken := e.locker.LockPage(dirToken, pageIdent.PageID, txns.PAGE_LOCK_EXCLUSIVE)
	if pToken == nil {
		return fmt.Errorf("failed to lock page: %v", pageIdent)
	}

	pg, err := e.pool.GetPageNoCreate(pageIdent)
	if err != nil {
		return fmt.Errorf("failed to get page: %w", err)
	}
	defer e.pool.Unpin(pageIdent)

	return e.pool.WithMarkDirty(
		txnID,
		pageIdent,
		pg,
		func(lockedPage *page.SlottedPage) (common.LogRecordLocInfo, error) {
			directoryRecordBytes, err := serializeDirectoryRecord(dirItem)
			if err != nil {
				return common.NewNilLogRecordLocation(), fmt.Errorf(
					"failed to serialize directory record: %w",
					err,
				)
			}
			return lockedPage.UpdateWithLogs(directoryRecordBytes, dirRID.R, ctxLogger)
		},
	)
}

func (e *Executor) deleteDirectoryItem(
	txnID common.TxnID,
	dirToken *txns.FileLockToken,
	dirItemID storage.DirItemID,
	ctxLogger common.ITxnLoggerWithContext,
) error {
	dirRID, err := e.se.GetDirectoryRID(txnID, dirToken.GetFileID(), dirItemID)
	if err != nil {
		return fmt.Errorf("failed to get directory RID: %w", err)
	}

	pageIdent := dirRID.R.PageIdentity()
	pToken := e.locker.LockPage(dirToken, pageIdent.PageID, txns.PAGE_LOCK_EXCLUSIVE)
	if pToken == nil {
		return fmt.Errorf("failed to lock page: %v", pageIdent)
	}

	pg, err := e.pool.GetPageNoCreate(pageIdent)
	if err != nil {
		return fmt.Errorf("failed to get page: %w", err)
	}
	defer e.pool.Unpin(pageIdent)

	return e.pool.WithMarkDirty(
		txnID,
		pageIdent,
		pg,
		func(lockedPage *page.SlottedPage) (common.LogRecordLocInfo, error) {
			return lockedPage.DeleteWithLogs(dirRID.R, ctxLogger)
		},
	)
}

func (e *Executor) DeleteEdge(
	txnID common.TxnID,
	fToken *txns.FileLockToken,
	edgeID storage.EdgeID,
	ctxLogger common.ITxnLoggerWithContext,
) error {
	edgeRID, err := e.se.GetEdgeRID(txnID, fToken.GetFileID(), edgeID)
	if err != nil {
		return fmt.Errorf("failed to get edge RID: %w", err)
	}

	pageIdent := edgeRID.R.PageIdentity()
	pToken := e.locker.LockPage(fToken, pageIdent.PageID, txns.PAGE_LOCK_EXCLUSIVE)
	if pToken == nil {
		return fmt.Errorf("failed to lock page: %v", pageIdent)
	}

	pg, err := e.pool.GetPageNoCreate(pageIdent)
	if err != nil {
		return fmt.Errorf("failed to get page: %w", err)
	}
	defer e.pool.Unpin(pageIdent)

	curEdgeData := pg.LockedRead(edgeRID.R.SlotNum)
	curEdgeInternalFields, _, err := parseEdgeRecordHeader(curEdgeData)
	if err != nil {
		return fmt.Errorf("failed to parse edge record header: %w", err)
	}

	err = e.updateEdgeNextID(
		txnID,
		fToken,
		curEdgeInternalFields.PrevEdgeID,
		curEdgeInternalFields.NextEdgeID,
		ctxLogger,
	)
	if err != nil {
		return fmt.Errorf("failed to update edge next ID: %w", err)
	}
	e.updateEdgePrevID(
		txnID,
		fToken,
		curEdgeInternalFields.NextEdgeID,
		curEdgeInternalFields.PrevEdgeID,
		ctxLogger,
	)
	if err != nil {
		return fmt.Errorf("failed to update edge prev ID: %w", err)
	}
	return nil
}

func (e *Executor) UpdateEdge() error {
	return nil
}

func (e *Executor) updateEdgePrevID(
	txnID common.TxnID,
	fToken *txns.FileLockToken,
	edgeID storage.EdgeID,
	prevEdgeID storage.EdgeID,
	ctxLogger common.ITxnLoggerWithContext,
) error {
	edgeRID, err := e.se.GetEdgeRID(txnID, fToken.GetFileID(), edgeID)
	if err != nil {
		return fmt.Errorf("failed to get edge RID: %w", err)
	}

	pageIdent := edgeRID.R.PageIdentity()
	pToken := e.locker.LockPage(fToken, pageIdent.PageID, txns.PAGE_LOCK_EXCLUSIVE)
	if pToken == nil {
		return fmt.Errorf("failed to lock page: %v", pageIdent)
	}

	pg, err := e.pool.GetPageNoCreate(pageIdent)
	if err != nil {
		return fmt.Errorf("failed to get page: %w", err)
	}
	defer e.pool.Unpin(pageIdent)

	return e.pool.WithMarkDirty(
		txnID,
		pageIdent,
		pg,
		func(lockedPage *page.SlottedPage) (common.LogRecordLocInfo, error) {
			edgeRecordBytes := lockedPage.UnsafeRead(edgeRID.R.SlotNum)
			edgeInternalFields, tail, err := parseEdgeRecordHeader(edgeRecordBytes)
			if err != nil {
				return common.NewNilLogRecordLocation(), fmt.Errorf(
					"failed to parse edge record header: %w",
					err,
				)
			}
			edgeInternalFields.PrevEdgeID = prevEdgeID
			edgeRecordBytes, err = serializeEdgeRecordHeader(edgeInternalFields, tail)
			if err != nil {
				return common.NewNilLogRecordLocation(), fmt.Errorf(
					"failed to serialize edge record header: %w",
					err,
				)
			}
			return lockedPage.UpdateWithLogs(edgeRecordBytes, edgeRID.R, ctxLogger)
		},
	)
}

func (e *Executor) updateEdgeNextID(
	txnID common.TxnID,
	fToken *txns.FileLockToken,
	edgeID storage.EdgeID,
	nextEdgeID storage.EdgeID,
	ctxLogger common.ITxnLoggerWithContext,
) error {
	edgeRID, err := e.se.GetEdgeRID(txnID, fToken.GetFileID(), edgeID)
	if err != nil {
		return fmt.Errorf("failed to get edge RID: %w", err)
	}

	pageIdent := edgeRID.R.PageIdentity()
	pToken := e.locker.LockPage(fToken, pageIdent.PageID, txns.PAGE_LOCK_EXCLUSIVE)
	if pToken == nil {
		return fmt.Errorf("failed to lock page: %v", pageIdent)
	}

	pg, err := e.pool.GetPageNoCreate(pageIdent)
	if err != nil {
		return fmt.Errorf("failed to get page: %w", err)
	}
	defer e.pool.Unpin(pageIdent)

	return e.pool.WithMarkDirty(
		txnID,
		pageIdent,
		pg,
		func(lockedPage *page.SlottedPage) (common.LogRecordLocInfo, error) {
			edgeRecordBytes := lockedPage.UnsafeRead(edgeRID.R.SlotNum)
			edgeInternalFields, tail, err := parseEdgeRecordHeader(edgeRecordBytes)
			if err != nil {
				return common.NewNilLogRecordLocation(), fmt.Errorf(
					"failed to parse edge record header: %w",
					err,
				)
			}
			edgeInternalFields.NextEdgeID = nextEdgeID
			edgeRecordBytes, err = serializeEdgeRecordHeader(edgeInternalFields, tail)
			if err != nil {
				return common.NewNilLogRecordLocation(), fmt.Errorf(
					"failed to serialize edge record header: %w",
					err,
				)
			}
			return lockedPage.UpdateWithLogs(edgeRecordBytes, edgeRID.R, ctxLogger)
		},
	)
}

func (e *Executor) updateEdgeDirItemID(
	txnID common.TxnID,
	edgesFileToken *txns.FileLockToken,
	edgeID storage.EdgeID,
	dirItemID storage.DirItemID,
	ctxLogger common.ITxnLoggerWithContext,
) error {
	edgeRID, err := e.se.GetEdgeRID(txnID, edgesFileToken.GetFileID(), edgeID)
	if err != nil {
		return fmt.Errorf("failed to get edge RID: %w", err)
	}

	pageIdent := edgeRID.R.PageIdentity()
	pToken := e.locker.LockPage(edgesFileToken, pageIdent.PageID, txns.PAGE_LOCK_EXCLUSIVE)
	if pToken == nil {
		return fmt.Errorf("failed to lock page: %v", pageIdent)
	}

	pg, err := e.pool.GetPageNoCreate(pageIdent)
	if err != nil {
		return fmt.Errorf("failed to get page: %w", err)
	}
	defer e.pool.Unpin(pageIdent)

	return e.pool.WithMarkDirty(
		txnID,
		pageIdent,
		pg,
		func(lockedPage *page.SlottedPage) (common.LogRecordLocInfo, error) {
			edgeRecordBytes := lockedPage.UnsafeRead(edgeRID.R.SlotNum)
			edgeInternalFields, tail, err := parseEdgeRecordHeader(edgeRecordBytes)
			if err != nil {
				return common.NewNilLogRecordLocation(), fmt.Errorf(
					"failed to parse edge record header: %w",
					err,
				)
			}
			edgeInternalFields.DirectoryItemID = dirItemID
			edgeRecordBytes, err = serializeEdgeRecordHeader(edgeInternalFields, tail)
			if err != nil {
				return common.NewNilLogRecordLocation(), fmt.Errorf(
					"failed to serialize edge record header: %w",
					err,
				)
			}
			return lockedPage.UpdateWithLogs(edgeRecordBytes, edgeRID.R, ctxLogger)
		},
	)

}
