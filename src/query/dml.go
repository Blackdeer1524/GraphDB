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
	vertexFileToken *txns.FileLockToken,
	vertexID storage.VertexID,
) ([]byte, error) {
	vertexRID, err := e.se.GetVertexRID(txnID, vertexFileToken.GetFileID(), vertexID)
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
	vertexFileToken *txns.FileLockToken,
	vertexID storage.VertexID,
	schema storage.Schema,
) (engine.VertexInternalFields, map[string]any, error) {
	data, err := e.getSerializedVertex(txnID, vertexFileToken, vertexID)
	if err != nil {
		err = fmt.Errorf("failed to get serialized vertex: %w", err)
		return engine.VertexInternalFields{}, nil, err
	}

	vertexInternalFields, record, err := parseVertexRecord(data, schema)
	if err != nil {
		err = fmt.Errorf("failed to parse vertex record: %w", err)
		return engine.VertexInternalFields{}, nil, err
	}

	return vertexInternalFields, record, nil
}

func (e *Executor) getPageWithFreeSpace(fileID common.FileID) (common.PageID, error) {
	panic("not implemented")
}

func (e *Executor) InsertVertex(
	txnID common.TxnID,
	vertexFileToken *txns.FileLockToken,
	data map[string]any,
	schema storage.Schema,
	ctxLogger common.ITxnLoggerWithContext,
) error {
	pageID, err := e.getPageWithFreeSpace(vertexFileToken.GetFileID())
	if err != nil {
		return fmt.Errorf("failed to get free page: %w", err)
	}
	if e.locker.LockPage(vertexFileToken, pageID, txns.PAGE_LOCK_EXCLUSIVE) == nil {
		return fmt.Errorf("failed to lock page: %w", err)
	}

	pageIdent := common.PageIdentity{
		FileID: vertexFileToken.GetFileID(),
		PageID: pageID,
	}
	pg, err := e.pool.GetPage(pageIdent)
	if err != nil {
		return fmt.Errorf("failed to get page: %w", err)
	}
	defer e.pool.Unpin(pageIdent)

	internalFields := engine.VertexInternalFields{
		ID:        storage.VertexID(uuid.New()),
		DirItemID: storage.NilDirItemID,
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
	vertexFileToken *txns.FileLockToken,
	vertexID storage.VertexID,
	ctxLogger common.ITxnLoggerWithContext,
) error {
	vertexRID, err := e.se.GetVertexRID(txnID, vertexFileToken.GetFileID(), vertexID)
	if err != nil {
		return fmt.Errorf("failed to get vertex RID: %w", err)
	}

	pageIdent := vertexRID.R.PageIdentity()
	pToken := e.locker.LockPage(vertexFileToken, pageIdent.PageID, txns.PAGE_LOCK_EXCLUSIVE)
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
	vertexFileToken *txns.FileLockToken,
	vertexID storage.VertexID,
	newData map[string]any,
	schema storage.Schema,
	ctxLogger common.ITxnLoggerWithContext,
) error {
	vertexRID, err := e.se.GetVertexRID(txnID, vertexFileToken.GetFileID(), vertexID)
	if err != nil {
		return fmt.Errorf("failed to get vertex RID: %w", err)
	}

	pageIdent := vertexRID.R.PageIdentity()
	pToken := e.locker.LockPage(vertexFileToken, pageIdent.PageID, txns.PAGE_LOCK_EXCLUSIVE)
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
				err = fmt.Errorf("failed to serialize vertex record: %w", err)
				return common.NewNilLogRecordLocation(), err
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
				err = fmt.Errorf("failed to parse vertex record header: %w", err)
				return common.NewNilLogRecordLocation(), err
			}

			vertexInternalFields.DirItemID = dirItemID
			udpatedVertexData, err := serializeVertexRecordHeader(vertexInternalFields, tail)
			if err != nil {
				err = fmt.Errorf("failed to serialize vertex record header: %w", err)
				return common.NewNilLogRecordLocation(), err
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
	edgeFileToken *txns.FileLockToken,
	schema storage.Schema,
) (engine.EdgeInternalFields, map[string]any, error) {
	edgeRID, err := e.se.GetEdgeRID(txnID, edgeFileToken.GetFileID(), edgeID)
	if err != nil {
		return engine.EdgeInternalFields{}, nil, fmt.Errorf("failed to get edge RID: %w", err)
	}

	pToken := e.locker.LockPage(
		edgeFileToken,
		edgeRID.R.PageIdentity().PageID,
		txns.PAGE_LOCK_SHARED,
	)
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
		return storage.NilEdgeID, fmt.Errorf("failed to get free page: %w", err)
	}
	if e.locker.LockPage(edgesFileToken, edgePageID, txns.PAGE_LOCK_EXCLUSIVE) == nil {
		return storage.NilEdgeID, fmt.Errorf("failed to lock page: %w", err)
	}

	edgeInternalFields := engine.NewEdgeInternalFields(
		storage.EdgeID(uuid.New()),
		dirItemID,
		srcVertexID,
		dstVertexID,
		storage.NilEdgeID,
		nextEdgeID,
	)

	edgePageIdent := common.PageIdentity{
		FileID: edgesFileToken.GetFileID(),
		PageID: edgePageID,
	}
	edgePg, err := e.pool.GetPage(edgePageIdent)
	if err != nil {
		return storage.NilEdgeID, fmt.Errorf("failed to get page: %w", err)
	}
	defer e.pool.Unpin(edgePageIdent)

	edgeRecordBytes, err := serializeEdgeRecord(edgeInternalFields, edgeFields, schema)
	if err != nil {
		err = fmt.Errorf("failed to serialize edge record: %w", err)
		return storage.NilEdgeID, err
	}
	err = e.pool.WithMarkDirty(
		txnID,
		edgePageIdent,
		edgePg,
		func(lockedPage *page.SlottedPage) (common.LogRecordLocInfo, error) {
			_, loc, err := lockedPage.InsertWithLogs(edgeRecordBytes, edgePageIdent, ctxLogger)
			return loc, err
		},
	)
	if err != nil {
		return storage.NilEdgeID, fmt.Errorf("failed to insert edge record: %w", err)
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

	if srcVertInternalFields.DirItemID.IsNil() {
		_, newDirItemID, err := e.insertEdgeWithDirItem(
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
		err = e.updateVertexDirItemID(txnID, srcVertToken, srcVertexID, newDirItemID, ctxLogger)
		if err != nil {
			return fmt.Errorf("failed to update vertex directory item ID: %w", err)
		}
		return nil
	}

	dirItem, err := e.selectDirectoryItem(
		txnID,
		srcVertDirToken,
		srcVertInternalFields.DirItemID,
	)
	if err != nil {
		return fmt.Errorf("failed to select directory item: %w", err)
	}

	for {
		if dirItem.EdgeFileID != edgesFileToken.GetFileID() {
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

			err = e.updateEdgeNextID(
				txnID,
				edgesFileToken,
				insertedEdgeID,
				dirItem.EdgeID,
				ctxLogger,
			)
			if err != nil {
				return fmt.Errorf("failed to update next edge ID: %w", err)
			}
		}

		err = e.updateDirItemEdgeID(
			txnID,
			srcVertDirToken,
			dirItem.ID,
			insertedEdgeID,
			ctxLogger,
		)
		if err != nil {
			return fmt.Errorf("failed to update directory item: %w", err)
		}
		return nil
	}

	_, newDirItemID, err := e.insertEdgeWithDirItem(
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

	err = e.updateDirectoryItemNextID(txnID, srcVertDirToken, dirItem.ID, newDirItemID, ctxLogger)
	if err != nil {
		return fmt.Errorf("failed to update directory item next ID: %w", err)
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
		return storage.NilDirItemID, fmt.Errorf("failed to get free page: %w", err)
	}
	if e.locker.LockPage(dirToken, pageID, txns.PAGE_LOCK_EXCLUSIVE) == nil {
		return storage.NilDirItemID, fmt.Errorf("failed to lock page: %w", err)
	}

	pageIdent := common.PageIdentity{
		FileID: dirToken.GetFileID(),
		PageID: pageID,
	}

	pToken := e.locker.LockPage(dirToken, pageID, txns.PAGE_LOCK_EXCLUSIVE)
	if pToken == nil {
		return storage.NilDirItemID, fmt.Errorf("failed to lock page: %v", pageIdent)
	}

	pg, err := e.pool.GetPageNoCreate(pageIdent)
	if err != nil {
		return storage.NilDirItemID, fmt.Errorf("failed to get page: %w", err)
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
					NextItemID: storage.NilDirItemID,
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
		return storage.NilDirItemID, fmt.Errorf("failed to insert directory item: %w", err)
	}
	return newDirItemID, nil
}

func (e *Executor) updateDirItemEdgeID(
	txnID common.TxnID,
	dirToken *txns.FileLockToken,
	dirItemID storage.DirItemID,
	edgeID storage.EdgeID,
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
			dirRecordBytes := lockedPage.UnsafeRead(dirRID.R.SlotNum)
			dirItem, err := parseDirectoryRecord(dirRecordBytes)
			if err != nil {
				err = fmt.Errorf(
					"failed to parse directory record: %w",
					err,
				)
				return common.NewNilLogRecordLocation(), err
			}
			dirItem.DirectoryItemDataFields.EdgeID = edgeID
			dirRecordBytes, err = serializeDirectoryRecord(dirItem)
			if err != nil {
				err = fmt.Errorf(
					"failed to serialize directory record: %w",
					err,
				)
				return common.NewNilLogRecordLocation(), err
			}
			return lockedPage.UpdateWithLogs(dirRecordBytes, dirRID.R, ctxLogger)
		},
	)
}

func (e *Executor) updateDirectoryItemNextID(
	txnID common.TxnID,
	dirToken *txns.FileLockToken,
	dirItemID storage.DirItemID,
	newNextItemID storage.DirItemID,
	ctxLogger common.ITxnLoggerWithContext,
) error {
	dirRID, err := e.se.GetDirectoryRID(txnID, dirToken.GetFileID(), dirItemID)
	if err != nil {
		return fmt.Errorf("failed to get directory RID: %w", err)
	}

	pToken := e.locker.LockPage(dirToken, dirRID.R.PageIdentity().PageID, txns.PAGE_LOCK_EXCLUSIVE)
	if pToken == nil {
		return fmt.Errorf("failed to lock page: %v", dirRID.R.PageIdentity())
	}

	pg, err := e.pool.GetPageNoCreate(dirRID.R.PageIdentity())
	if err != nil {
		return fmt.Errorf("failed to get page: %w", err)
	}
	defer e.pool.Unpin(dirRID.R.PageIdentity())

	return e.pool.WithMarkDirty(
		txnID,
		dirRID.R.PageIdentity(),
		pg,
		func(lockedPage *page.SlottedPage) (common.LogRecordLocInfo, error) {
			dirRecordBytes := lockedPage.UnsafeRead(dirRID.R.SlotNum)
			dirItem, err := parseDirectoryRecord(dirRecordBytes)
			if err != nil {
				err = fmt.Errorf("failed to parse directory record: %w", err)
				return common.NewNilLogRecordLocation(), err
			}
			dirItem.NextItemID = newNextItemID
			dirRecordBytes, err = serializeDirectoryRecord(dirItem)
			if err != nil {
				err = fmt.Errorf("failed to serialize directory record: %w", err)
				return common.NewNilLogRecordLocation(), err
			}
			return lockedPage.UpdateWithLogs(dirRecordBytes, dirRID.R, ctxLogger)
		},
	)
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

func (e *Executor) DeleteEdge(
	txnID common.TxnID,
	edgesFileToken *txns.FileLockToken,
	dirFileToken *txns.FileLockToken,
	edgeID storage.EdgeID,
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

	curEdgeData := pg.LockedRead(edgeRID.R.SlotNum)
	curEdgeInternals, _, err := parseEdgeRecordHeader(curEdgeData)
	if err != nil {
		return fmt.Errorf("failed to parse edge record header: %w", err)
	}

	if curEdgeInternals.PrevEdgeID.IsNil() {
		err = e.updateDirItemEdgeID(
			txnID,
			dirFileToken,
			curEdgeInternals.DirectoryItemID,
			curEdgeInternals.NextEdgeID,
			ctxLogger,
		)
		if err != nil {
			return fmt.Errorf("failed to update dir item edge ID: %w", err)
		}
	} else {
		err = e.updateEdgeNextID(
			txnID,
			edgesFileToken,
			curEdgeInternals.PrevEdgeID,
			curEdgeInternals.NextEdgeID,
			ctxLogger,
		)
		if err != nil {
			return fmt.Errorf("failed to update edge next ID: %w", err)
		}
	}

	err = e.updateEdgePrevID(
		txnID,
		edgesFileToken,
		curEdgeInternals.NextEdgeID,
		curEdgeInternals.PrevEdgeID,
		ctxLogger,
	)
	if err != nil {
		return fmt.Errorf("failed to update edge prev ID: %w", err)
	}

	return nil
}

func (e *Executor) UpdateEdge(
	txnID common.TxnID,
	edgesFileToken *txns.FileLockToken,
	edgeID storage.EdgeID,
	edgeFields map[string]any,
	schema storage.Schema,
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
			edgeInternalFields, _, err := parseEdgeRecordHeader(edgeRecordBytes)
			if err != nil {
				err = fmt.Errorf("failed to parse edge record header: %w", err)
				return common.NewNilLogRecordLocation(), err
			}

			newEdgeRecordBytes, err := serializeEdgeRecord(edgeInternalFields, edgeFields, schema)
			if err != nil {
				err = fmt.Errorf("failed to serialize edge record: %w", err)
				return common.NewNilLogRecordLocation(), err
			}

			return lockedPage.UpdateWithLogs(newEdgeRecordBytes, edgeRID.R, ctxLogger)
		},
	)
}

func (e *Executor) updateEdgePrevID(
	txnID common.TxnID,
	edgesFileToken *txns.FileLockToken,
	edgeID storage.EdgeID,
	prevEdgeID storage.EdgeID,
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
				err = fmt.Errorf("failed to parse edge record header: %w", err)
				return common.NewNilLogRecordLocation(), err
			}
			edgeInternalFields.PrevEdgeID = prevEdgeID
			edgeRecordBytes, err = serializeEdgeRecordHeader(edgeInternalFields, tail)
			if err != nil {
				err = fmt.Errorf("failed to serialize edge record header: %w", err)
				return common.NewNilLogRecordLocation(), err
			}
			return lockedPage.UpdateWithLogs(edgeRecordBytes, edgeRID.R, ctxLogger)
		},
	)
}

func (e *Executor) updateEdgeNextID(
	txnID common.TxnID,
	edgesFileToken *txns.FileLockToken,
	edgeID storage.EdgeID,
	nextEdgeID storage.EdgeID,
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
				err = fmt.Errorf("failed to parse edge record header: %w", err)
				return common.NewNilLogRecordLocation(), err
			}
			edgeInternalFields.NextEdgeID = nextEdgeID
			edgeRecordBytes, err = serializeEdgeRecordHeader(edgeInternalFields, tail)
			if err != nil {
				err = fmt.Errorf("failed to serialize edge record header: %w", err)
				return common.NewNilLogRecordLocation(), err
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
				err = fmt.Errorf("failed to parse edge record header: %w", err)
				return common.NewNilLogRecordLocation(), err
			}
			edgeInternalFields.DirectoryItemID = dirItemID
			edgeRecordBytes, err = serializeEdgeRecordHeader(edgeInternalFields, tail)
			if err != nil {
				err = fmt.Errorf("failed to serialize edge record header: %w", err)
				return common.NewNilLogRecordLocation(), err
			}
			return lockedPage.UpdateWithLogs(edgeRecordBytes, edgeRID.R, ctxLogger)
		},
	)

}
