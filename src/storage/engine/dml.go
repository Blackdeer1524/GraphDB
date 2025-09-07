package engine

import (
	"fmt"

	"github.com/google/uuid"

	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/storage"
	"github.com/Blackdeer1524/GraphDB/src/storage/page"
	"github.com/Blackdeer1524/GraphDB/src/txns"
)

func (s *StorageEngine) getPageWithFreeSpace(fileID common.FileID) (common.PageID, error) {
	panic("not implemented")
}

func GetVertexRID(
	txnID common.TxnID,
	vertexID storage.VertexSystemID,
	vertexIndex storage.Index,
) (storage.VertexSystemIDWithRID, error) {
	b, err := vertexID.MarshalBinary()
	if err != nil {
		return storage.VertexSystemIDWithRID{}, fmt.Errorf("failed to marshal vertex ID: %w", err)
	}

	rid, err := vertexIndex.Get(b)
	if err != nil {
		return storage.VertexSystemIDWithRID{}, fmt.Errorf("failed to get vertex RID: %w", err)
	}

	res := storage.VertexSystemIDWithRID{
		V: vertexID,
		R: rid,
	}
	return res, nil
}

func GetEdgeRID(
	txnID common.TxnID,
	edgeID storage.EdgeSystemID,
	edgeIndex storage.Index,
) (storage.EdgeSystemIDWithRID, error) {
	b, err := edgeID.MarshalBinary()
	if err != nil {
		return storage.EdgeSystemIDWithRID{}, fmt.Errorf("failed to marshal edge ID: %w", err)
	}

	rid, err := edgeIndex.Get(b)
	if err != nil {
		return storage.EdgeSystemIDWithRID{}, fmt.Errorf("failed to get edge RID: %w", err)
	}

	res := storage.EdgeSystemIDWithRID{
		E: edgeID,
		R: rid,
	}

	return res, nil
}

func GetDirectoryRID(
	txnID common.TxnID,
	dirItemID storage.DirItemSystemID,
	dirSystemIndex storage.Index,
) (storage.DirItemSystemIDWithRID, error) {
	b, err := dirItemID.MarshalBinary()
	if err != nil {
		return storage.DirItemSystemIDWithRID{}, fmt.Errorf(
			"failed to marshal directory ID: %w",
			err,
		)
	}

	rid, err := dirSystemIndex.Get(b)
	if err != nil {
		return storage.DirItemSystemIDWithRID{}, fmt.Errorf(
			"failed to get directory RID: %w",
			err,
		)
	}

	res := storage.DirItemSystemIDWithRID{
		D: dirItemID,
		R: rid,
	}

	return res, nil
}

func (s *StorageEngine) getSerializedVertex(
	txnID common.TxnID,
	vertexID storage.VertexSystemID,
	vertexIndex storage.Index,
) ([]byte, error) {
	vertexRID, err := GetVertexRID(txnID, vertexID, vertexIndex)
	if err != nil {
		return nil, fmt.Errorf("failed to get vertex RID: %w", err)
	}

	pageIdent := vertexRID.R.PageIdentity()
	pg, err := s.pool.GetPageNoCreate(pageIdent)
	if err != nil {
		return nil, fmt.Errorf("failed to get page: %w", err)
	}
	defer s.pool.Unpin(pageIdent)

	data := pg.LockedRead(vertexRID.R.SlotNum)
	return data, nil
}

func (s *StorageEngine) SelectVertex(
	txnID common.TxnID,
	vertexID storage.VertexSystemID,
	vertexIndex storage.Index,
	schema storage.Schema,
) (storage.VertexSystemFields, map[string]any, error) {
	data, err := s.getSerializedVertex(txnID, vertexID, vertexIndex)
	if err != nil {
		err = fmt.Errorf("failed to get serialized vertex: %w", err)
		return storage.VertexSystemFields{}, nil, err
	}

	vertexSystemFields, record, err := parseVertexRecord(data, schema)
	if err != nil {
		err = fmt.Errorf("failed to parse vertex record: %w", err)
		return storage.VertexSystemFields{}, nil, err
	}

	return vertexSystemFields, record, nil
}

func (s *StorageEngine) InsertVertex(
	txnID common.TxnID,

	data map[string]any,
	schema storage.Schema,

	vertexFileToken *txns.FileLockToken,
	vertexIndex storage.Index,

	ctxLogger common.ITxnLoggerWithContext,
) (storage.VertexSystemID, error) {
	pageID, err := s.getPageWithFreeSpace(vertexFileToken.GetFileID())
	if err != nil {
		return storage.NilVertexID, fmt.Errorf("failed to get free page: %w", err)
	}
	if s.locker.LockPage(vertexFileToken, pageID, txns.PageLockExclusive) == nil {
		return storage.NilVertexID, fmt.Errorf("failed to lock page: %w", err)
	}

	pageIdent := common.PageIdentity{
		FileID: vertexFileToken.GetFileID(),
		PageID: pageID,
	}
	pg, err := s.pool.GetPage(pageIdent)
	if err != nil {
		return storage.NilVertexID, fmt.Errorf("failed to get page: %w", err)
	}
	defer s.pool.Unpin(pageIdent)

	internalFields := storage.NewVertexSystemFields(
		storage.VertexSystemID(uuid.New()),
		storage.NilDirItemID,
	)
	serializedData, err := serializeVertexRecord(
		internalFields,
		data,
		schema,
	)
	if err != nil {
		return storage.NilVertexID, fmt.Errorf("failed to serialize vertex record: %w", err)
	}

	slot := uint16(0)
	err = s.pool.WithMarkDirty(
		txnID,
		pageIdent,
		pg,
		func(lockedPage *page.SlottedPage) (common.LogRecordLocInfo, error) {
			var loc common.LogRecordLocInfo
			var err error
			slot, loc, err = lockedPage.InsertWithLogs(serializedData, pageIdent, ctxLogger)
			return loc, err
		},
	)
	if err != nil {
		return storage.NilVertexID, fmt.Errorf("failed to insert vertex record: %w", err)
	}

	vertexRID := common.RecordID{
		FileID:  pageIdent.FileID,
		PageID:  pageIdent.PageID,
		SlotNum: slot,
	}

	marshalledID, err := internalFields.ID.MarshalBinary()
	if err != nil {
		return storage.NilVertexID, fmt.Errorf("failed to marshal vertex ID: %w", err)
	}
	err = vertexIndex.Insert(marshalledID, vertexRID)
	if err != nil {
		return storage.NilVertexID, fmt.Errorf("failed to insert vertex record: %w", err)
	}
	return internalFields.ID, nil
}

func (s *StorageEngine) DeleteVertex(
	txnID common.TxnID,
	vertexID storage.VertexSystemID,
	vertexFileToken *txns.FileLockToken,
	vertexIndex storage.Index,
	ctxLogger common.ITxnLoggerWithContext,
) error {
	vertexRID, err := GetVertexRID(txnID, vertexID, vertexIndex)
	if err != nil {
		return fmt.Errorf("failed to get vertex RID: %w", err)
	}

	pageIdent := vertexRID.R.PageIdentity()
	pToken := s.locker.LockPage(vertexFileToken, pageIdent.PageID, txns.PageLockExclusive)
	if pToken == nil {
		return fmt.Errorf("failed to lock page: %v", pageIdent)
	}

	pg, err := s.pool.GetPageNoCreate(pageIdent)
	if err != nil {
		return fmt.Errorf("failed to get page: %w", err)
	}
	defer s.pool.Unpin(pageIdent)

	err = s.pool.WithMarkDirty(
		txnID,
		pageIdent,
		pg,
		func(lockedPage *page.SlottedPage) (common.LogRecordLocInfo, error) {
			return lockedPage.DeleteWithLogs(vertexRID.R, ctxLogger)
		},
	)
	if err != nil {
		return fmt.Errorf("failed to delete vertex record: %w", err)
	}

	marshalledID, err := vertexID.MarshalBinary()
	if err != nil {
		return fmt.Errorf("failed to marshal vertex ID: %w", err)
	}

	err = vertexIndex.Delete(marshalledID)
	if err != nil {
		return fmt.Errorf("failed to delete vertex index: %w", err)
	}
	return nil
}

func (s *StorageEngine) UpdateVertex(
	txnID common.TxnID,
	vertexID storage.VertexSystemID,
	newData map[string]any,
	schema storage.Schema,
	vertexFileToken *txns.FileLockToken,
	vertexIndex storage.Index,
	ctxLogger common.ITxnLoggerWithContext,
) error {
	vertexRID, err := GetVertexRID(txnID, vertexID, vertexIndex)
	if err != nil {
		return fmt.Errorf("failed to get vertex RID: %w", err)
	}

	pageIdent := vertexRID.R.PageIdentity()
	pToken := s.locker.LockPage(vertexFileToken, pageIdent.PageID, txns.PageLockExclusive)
	if pToken == nil {
		return fmt.Errorf("failed to lock page: %v", pageIdent)
	}

	pg, err := s.pool.GetPageNoCreate(pageIdent)
	if err != nil {
		return fmt.Errorf("failed to get page: %w", err)
	}
	defer s.pool.Unpin(pageIdent)

	return s.pool.WithMarkDirty(
		txnID,
		pageIdent,
		pg,
		func(lockedPage *page.SlottedPage) (common.LogRecordLocInfo, error) {
			storedVertex := lockedPage.UnsafeRead(vertexRID.R.SlotNum)
			vertexSystemFields, _, err := parseVertexRecord(storedVertex, schema)
			if err != nil {
				return common.NewNilLogRecordLocation(), fmt.Errorf(
					"failed to parse vertex record: %w",
					err,
				)
			}

			serializedData, err := serializeVertexRecord(
				vertexSystemFields,
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

func (s *StorageEngine) updateVertexDirItemID(
	txnID common.TxnID,

	srcVertexID storage.VertexSystemID,
	dirItemID storage.DirItemSystemID,

	srcVertToken *txns.FileLockToken,
	vertexIndex storage.Index,
	ctxLogger common.ITxnLoggerWithContext,
) error {
	vertexRID, err := GetVertexRID(txnID, srcVertexID, vertexIndex)
	if err != nil {
		return fmt.Errorf("failed to get vertex RID: %w", err)
	}

	vToken := s.locker.LockPage(
		srcVertToken,
		vertexRID.R.PageIdentity().PageID,
		txns.PageLockExclusive,
	)
	if vToken == nil {
		return fmt.Errorf("failed to lock vertex page: %v", vertexRID.R.PageIdentity())
	}

	pg, err := s.pool.GetPageNoCreate(vertexRID.R.PageIdentity())
	if err != nil {
		return fmt.Errorf("failed to get vertex page: %w", err)
	}
	defer s.pool.Unpin(vertexRID.R.PageIdentity())

	err = s.pool.WithMarkDirty(
		txnID,
		vertexRID.R.PageIdentity(),
		pg,
		func(lockedPage *page.SlottedPage) (common.LogRecordLocInfo, error) {
			vertexRecordBytes := lockedPage.UnsafeRead(vertexRID.R.SlotNum)
			vertexSystemFields, tail, err := parseVertexRecordHeader(vertexRecordBytes)
			if err != nil {
				err = fmt.Errorf("failed to parse vertex record header: %w", err)
				return common.NewNilLogRecordLocation(), err
			}

			vertexSystemFields.DirItemID = dirItemID
			udpatedVertexData, err := serializeVertexRecordHeader(vertexSystemFields, tail)
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

func (s *StorageEngine) SelectEdge(
	txnID common.TxnID,
	edgeID storage.EdgeSystemID,
	edgeFileToken *txns.FileLockToken,
	edgeSystemIndex storage.Index,
	schema storage.Schema,
) (storage.EdgeSystemFields, map[string]any, error) {
	edgeRID, err := GetEdgeRID(txnID, edgeID, edgeSystemIndex)
	if err != nil {
		return storage.EdgeSystemFields{}, nil, fmt.Errorf("failed to get edge RID: %w", err)
	}

	pToken := s.locker.LockPage(
		edgeFileToken,
		edgeRID.R.PageIdentity().PageID,
		txns.PageLockShared,
	)
	if pToken == nil {
		err = fmt.Errorf("failed to lock page: %v", edgeRID.R.PageIdentity())
		return storage.EdgeSystemFields{}, nil, err
	}

	pageIdent := edgeRID.R.PageIdentity()
	pg, err := s.pool.GetPageNoCreate(pageIdent)
	if err != nil {
		return storage.EdgeSystemFields{}, nil, fmt.Errorf("failed to get page: %w", err)
	}
	defer s.pool.Unpin(pageIdent)

	data := pg.LockedRead(edgeRID.R.SlotNum)
	return parseEdgeRecord(data, schema)
}

func (s *StorageEngine) insertEdgeHelper(
	txnID common.TxnID,
	srcVertexID storage.VertexSystemID,
	dstVertexID storage.VertexSystemID,
	dirItemID storage.DirItemSystemID,
	nextEdgeID storage.EdgeSystemID,
	edgeFields map[string]any,
	schema storage.Schema,
	edgesFileToken *txns.FileLockToken,
	edgeSystemIndex storage.Index,
	ctxLogger common.ITxnLoggerWithContext,
) (storage.EdgeSystemID, error) {
	edgePageID, err := s.getPageWithFreeSpace(edgesFileToken.GetFileID())
	if err != nil {
		return storage.NilEdgeID, fmt.Errorf("failed to get free page: %w", err)
	}
	if s.locker.LockPage(edgesFileToken, edgePageID, txns.PageLockExclusive) == nil {
		return storage.NilEdgeID, fmt.Errorf("failed to lock page: %w", err)
	}

	edgeSystemFields := storage.NewEdgeSystemFields(
		storage.EdgeSystemID(uuid.New()),
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
	edgePg, err := s.pool.GetPage(edgePageIdent)
	if err != nil {
		return storage.NilEdgeID, fmt.Errorf("failed to get page: %w", err)
	}
	defer s.pool.Unpin(edgePageIdent)

	edgeRecordBytes, err := serializeEdgeRecord(edgeSystemFields, edgeFields, schema)
	if err != nil {
		err = fmt.Errorf("failed to serialize edge record: %w", err)
		return storage.NilEdgeID, err
	}

	slot := uint16(0)
	err = s.pool.WithMarkDirty(
		txnID,
		edgePageIdent,
		edgePg,
		func(lockedPage *page.SlottedPage) (common.LogRecordLocInfo, error) {
			var loc common.LogRecordLocInfo
			var err error
			slot, loc, err = lockedPage.InsertWithLogs(edgeRecordBytes, edgePageIdent, ctxLogger)
			return loc, err
		},
	)

	if err != nil {
		return storage.NilEdgeID, fmt.Errorf("failed to insert edge record: %w", err)
	}

	marshalledEdgeID, err := edgeSystemFields.ID.MarshalBinary()
	if err != nil {
		return storage.NilEdgeID, fmt.Errorf("failed to marshal edge ID: %w", err)
	}

	edgeRID := common.RecordID{
		FileID:  edgePageIdent.FileID,
		PageID:  edgePageIdent.PageID,
		SlotNum: slot,
	}
	err = edgeSystemIndex.Insert(marshalledEdgeID, edgeRID)
	if err != nil {
		return storage.NilEdgeID, fmt.Errorf("failed to insert edge info into index: %w", err)
	}

	return edgeSystemFields.ID, nil
}

func (s *StorageEngine) insertEdgeWithDirItem(
	txnID common.TxnID,

	srcVertexID storage.VertexSystemID,
	dstVertexID storage.VertexSystemID,
	edgeFields map[string]any,
	schema storage.Schema,
	edgesFileToken *txns.FileLockToken,
	edgeSystemIndex storage.Index,

	prevDirItemID storage.DirItemSystemID,
	nextDirItemID storage.DirItemSystemID,
	srcVertDirToken *txns.FileLockToken,
	srcVertDirSystemIndex storage.Index,

	ctxLogger common.ITxnLoggerWithContext,
) (storage.EdgeSystemID, storage.DirItemSystemID, error) {
	newEdgeID, err := s.insertEdgeHelper(
		txnID,
		srcVertexID,
		dstVertexID,
		storage.NilDirItemID,
		storage.NilEdgeID,
		edgeFields,
		schema,
		edgesFileToken,
		edgeSystemIndex,
		ctxLogger,
	)
	if err != nil {
		err = fmt.Errorf("failed to insert very first edge: %w", err)
		return storage.NilEdgeID, storage.NilDirItemID, err
	}

	dirItemGraphFields := storage.NewDirectoryItemGraphFields(
		srcVertexID,
		edgesFileToken.GetFileID(),
		newEdgeID,
	)
	dirItemID, err := s.insertDirectoryItem(
		txnID,
		dirItemGraphFields,
		prevDirItemID,
		nextDirItemID,
		srcVertDirToken,
		srcVertDirSystemIndex,
		ctxLogger,
	)
	if err != nil {
		err = fmt.Errorf("failed to create directory: %w", err)
		return storage.NilEdgeID, storage.NilDirItemID, err
	}

	err = s.updateEdgeDirItemID(
		txnID,
		newEdgeID,
		dirItemID,
		edgesFileToken,
		edgeSystemIndex,
		ctxLogger,
	)
	if err != nil {
		err = fmt.Errorf("failed to update edge directory item ID: %w", err)
		return storage.NilEdgeID, storage.NilDirItemID, err
	}

	return newEdgeID, dirItemID, nil
}

func (s *StorageEngine) InsertEdge(
	txnID common.TxnID,

	srcVertexID storage.VertexSystemID,
	dstVertexID storage.VertexSystemID,
	edgeFields map[string]any,
	schema storage.Schema,

	srcVertToken *txns.FileLockToken,
	srcVertSystemIndex storage.Index,

	srcVertDirToken *txns.FileLockToken,
	srcVertDirSystemIndex storage.Index,

	edgesFileToken *txns.FileLockToken,
	edgeSystemIndex storage.Index,

	ctxLogger common.ITxnLoggerWithContext,
) (storage.EdgeSystemID, error) {
	srcVertSystemFields, _, err := s.SelectVertex(txnID, srcVertexID, srcVertSystemIndex, schema)
	if err != nil {
		return storage.NilEdgeID, fmt.Errorf("failed to get serialized src vertex: %w", err)
	}

	if srcVertSystemFields.DirItemID.IsNil() {
		newEdgeID, newDirItemID, err := s.insertEdgeWithDirItem(
			txnID,

			srcVertexID,
			dstVertexID,
			edgeFields,
			schema,
			edgesFileToken,
			edgeSystemIndex,

			storage.NilDirItemID,
			storage.NilDirItemID,

			srcVertDirToken,
			srcVertDirSystemIndex,

			ctxLogger,
		)
		if err != nil {
			return storage.NilEdgeID, fmt.Errorf(
				"failed to insert edge with directory item: %w",
				err,
			)
		}
		err = s.updateVertexDirItemID(
			txnID,
			srcVertexID,
			newDirItemID,
			srcVertToken,
			srcVertSystemIndex,
			ctxLogger,
		)
		if err != nil {
			return storage.NilEdgeID, fmt.Errorf(
				"failed to update vertex directory item ID: %w",
				err,
			)
		}
		return newEdgeID, nil
	}

	dirItem, err := s.selectDirectoryItem(
		txnID,
		srcVertSystemFields.DirItemID,
		srcVertDirToken,
		srcVertDirSystemIndex,
	)
	if err != nil {
		return storage.NilEdgeID, fmt.Errorf("failed to select directory item: %w", err)
	}

	for {
		if dirItem.EdgeFileID != edgesFileToken.GetFileID() {
			if dirItem.NextItemID.IsNil() {
				break
			}
			dirItem, err = s.selectDirectoryItem(
				txnID,
				dirItem.NextItemID,
				srcVertDirToken,
				srcVertDirSystemIndex,
			)
			if err != nil {
				return storage.NilEdgeID, fmt.Errorf("failed to select directory item: %w", err)
			}
			continue
		}
		insertedEdgeID, err := s.insertEdgeHelper(
			txnID,

			srcVertexID,
			dstVertexID,
			dirItem.ID,
			dirItem.EdgeID,
			edgeFields,
			schema,

			edgesFileToken,
			edgeSystemIndex,

			ctxLogger,
		)
		if err != nil {
			return storage.NilEdgeID, fmt.Errorf("failed to insert edge: %w", err)
		}

		if !dirItem.EdgeID.IsNil() {
			err = s.updateEdgePrevID(
				txnID,

				dirItem.EdgeID,
				insertedEdgeID,

				edgesFileToken,
				edgeSystemIndex,

				ctxLogger,
			)
			if err != nil {
				return storage.NilEdgeID, fmt.Errorf("failed to update prev edge ID: %w", err)
			}
		}

		err = s.updateDirItemEdgeID(
			txnID,
			dirItem.ID,
			insertedEdgeID,
			srcVertDirToken,
			srcVertDirSystemIndex,
			ctxLogger,
		)
		if err != nil {
			return storage.NilEdgeID, fmt.Errorf("failed to update directory item: %w", err)
		}
		return insertedEdgeID, nil
	}

	insertedEdgeID, newDirItemID, err := s.insertEdgeWithDirItem(
		txnID,

		srcVertexID,
		dstVertexID,
		edgeFields,
		schema,

		edgesFileToken,
		edgeSystemIndex,

		dirItem.ID,
		storage.NilDirItemID,

		srcVertDirToken,
		srcVertDirSystemIndex,
		ctxLogger,
	)
	if err != nil {
		return storage.NilEdgeID, fmt.Errorf("failed to insert edge with directory item: %w", err)
	}

	err = s.updateDirectoryItemNextID(
		txnID,
		dirItem.ID,
		newDirItemID,
		srcVertDirToken,
		srcVertDirSystemIndex,
		ctxLogger,
	)
	if err != nil {
		return storage.NilEdgeID, fmt.Errorf("failed to update directory item next ID: %w", err)
	}

	return insertedEdgeID, nil
}

func (s *StorageEngine) selectDirectoryItem(
	txnID common.TxnID,
	dirItemID storage.DirItemSystemID,
	dirToken *txns.FileLockToken,
	dirSystemIndex storage.Index,
) (storage.DirectoryItem, error) {
	dirRID, err := GetDirectoryRID(txnID, dirItemID, dirSystemIndex)
	if err != nil {
		return storage.DirectoryItem{}, fmt.Errorf("failed to get directory RID: %w", err)
	}

	pageIdent := dirRID.R.PageIdentity()
	pToken := s.locker.LockPage(dirToken, pageIdent.PageID, txns.PageLockShared)
	if pToken == nil {
		return storage.DirectoryItem{}, fmt.Errorf("failed to lock page: %v", pageIdent)
	}

	pg, err := s.pool.GetPageNoCreate(pageIdent)
	if err != nil {
		return storage.DirectoryItem{}, fmt.Errorf("failed to get page: %w", err)
	}
	defer s.pool.Unpin(pageIdent)
	data := pg.LockedRead(dirRID.R.SlotNum)
	return parseDirectoryRecord(data)
}

func (s *StorageEngine) insertDirectoryItem(
	txnID common.TxnID,

	dirItemGraphFields storage.DirectoryItemGraphFields,
	prevDirItemID storage.DirItemSystemID,
	nextDirItemID storage.DirItemSystemID,

	dirFileToken *txns.FileLockToken,
	dirSystemIndex storage.Index,

	ctxLogger common.ITxnLoggerWithContext,
) (storage.DirItemSystemID, error) {
	dirItemSystemFields := storage.NewDirectoryItemSystemFields(
		storage.DirItemSystemID(uuid.New()),
		nextDirItemID,
		prevDirItemID,
	)
	dirItem := storage.DirectoryItem{
		DirectoryItemSystemFields: dirItemSystemFields,
		DirectoryItemGraphFields:  dirItemGraphFields,
	}
	directoryRecordBytes, err := serializeDirectoryRecord(dirItem)
	if err != nil {
		err = fmt.Errorf("failed to serialize directory record: %w", err)
		return storage.NilDirItemID, err
	}

	pageID, err := s.getPageWithFreeSpace(dirFileToken.GetFileID())
	if err != nil {
		return storage.NilDirItemID, fmt.Errorf("failed to get free page: %w", err)
	}

	if s.locker.LockPage(dirFileToken, pageID, txns.PageLockExclusive) == nil {
		return storage.NilDirItemID, fmt.Errorf("failed to lock page: %w", err)
	}

	pageIdent := common.PageIdentity{
		FileID: dirFileToken.GetFileID(),
		PageID: pageID,
	}

	pToken := s.locker.LockPage(dirFileToken, pageID, txns.PageLockExclusive)
	if pToken == nil {
		return storage.NilDirItemID, fmt.Errorf("failed to lock page: %v", pageIdent)
	}

	pg, err := s.pool.GetPageNoCreate(pageIdent)
	if err != nil {
		return storage.NilDirItemID, fmt.Errorf("failed to get page: %w", err)
	}
	defer s.pool.Unpin(pageIdent)

	slot := uint16(0)
	err = s.pool.WithMarkDirty(
		txnID,
		pageIdent,
		pg,
		func(lockedPage *page.SlottedPage) (common.LogRecordLocInfo, error) {
			var loc common.LogRecordLocInfo
			var err error
			slot, loc, err = lockedPage.InsertWithLogs(directoryRecordBytes, pageIdent, ctxLogger)
			return loc, err
		},
	)
	if err != nil {
		return storage.NilDirItemID, fmt.Errorf("failed to insert directory item: %w", err)
	}

	dirRID := common.RecordID{
		FileID:  pageIdent.FileID,
		PageID:  pageIdent.PageID,
		SlotNum: slot,
	}

	marshalledDirID, err := dirItemSystemFields.ID.MarshalBinary()
	if err != nil {
		err = fmt.Errorf("failed to marshal directory item ID: %w", err)
		return storage.NilDirItemID, err
	}

	err = dirSystemIndex.Insert(marshalledDirID, dirRID)
	if err != nil {
		err = fmt.Errorf("failed to insert directory item info into index: %w", err)
		return storage.NilDirItemID, err
	}

	return dirItemSystemFields.ID, nil
}

func (s *StorageEngine) updateDirItemEdgeID(
	txnID common.TxnID,
	dirItemID storage.DirItemSystemID,
	edgeID storage.EdgeSystemID,
	dirFileToken *txns.FileLockToken,
	dirSystemIndex storage.Index,
	ctxLogger common.ITxnLoggerWithContext,
) error {
	dirRID, err := GetDirectoryRID(txnID, dirItemID, dirSystemIndex)
	if err != nil {
		return fmt.Errorf("failed to get directory RID: %w", err)
	}

	pageIdent := dirRID.R.PageIdentity()
	pToken := s.locker.LockPage(dirFileToken, pageIdent.PageID, txns.PageLockExclusive)
	if pToken == nil {
		return fmt.Errorf("failed to lock page: %v", pageIdent)
	}

	pg, err := s.pool.GetPageNoCreate(pageIdent)
	if err != nil {
		return fmt.Errorf("failed to get page: %w", err)
	}
	defer s.pool.Unpin(pageIdent)

	return s.pool.WithMarkDirty(
		txnID,
		pageIdent,
		pg,
		func(lockedPage *page.SlottedPage) (common.LogRecordLocInfo, error) {
			dirRecordBytes := lockedPage.UnsafeRead(dirRID.R.SlotNum)
			dirItem, err := parseDirectoryRecord(dirRecordBytes)
			if err != nil {
				err = fmt.Errorf("failed to parse directory record: %w", err)
				return common.NewNilLogRecordLocation(), err
			}
			dirItem.EdgeID = edgeID
			dirRecordBytes, err = serializeDirectoryRecord(dirItem)
			if err != nil {
				err = fmt.Errorf("failed to serialize directory record: %w", err)
				return common.NewNilLogRecordLocation(), err
			}
			return lockedPage.UpdateWithLogs(dirRecordBytes, dirRID.R, ctxLogger)
		},
	)
}

func (s *StorageEngine) updateDirectoryItemNextID(
	txnID common.TxnID,
	dirItemID storage.DirItemSystemID,
	newNextItemID storage.DirItemSystemID,
	dirToken *txns.FileLockToken,
	dirSystemIndex storage.Index,
	ctxLogger common.ITxnLoggerWithContext,
) error {
	dirRID, err := GetDirectoryRID(txnID, dirItemID, dirSystemIndex)
	if err != nil {
		return fmt.Errorf("failed to get directory RID: %w", err)
	}

	pToken := s.locker.LockPage(dirToken, dirRID.R.PageIdentity().PageID, txns.PageLockExclusive)
	if pToken == nil {
		return fmt.Errorf("failed to lock page: %v", dirRID.R.PageIdentity())
	}

	pg, err := s.pool.GetPageNoCreate(dirRID.R.PageIdentity())
	if err != nil {
		return fmt.Errorf("failed to get page: %w", err)
	}
	defer s.pool.Unpin(dirRID.R.PageIdentity())

	return s.pool.WithMarkDirty(
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

func (s *StorageEngine) updateDirectoryItem(
	txnID common.TxnID,
	dirItemID storage.DirItemSystemID,
	dirItem storage.DirectoryItem,
	dirToken *txns.FileLockToken,
	dirSystemIndex storage.Index,
	ctxLogger common.ITxnLoggerWithContext,
) error {
	directoryRecordBytes, err := serializeDirectoryRecord(dirItem)
	if err != nil {
		return fmt.Errorf("failed to serialize directory record: %w", err)
	}

	dirRID, err := GetDirectoryRID(txnID, dirItemID, dirSystemIndex)
	if err != nil {
		return fmt.Errorf("failed to get directory RID: %w", err)
	}

	pageIdent := dirRID.R.PageIdentity()
	pToken := s.locker.LockPage(dirToken, pageIdent.PageID, txns.PageLockExclusive)
	if pToken == nil {
		return fmt.Errorf("failed to lock page: %v", pageIdent)
	}

	pg, err := s.pool.GetPageNoCreate(pageIdent)
	if err != nil {
		return fmt.Errorf("failed to get page: %w", err)
	}
	defer s.pool.Unpin(pageIdent)

	return s.pool.WithMarkDirty(
		txnID,
		pageIdent,
		pg,
		func(lockedPage *page.SlottedPage) (common.LogRecordLocInfo, error) {
			return lockedPage.UpdateWithLogs(directoryRecordBytes, dirRID.R, ctxLogger)
		},
	)
}

func (s *StorageEngine) UpdateEdge(
	txnID common.TxnID,
	edgeID storage.EdgeSystemID,
	edgeFields map[string]any,
	edgesFileToken *txns.FileLockToken,
	edgeSystemIndex storage.Index,
	schema storage.Schema,
	ctxLogger common.ITxnLoggerWithContext,
) error {
	edgeRID, err := GetEdgeRID(txnID, edgeID, edgeSystemIndex)
	if err != nil {
		return fmt.Errorf("failed to get edge RID: %w", err)
	}

	pageIdent := edgeRID.R.PageIdentity()
	pToken := s.locker.LockPage(edgesFileToken, pageIdent.PageID, txns.PageLockExclusive)
	if pToken == nil {
		return fmt.Errorf("failed to lock page: %v", pageIdent)
	}

	pg, err := s.pool.GetPageNoCreate(pageIdent)
	if err != nil {
		return fmt.Errorf("failed to get page: %w", err)
	}
	defer s.pool.Unpin(pageIdent)

	return s.pool.WithMarkDirty(
		txnID,
		pageIdent,
		pg,
		func(lockedPage *page.SlottedPage) (common.LogRecordLocInfo, error) {
			edgeRecordBytes := lockedPage.UnsafeRead(edgeRID.R.SlotNum)
			edgeSystemFields, _, err := parseEdgeRecordHeader(edgeRecordBytes)
			if err != nil {
				err = fmt.Errorf("failed to parse edge record header: %w", err)
				return common.NewNilLogRecordLocation(), err
			}

			newEdgeRecordBytes, err := serializeEdgeRecord(edgeSystemFields, edgeFields, schema)
			if err != nil {
				err = fmt.Errorf("failed to serialize edge record: %w", err)
				return common.NewNilLogRecordLocation(), err
			}

			return lockedPage.UpdateWithLogs(newEdgeRecordBytes, edgeRID.R, ctxLogger)
		},
	)
}

func (s *StorageEngine) updateEdgePrevID(
	txnID common.TxnID,
	edgeID storage.EdgeSystemID,
	prevEdgeID storage.EdgeSystemID,
	edgesFileToken *txns.FileLockToken,
	edgeSystemIndex storage.Index,
	ctxLogger common.ITxnLoggerWithContext,
) error {
	edgeRID, err := GetEdgeRID(txnID, edgeID, edgeSystemIndex)
	if err != nil {
		return fmt.Errorf("failed to get edge RID: %w", err)
	}

	pageIdent := edgeRID.R.PageIdentity()
	pToken := s.locker.LockPage(edgesFileToken, pageIdent.PageID, txns.PageLockExclusive)
	if pToken == nil {
		return fmt.Errorf("failed to lock page: %v", pageIdent)
	}

	pg, err := s.pool.GetPageNoCreate(pageIdent)
	if err != nil {
		return fmt.Errorf("failed to get page: %w", err)
	}
	defer s.pool.Unpin(pageIdent)

	return s.pool.WithMarkDirty(
		txnID,
		pageIdent,
		pg,
		func(lockedPage *page.SlottedPage) (common.LogRecordLocInfo, error) {
			edgeRecordBytes := lockedPage.UnsafeRead(edgeRID.R.SlotNum)
			edgeSystemFields, tail, err := parseEdgeRecordHeader(edgeRecordBytes)
			if err != nil {
				err = fmt.Errorf("failed to parse edge record header: %w", err)
				return common.NewNilLogRecordLocation(), err
			}
			edgeSystemFields.PrevEdgeID = prevEdgeID
			edgeRecordBytes, err = serializeEdgeRecordHeader(edgeSystemFields, tail)
			if err != nil {
				err = fmt.Errorf("failed to serialize edge record header: %w", err)
				return common.NewNilLogRecordLocation(), err
			}
			return lockedPage.UpdateWithLogs(edgeRecordBytes, edgeRID.R, ctxLogger)
		},
	)
}

func (s *StorageEngine) updateEdgeNextID(
	txnID common.TxnID,
	edgeID storage.EdgeSystemID,
	nextEdgeID storage.EdgeSystemID,
	edgesFileToken *txns.FileLockToken,
	edgeSystemIndex storage.Index,
	ctxLogger common.ITxnLoggerWithContext,
) error {
	edgeRID, err := GetEdgeRID(txnID, edgeID, edgeSystemIndex)
	if err != nil {
		return fmt.Errorf("failed to get edge RID: %w", err)
	}

	pageIdent := edgeRID.R.PageIdentity()
	pToken := s.locker.LockPage(edgesFileToken, pageIdent.PageID, txns.PageLockExclusive)
	if pToken == nil {
		return fmt.Errorf("failed to lock page: %v", pageIdent)
	}

	pg, err := s.pool.GetPageNoCreate(pageIdent)
	if err != nil {
		return fmt.Errorf("failed to get page: %w", err)
	}
	defer s.pool.Unpin(pageIdent)

	return s.pool.WithMarkDirty(
		txnID,
		pageIdent,
		pg,
		func(lockedPage *page.SlottedPage) (common.LogRecordLocInfo, error) {
			edgeRecordBytes := lockedPage.UnsafeRead(edgeRID.R.SlotNum)
			edgeSystemFields, tail, err := parseEdgeRecordHeader(edgeRecordBytes)
			if err != nil {
				err = fmt.Errorf("failed to parse edge record header: %w", err)
				return common.NewNilLogRecordLocation(), err
			}
			edgeSystemFields.NextEdgeID = nextEdgeID
			edgeRecordBytes, err = serializeEdgeRecordHeader(edgeSystemFields, tail)
			if err != nil {
				err = fmt.Errorf("failed to serialize edge record header: %w", err)
				return common.NewNilLogRecordLocation(), err
			}
			return lockedPage.UpdateWithLogs(edgeRecordBytes, edgeRID.R, ctxLogger)
		},
	)
}

func (s *StorageEngine) updateEdgeDirItemID(
	txnID common.TxnID,
	edgeID storage.EdgeSystemID,
	dirItemID storage.DirItemSystemID,
	edgesFileToken *txns.FileLockToken,
	edgeSystemIndex storage.Index,
	ctxLogger common.ITxnLoggerWithContext,
) error {
	edgeRID, err := GetEdgeRID(txnID, edgeID, edgeSystemIndex)
	if err != nil {
		return fmt.Errorf("failed to get edge RID: %w", err)
	}

	pageIdent := edgeRID.R.PageIdentity()
	pToken := s.locker.LockPage(edgesFileToken, pageIdent.PageID, txns.PageLockExclusive)
	if pToken == nil {
		return fmt.Errorf("failed to lock page: %v", pageIdent)
	}

	pg, err := s.pool.GetPageNoCreate(pageIdent)
	if err != nil {
		return fmt.Errorf("failed to get page: %w", err)
	}
	defer s.pool.Unpin(pageIdent)

	return s.pool.WithMarkDirty(
		txnID,
		pageIdent,
		pg,
		func(lockedPage *page.SlottedPage) (common.LogRecordLocInfo, error) {
			edgeRecordBytes := lockedPage.UnsafeRead(edgeRID.R.SlotNum)
			edgeSystemFields, tail, err := parseEdgeRecordHeader(edgeRecordBytes)
			if err != nil {
				err = fmt.Errorf("failed to parse edge record header: %w", err)
				return common.NewNilLogRecordLocation(), err
			}
			edgeSystemFields.DirectoryItemID = dirItemID
			edgeRecordBytes, err = serializeEdgeRecordHeader(edgeSystemFields, tail)
			if err != nil {
				err = fmt.Errorf("failed to serialize edge record header: %w", err)
				return common.NewNilLogRecordLocation(), err
			}
			return lockedPage.UpdateWithLogs(edgeRecordBytes, edgeRID.R, ctxLogger)
		},
	)
}

func (s *StorageEngine) DeleteEdge(
	txnID common.TxnID,
	edgeID storage.EdgeSystemID,
	edgesFileToken *txns.FileLockToken,
	edgeSystemIndex storage.Index,
	dirFileToken *txns.FileLockToken,
	dirSystemIndex storage.Index,
	ctxLogger common.ITxnLoggerWithContext,
) error {
	edgeRID, err := GetEdgeRID(txnID, edgeID, edgeSystemIndex)
	if err != nil {
		return fmt.Errorf("failed to get edge RID: %w", err)
	}

	pageIdent := edgeRID.R.PageIdentity()
	pToken := s.locker.LockPage(edgesFileToken, pageIdent.PageID, txns.PageLockExclusive)
	if pToken == nil {
		return fmt.Errorf("failed to lock page: %v", pageIdent)
	}

	pg, err := s.pool.GetPageNoCreate(pageIdent)
	if err != nil {
		return fmt.Errorf("failed to get page: %w", err)
	}
	defer s.pool.Unpin(pageIdent)

	curEdgeData := pg.LockedRead(edgeRID.R.SlotNum)
	curEdgeSystems, _, err := parseEdgeRecordHeader(curEdgeData)
	if err != nil {
		return fmt.Errorf("failed to parse edge record header: %w", err)
	}

	if curEdgeSystems.PrevEdgeID.IsNil() {
		err = s.updateDirItemEdgeID(
			txnID,
			curEdgeSystems.DirectoryItemID,
			curEdgeSystems.NextEdgeID,
			dirFileToken,
			dirSystemIndex,
			ctxLogger,
		)
		if err != nil {
			return fmt.Errorf("failed to update dir item edge ID: %w", err)
		}
	} else {
		err = s.updateEdgeNextID(
			txnID,
			curEdgeSystems.PrevEdgeID,
			curEdgeSystems.NextEdgeID,
			edgesFileToken,
			edgeSystemIndex,
			ctxLogger,
		)
		if err != nil {
			return fmt.Errorf("failed to update edge next ID: %w", err)
		}
	}

	err = s.updateEdgePrevID(
		txnID,
		curEdgeSystems.NextEdgeID,
		curEdgeSystems.PrevEdgeID,
		edgesFileToken,
		edgeSystemIndex,
		ctxLogger,
	)
	if err != nil {
		return fmt.Errorf("failed to update edge prev ID: %w", err)
	}
	return nil
}
