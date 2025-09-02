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

func (s *StorageEngine) GetVertexRID(
	txnID common.TxnID,
	vertexID storage.VertexID,
	vertexIndex storage.Index,
) (storage.VertexIDWithRID, error) {
	b, err := vertexID.MarshalBinary()
	if err != nil {
		return storage.VertexIDWithRID{}, fmt.Errorf("failed to marshal vertex ID: %w", err)
	}

	rid, err := vertexIndex.Get(b)
	if err != nil {
		return storage.VertexIDWithRID{}, fmt.Errorf("failed to get vertex RID: %w", err)
	}

	res := storage.VertexIDWithRID{
		V: vertexID,
		R: rid,
	}
	return res, nil
}

func (s *StorageEngine) GetEdgeRID(
	txnID common.TxnID,
	edgeID storage.EdgeID,
	edgeIndex storage.Index,
) (storage.EdgeIDWithRID, error) {
	b, err := edgeID.MarshalBinary()
	if err != nil {
		return storage.EdgeIDWithRID{}, fmt.Errorf("failed to marshal edge ID: %w", err)
	}

	rid, err := edgeIndex.Get(b)
	if err != nil {
		return storage.EdgeIDWithRID{}, fmt.Errorf("failed to get edge RID: %w", err)
	}

	res := storage.EdgeIDWithRID{
		E: edgeID,
		R: rid,
	}

	return res, nil
}

func (s *StorageEngine) GetDirectoryRID(
	txnID common.TxnID,
	dirItemID storage.DirItemID,
	dirSystemIndex storage.Index,
) (storage.DirectoryIDWithRID, error) {
	b, err := dirItemID.MarshalBinary()
	if err != nil {
		return storage.DirectoryIDWithRID{}, fmt.Errorf("failed to marshal directory ID: %w", err)
	}

	rid, err := dirSystemIndex.Get(b)
	if err != nil {
		return storage.DirectoryIDWithRID{}, fmt.Errorf("failed to get directory RID: %w", err)
	}

	res := storage.DirectoryIDWithRID{
		D: dirItemID,
		R: rid,
	}

	return res, nil
}

func (s *StorageEngine) getSerializedVertex(
	txnID common.TxnID,
	vertexID storage.VertexID,
	vertexIndex storage.Index,
) ([]byte, error) {
	vertexRID, err := s.GetVertexRID(txnID, vertexID, vertexIndex)
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
	vertexID storage.VertexID,
	vertexIndex storage.Index,
	schema storage.Schema,
) (storage.VertexInternalFields, map[string]any, error) {
	data, err := s.getSerializedVertex(txnID, vertexID, vertexIndex)
	if err != nil {
		err = fmt.Errorf("failed to get serialized vertex: %w", err)
		return storage.VertexInternalFields{}, nil, err
	}

	vertexInternalFields, record, err := parseVertexRecord(data, schema)
	if err != nil {
		err = fmt.Errorf("failed to parse vertex record: %w", err)
		return storage.VertexInternalFields{}, nil, err
	}

	return vertexInternalFields, record, nil
}

func (s *StorageEngine) InsertVertex(
	txnID common.TxnID,

	data map[string]any,
	schema storage.Schema,

	vertexFileToken *txns.FileLockToken,
	vertexIndex storage.Index,

	ctxLogger common.ITxnLoggerWithContext,
) error {
	pageID, err := s.getPageWithFreeSpace(vertexFileToken.GetFileID())
	if err != nil {
		return fmt.Errorf("failed to get free page: %w", err)
	}
	if s.locker.LockPage(vertexFileToken, pageID, txns.PageLockExclusive) == nil {
		return fmt.Errorf("failed to lock page: %w", err)
	}

	pageIdent := common.PageIdentity{
		FileID: vertexFileToken.GetFileID(),
		PageID: pageID,
	}
	pg, err := s.pool.GetPage(pageIdent)
	if err != nil {
		return fmt.Errorf("failed to get page: %w", err)
	}
	defer s.pool.Unpin(pageIdent)

	internalFields := storage.NewVertexInternalFields(
		storage.VertexID(uuid.New()),
		storage.NilDirItemID,
	)
	serializedData, err := serializeVertexRecord(
		internalFields,
		data,
		schema,
	)
	if err != nil {
		return fmt.Errorf("failed to serialize vertex record: %w", err)
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
		return fmt.Errorf("failed to insert vertex record: %w", err)
	}

	vertexRID := common.RecordID{
		FileID:  pageIdent.FileID,
		PageID:  pageIdent.PageID,
		SlotNum: slot,
	}

	marshalledID, err := internalFields.ID.MarshalBinary()
	if err != nil {
		return fmt.Errorf("failed to marshal vertex ID: %w", err)
	}
	err = vertexIndex.Insert(marshalledID, vertexRID)
	if err != nil {
		return fmt.Errorf("failed to insert vertex record: %w", err)
	}
	return err
}

func (s *StorageEngine) DeleteVertex(
	txnID common.TxnID,
	vertexID storage.VertexID,
	vertexFileToken *txns.FileLockToken,
	vertexIndex storage.Index,
	ctxLogger common.ITxnLoggerWithContext,
) error {
	vertexRID, err := s.GetVertexRID(txnID, vertexID, vertexIndex)
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
	vertexID storage.VertexID,
	newData map[string]any,
	schema storage.Schema,
	vertexFileToken *txns.FileLockToken,
	vertexIndex storage.Index,
	ctxLogger common.ITxnLoggerWithContext,
) error {
	vertexRID, err := s.GetVertexRID(txnID, vertexID, vertexIndex)
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

func (s *StorageEngine) updateVertexDirItemID(
	txnID common.TxnID,

	srcVertexID storage.VertexID,
	dirItemID storage.DirItemID,

	srcVertToken *txns.FileLockToken,
	vertexIndex storage.Index,
	ctxLogger common.ITxnLoggerWithContext,
) error {
	vertexRID, err := s.GetVertexRID(txnID, srcVertexID, vertexIndex)
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

func (s *StorageEngine) SelectEdge(
	txnID common.TxnID,
	edgeID storage.EdgeID,
	edgeFileToken *txns.FileLockToken,
	edgeSystemIndex storage.Index,
	schema storage.Schema,
) (storage.EdgeInternalFields, map[string]any, error) {
	edgeRID, err := s.GetEdgeRID(txnID, edgeID, edgeSystemIndex)
	if err != nil {
		return storage.EdgeInternalFields{}, nil, fmt.Errorf("failed to get edge RID: %w", err)
	}

	pToken := s.locker.LockPage(
		edgeFileToken,
		edgeRID.R.PageIdentity().PageID,
		txns.PageLockShared,
	)
	if pToken == nil {
		err = fmt.Errorf("failed to lock page: %v", edgeRID.R.PageIdentity())
		return storage.EdgeInternalFields{}, nil, err
	}

	pageIdent := edgeRID.R.PageIdentity()
	pg, err := s.pool.GetPageNoCreate(pageIdent)
	if err != nil {
		return storage.EdgeInternalFields{}, nil, fmt.Errorf("failed to get page: %w", err)
	}
	defer s.pool.Unpin(pageIdent)

	data := pg.LockedRead(edgeRID.R.SlotNum)
	return parseEdgeRecord(data, schema)
}

func (s *StorageEngine) insertEdgeHelper(
	txnID common.TxnID,
	srcVertexID storage.VertexID,
	dstVertexID storage.VertexID,
	dirItemID storage.DirItemID,
	nextEdgeID storage.EdgeID,
	edgeFields map[string]any,
	schema storage.Schema,
	edgesFileToken *txns.FileLockToken,
	edgeSystemIndex storage.Index,
	ctxLogger common.ITxnLoggerWithContext,
) (storage.EdgeID, error) {
	edgePageID, err := s.getPageWithFreeSpace(edgesFileToken.GetFileID())
	if err != nil {
		return storage.NilEdgeID, fmt.Errorf("failed to get free page: %w", err)
	}
	if s.locker.LockPage(edgesFileToken, edgePageID, txns.PageLockExclusive) == nil {
		return storage.NilEdgeID, fmt.Errorf("failed to lock page: %w", err)
	}

	edgeInternalFields := storage.NewEdgeInternalFields(
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
	edgePg, err := s.pool.GetPage(edgePageIdent)
	if err != nil {
		return storage.NilEdgeID, fmt.Errorf("failed to get page: %w", err)
	}
	defer s.pool.Unpin(edgePageIdent)

	edgeRecordBytes, err := serializeEdgeRecord(edgeInternalFields, edgeFields, schema)
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

	marshalledEdgeID, err := edgeInternalFields.ID.MarshalBinary()
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

	return edgeInternalFields.ID, nil
}

func (s *StorageEngine) insertEdgeWithDirItem(
	txnID common.TxnID,

	srcVertexID storage.VertexID,
	dstVertexID storage.VertexID,
	edgeFields map[string]any,
	schema storage.Schema,
	edgesFileToken *txns.FileLockToken,
	edgeSystemIndex storage.Index,

	prevDirItemID storage.DirItemID,
	nextDirItemID storage.DirItemID,
	srcVertDirToken *txns.FileLockToken,
	srcVertDirSystemIndex storage.Index,

	ctxLogger common.ITxnLoggerWithContext,
) (storage.EdgeID, storage.DirItemID, error) {
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

	srcVertexID storage.VertexID,
	dstVertexID storage.VertexID,
	edgeFields map[string]any,
	schema storage.Schema,

	srcVertToken *txns.FileLockToken,
	srcVertSystemIndex storage.Index,

	srcVertDirToken *txns.FileLockToken,
	srcVertDirSystemIndex storage.Index,

	edgesFileToken *txns.FileLockToken,
	edgeSystemIndex storage.Index,

	ctxLogger common.ITxnLoggerWithContext,
) error {
	srcVertInternalFields, _, err := s.SelectVertex(txnID, srcVertexID, srcVertSystemIndex, schema)
	if err != nil {
		return fmt.Errorf("failed to get serialized src vertex: %w", err)
	}

	if srcVertInternalFields.DirItemID.IsNil() {
		_, newDirItemID, err := s.insertEdgeWithDirItem(
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
			return fmt.Errorf("failed to insert edge with directory item: %w", err)
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
			return fmt.Errorf("failed to update vertex directory item ID: %w", err)
		}
		return nil
	}

	dirItem, err := s.selectDirectoryItem(
		txnID,
		srcVertInternalFields.DirItemID,
		srcVertDirToken,
		srcVertDirSystemIndex,
	)
	if err != nil {
		return fmt.Errorf("failed to select directory item: %w", err)
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
				return fmt.Errorf("failed to select directory item: %w", err)
			}
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
			return fmt.Errorf("failed to insert edge: %w", err)
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
				return fmt.Errorf("failed to update prev edge ID: %w", err)
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
			return fmt.Errorf("failed to update directory item: %w", err)
		}
		return nil
	}

	_, newDirItemID, err := s.insertEdgeWithDirItem(
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
		return fmt.Errorf("failed to insert edge with directory item: %w", err)
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
		return fmt.Errorf("failed to update directory item next ID: %w", err)
	}

	return nil
}

func (s *StorageEngine) selectDirectoryItem(
	txnID common.TxnID,
	dirItemID storage.DirItemID,
	dirToken *txns.FileLockToken,
	dirSystemIndex storage.Index,
) (storage.DirectoryItem, error) {
	dirRID, err := s.GetDirectoryRID(txnID, dirItemID, dirSystemIndex)
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
	prevDirItemID storage.DirItemID,
	nextDirItemID storage.DirItemID,

	dirFileToken *txns.FileLockToken,
	dirSystemIndex storage.Index,

	ctxLogger common.ITxnLoggerWithContext,
) (storage.DirItemID, error) {
	dirItemInternalFields := storage.NewDirectoryItemInternalFields(
		storage.DirItemID(uuid.New()),
		nextDirItemID,
		prevDirItemID,
	)
	dirItem := storage.DirectoryItem{
		DirectoryItemInternalFields: dirItemInternalFields,
		DirectoryItemGraphFields:    dirItemGraphFields,
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

	marshalledDirID, err := dirItemInternalFields.ID.MarshalBinary()
	if err != nil {
		err = fmt.Errorf("failed to marshal directory item ID: %w", err)
		return storage.NilDirItemID, err
	}

	err = dirSystemIndex.Insert(marshalledDirID, dirRID)
	if err != nil {
		err = fmt.Errorf("failed to insert directory item info into index: %w", err)
		return storage.NilDirItemID, err
	}

	return dirItemInternalFields.ID, nil
}

func (s *StorageEngine) updateDirItemEdgeID(
	txnID common.TxnID,
	dirItemID storage.DirItemID,
	edgeID storage.EdgeID,
	dirFileToken *txns.FileLockToken,
	dirSystemIndex storage.Index,
	ctxLogger common.ITxnLoggerWithContext,
) error {
	dirRID, err := s.GetDirectoryRID(txnID, dirItemID, dirSystemIndex)
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
	dirItemID storage.DirItemID,
	newNextItemID storage.DirItemID,
	dirToken *txns.FileLockToken,
	dirSystemIndex storage.Index,
	ctxLogger common.ITxnLoggerWithContext,
) error {
	dirRID, err := s.GetDirectoryRID(txnID, dirItemID, dirSystemIndex)
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
	dirItemID storage.DirItemID,
	dirItem storage.DirectoryItem,
	dirToken *txns.FileLockToken,
	dirSystemIndex storage.Index,
	ctxLogger common.ITxnLoggerWithContext,
) error {
	directoryRecordBytes, err := serializeDirectoryRecord(dirItem)
	if err != nil {
		return fmt.Errorf("failed to serialize directory record: %w", err)
	}

	dirRID, err := s.GetDirectoryRID(txnID, dirItemID, dirSystemIndex)
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
	edgeID storage.EdgeID,
	edgeFields map[string]any,
	edgesFileToken *txns.FileLockToken,
	edgeSystemIndex storage.Index,
	schema storage.Schema,
	ctxLogger common.ITxnLoggerWithContext,
) error {
	edgeRID, err := s.GetEdgeRID(txnID, edgeID, edgeSystemIndex)
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

func (s *StorageEngine) updateEdgePrevID(
	txnID common.TxnID,
	edgeID storage.EdgeID,
	prevEdgeID storage.EdgeID,
	edgesFileToken *txns.FileLockToken,
	edgeSystemIndex storage.Index,
	ctxLogger common.ITxnLoggerWithContext,
) error {
	edgeRID, err := s.GetEdgeRID(txnID, edgeID, edgeSystemIndex)
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

func (s *StorageEngine) updateEdgeNextID(
	txnID common.TxnID,
	edgeID storage.EdgeID,
	nextEdgeID storage.EdgeID,
	edgesFileToken *txns.FileLockToken,
	edgeSystemIndex storage.Index,
	ctxLogger common.ITxnLoggerWithContext,
) error {
	edgeRID, err := s.GetEdgeRID(txnID, edgeID, edgeSystemIndex)
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

func (s *StorageEngine) updateEdgeDirItemID(
	txnID common.TxnID,
	edgeID storage.EdgeID,
	dirItemID storage.DirItemID,
	edgesFileToken *txns.FileLockToken,
	edgeSystemIndex storage.Index,
	ctxLogger common.ITxnLoggerWithContext,
) error {
	edgeRID, err := s.GetEdgeRID(txnID, edgeID, edgeSystemIndex)
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

func (s *StorageEngine) DeleteEdge(
	txnID common.TxnID,
	edgeID storage.EdgeID,
	edgesFileToken *txns.FileLockToken,
	edgeSystemIndex storage.Index,
	dirFileToken *txns.FileLockToken,
	dirSystemIndex storage.Index,
	ctxLogger common.ITxnLoggerWithContext,
) error {
	edgeRID, err := s.GetEdgeRID(txnID, edgeID, edgeSystemIndex)
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
	curEdgeInternals, _, err := parseEdgeRecordHeader(curEdgeData)
	if err != nil {
		return fmt.Errorf("failed to parse edge record header: %w", err)
	}

	if curEdgeInternals.PrevEdgeID.IsNil() {
		err = s.updateDirItemEdgeID(
			txnID,
			curEdgeInternals.DirectoryItemID,
			curEdgeInternals.NextEdgeID,
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
			curEdgeInternals.PrevEdgeID,
			curEdgeInternals.NextEdgeID,
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
		curEdgeInternals.NextEdgeID,
		curEdgeInternals.PrevEdgeID,
		edgesFileToken,
		edgeSystemIndex,
		ctxLogger,
	)
	if err != nil {
		return fmt.Errorf("failed to update edge prev ID: %w", err)
	}
	return nil
}
