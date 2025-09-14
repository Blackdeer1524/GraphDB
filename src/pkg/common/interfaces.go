package common

type ITxnLogger interface {
	WithContext(txnID TxnID) ITxnLoggerWithContext
	GetLogfileID() FileID
	GetFlushLSN() LSN
	GetFlushInfo() (FileID, PageID, PageID, LSN)
	UpdateFirstUnflushedPage(pageID PageID)
	UpdateFlushLSN(lsn LSN)
	AppendCheckpointBegin() (LogRecordLocInfo, error)
	AppendCheckpointEnd(
		checkpointBeginLocation LogRecordLocInfo,
		att map[TxnID]LogRecordLocInfo,
		dpt map[PageIdentity]LogRecordLocInfo,
	) error
}

type ITxnLoggerWithContext interface {
	GetTxnID() TxnID // WARN: DON'T USE THIS METHOD IN LockManager!
	AppendBegin() error
	AppendInsert(recordID RecordID, value []byte) (LogRecordLocInfo, error)
	AppendUpdate(recordID RecordID, before []byte, after []byte) (LogRecordLocInfo, error)
	AppendDelete(recordID RecordID) (LogRecordLocInfo, error)
	AppendCommit() error
	AppendAbort() error
	AppendTxnEnd() error
	Rollback()
}

type Page interface {
	GetData() []byte
	SetData(d []byte)

	// latch methods
	Lock()
	Unlock()
	RLock()
	RUnlock()
}

type DiskManager[T Page] interface {
	Lock()
	Unlock()
	ReadPage(page T, pageIdent PageIdentity) error
	ReadPageAssumeLocked(page T, pageIdent PageIdentity) error
	GetPageNoNew(page T, pageIdent PageIdentity) error
	GetPageNoNewAssumeLocked(page T, pageIdent PageIdentity) error
	WritePageAssumeLocked(page T, pageIdent PageIdentity) error
	BulkWritePageAssumeLockedBegin(
		fileID FileID,
	) (func(page T, pageID PageID) error, func() error, error)
}
