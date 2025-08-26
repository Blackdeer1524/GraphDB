package common

type ITxnLogger interface {
	WithContext(txnID TxnID) ITxnLoggerWithContext
	GetFlushLSN() LSN
	GetFlushInfo() (FileID, PageID, PageID, LSN)
	UpdateFirstUnflushedPage(pageID PageID)
	UpdateFlushLSN(lsn LSN)
}

type ITxnLoggerWithContext interface {
	AppendBegin() error
	AssumeLockedAppendInsert(recordID RecordID, value []byte) (LogRecordLocInfo, error)
	AssumeLockedAppendUpdate(
		recordID RecordID,
		before []byte,
		after []byte,
	) (LogRecordLocInfo, error)
	AssumeLockedAppendDelete(recordID RecordID) (LogRecordLocInfo, error)
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
}
