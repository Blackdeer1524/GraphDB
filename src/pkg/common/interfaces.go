package common

type ITxnLogger interface {
	WithContext(txnID TxnID) ITxnLoggerWithContext
	GetMasterRecord() LSN
	Flush() error
}

type ITxnLoggerWithContext interface {
	AppendBegin() error
	Lock()
	Unlock()
	AssumeLockedAppendInsert(
		recordID RecordID,
		value []byte,
	) (LogRecordLocInfo, error)
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
	ReadPage(page T, pageIdent PageIdentity) error
	GetPageNoNew(page T, pageIdent PageIdentity) error
	WritePage(page T, pageIdent PageIdentity) error
}
