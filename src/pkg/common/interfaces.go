package common

type ITxnLogger interface {
	WithContext(txnID TxnID) ITxnLoggerWithContext
	GetMasterRecord() LSN
	Flush() error
	AppendAbort(txnID TxnID, prevLog LogRecordLocInfo) (LogRecordLocInfo, error)
	AppendBegin(TransactionID TxnID) (LogRecordLocInfo, error)
	AppendCommit(
		txnID TxnID,
		prevLog LogRecordLocInfo,
	) (LogRecordLocInfo, error)
	AppendDelete(txnID TxnID, prevLog LogRecordLocInfo, recordID RecordID,
	) (LogRecordLocInfo, error)
	AppendInsert(
		txnID TxnID,
		prevLog LogRecordLocInfo,
		recordID RecordID,
		value []byte,
	) (LogRecordLocInfo, error)
	AppendTxnEnd(
		txnID TxnID,
		prevLog LogRecordLocInfo,
	) (LogRecordLocInfo, error)
	AppendUpdate(
		txnID TxnID,
		prevLog LogRecordLocInfo,
		recordID RecordID,
		beforeValue []byte,
		afterValue []byte,
	) (LogRecordLocInfo, error)
	Rollback(abortLogRecord LogRecordLocInfo)
}

type ITxnLoggerWithContext interface {
	AppendBegin() error
	AppendDelete(recordID RecordID) (LogRecordLocInfo, error)
	AppendInsert(recordID RecordID, value []byte) (LogRecordLocInfo, error)
	AppendUpdate(
		recordID RecordID,
		before []byte,
		after []byte,
	) (LogRecordLocInfo, error)
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
