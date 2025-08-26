package common

type dummyLogger struct{}

var _ ITxnLogger = &dummyLogger{}

func DummyLogger() *dummyLogger {
	return &dummyLogger{}
}

func (d *dummyLogger) GetFlushInfo() (FileID, PageID, PageID, LSN) {
	return 0, 0, 0, 0
}

func (d *dummyLogger) GetFlushLSN() LSN {
	return 0
}

func (d *dummyLogger) UpdateFirstUnflushedPage(pageID PageID) {
}

func (d *dummyLogger) UpdateFlushLSN(lsn LSN) {
}

func (d *dummyLogger) WithContext(txnID TxnID) ITxnLoggerWithContext {
	return &DummyLoggerWithContext{}
}

type DummyLoggerWithContext struct{}

var _ ITxnLoggerWithContext = &DummyLoggerWithContext{}

func NoLogs() *DummyLoggerWithContext {
	return &DummyLoggerWithContext{}
}

func (l *DummyLoggerWithContext) GetTxnID() TxnID {
	return NilTxnID
}

func (l *DummyLoggerWithContext) AppendBegin() error {
	return nil
}

func (l *DummyLoggerWithContext) AppendDelete(
	recordID RecordID,
) (LogRecordLocInfo, error) {
	return NewNilLogRecordLocation(), nil
}

func (l *DummyLoggerWithContext) AppendInsert(
	recordID RecordID,
	value []byte,
) (LogRecordLocInfo, error) {
	return NewNilLogRecordLocation(), nil
}

func (l *DummyLoggerWithContext) AppendUpdate(
	recordID RecordID,
	before []byte,
	after []byte,
) (LogRecordLocInfo, error) {
	return NewNilLogRecordLocation(), nil
}

func (l *DummyLoggerWithContext) AppendCommit() error {
	return nil
}

func (l *DummyLoggerWithContext) AppendAbort() error {
	return nil
}

func (l *DummyLoggerWithContext) AppendTxnEnd() error {
	return nil
}

func (l *DummyLoggerWithContext) Rollback() {
}

func (l *DummyLoggerWithContext) Lock() {
}

func (l *DummyLoggerWithContext) Unlock() {
}
