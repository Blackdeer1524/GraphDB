package common

type dummyLogger struct{}

func (d *dummyLogger) GetLogfileID() FileID {
	return NilFileID
}

func (d *dummyLogger) AppendCheckpointBegin() (LogRecordLocInfo, error) {
	return NewNilLogRecordLocation(), nil
}

func (d *dummyLogger) AppendCheckpointEnd(
	checkpointBeginLocation LogRecordLocInfo,
	att map[TxnID]LogRecordLocInfo,
	dpt map[PageIdentity]LogRecordLocInfo,
) error {
	return nil
}

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

type DummyLoggerWithContext struct {
	txnID TxnID
}

var _ ITxnLoggerWithContext = &DummyLoggerWithContext{}

func NoLogs(txnID TxnID) *DummyLoggerWithContext {
	return &DummyLoggerWithContext{txnID: txnID}
}

func (l *DummyLoggerWithContext) GetTxnID() TxnID {
	return l.txnID
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
