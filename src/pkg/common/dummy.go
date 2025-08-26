package common

type dummyLogger struct{}

var _ ITxnLogger = &dummyLogger{}

func DummyLogger() *dummyLogger {
	return &dummyLogger{}
}

// GetFlushInfo implements ITxnLogger.
func (d *dummyLogger) GetFlushInfo() (FileID, PageID, PageID, LSN) {
	return 0, 0, 0, 0
}

// GetFlushLSN implements ITxnLogger.
func (d *dummyLogger) GetFlushLSN() LSN {
	return 0
}

// UpdateFirstUnflushedPage implements ITxnLogger.
func (d *dummyLogger) UpdateFirstUnflushedPage(pageID PageID) {
}

// UpdateFlushLSN implements ITxnLogger.
func (d *dummyLogger) UpdateFlushLSN(lsn LSN) {
}

// WithContext implements ITxnLogger.
func (d *dummyLogger) WithContext(txnID TxnID) ITxnLoggerWithContext {
	return &DummyLoggerWithContext{}
}

type DummyLoggerWithContext struct{}

var _ ITxnLoggerWithContext = &DummyLoggerWithContext{}

func NoLogs() *DummyLoggerWithContext {
	return &DummyLoggerWithContext{}
}

func (l *DummyLoggerWithContext) AppendBegin() error {
	return nil
}

func (l *DummyLoggerWithContext) AssumeLockedAppendDelete(
	recordID RecordID,
) (LogRecordLocInfo, error) {
	return NewNilLogRecordLocation(), nil
}

func (l *DummyLoggerWithContext) AssumeLockedAppendInsert(
	recordID RecordID,
	value []byte,
) (LogRecordLocInfo, error) {
	return NewNilLogRecordLocation(), nil
}

func (l *DummyLoggerWithContext) AssumeLockedAppendUpdate(
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
