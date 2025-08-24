package common

type DummyLoggerWithContext struct{}

var dummyLogger DummyLoggerWithContext = DummyLoggerWithContext{}

var _ ITxnLoggerWithContext = &DummyLoggerWithContext{}

func NoLogs() *DummyLoggerWithContext {
	return &dummyLogger
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
