package recovery

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/Blackdeer1524/GraphDB/src/bufferpool"
	"github.com/Blackdeer1524/GraphDB/src/pkg/assert"
	"github.com/Blackdeer1524/GraphDB/src/txns"
)

type LogRecordTypeTag byte

// Type tags for each log record type.
const (
	TypeBegin LogRecordTypeTag = iota + 1
	TypeUpdate
	TypeInsert
	TypeCommit
	TypeAbort
	TypeTxnEnd
	TypeCompensation
	TypeCheckpointBegin
	TypeCheckpointEnd
	TypeUnknown
)

func (l *LogRecordLocationInfo) MarshalBinary() ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.BigEndian, l.Lsn); err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.BigEndian, l.Location.PageID); err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.BigEndian, l.Location.SlotNum); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (l *LogRecordLocationInfo) UnmarshalBinary(data []byte) error {
	rd := bytes.NewReader(data)
	if err := binary.Read(rd, binary.BigEndian, &l.Lsn); err != nil {
		return err
	}

	if err := binary.Read(rd, binary.BigEndian, &l.Location.PageID); err != nil {
		return err
	}

	if err := binary.Read(rd, binary.BigEndian, &l.Location.SlotNum); err != nil {
		return err
	}

	return nil
}

// MarshalBinary for BeginLogRecord.
func (b *BeginLogRecord) MarshalBinary() ([]byte, error) {
	buf := new(bytes.Buffer)
	buf.WriteByte(byte(TypeBegin))

	if err := binary.Write(buf, binary.BigEndian, b.lsn); err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.BigEndian, b.TransactionID); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (b *BeginLogRecord) UnmarshalBinary(data []byte) error {
	if len(data) < 1 {
		return errors.New("insufficient data for type tag")
	}

	if data[0] != byte(TypeBegin) {
		return fmt.Errorf("invalid type tag for BeginLogRecord: %x", data[0])
	}

	reader := bytes.NewReader(data[1:])
	if err := binary.Read(reader, binary.BigEndian, &b.lsn); err != nil {
		return err
	}

	return binary.Read(reader, binary.BigEndian, &b.TransactionID)
}

// MarshalBinary for UpdateLogRecord.
func (u *UpdateLogRecord) MarshalBinary() ([]byte, error) {
	buf := new(bytes.Buffer)
	buf.WriteByte(byte(TypeUpdate))

	if err := binary.Write(buf, binary.BigEndian, u.lsn); err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.BigEndian, u.TransactionID); err != nil {
		return nil, err
	}

	locationBytes, err := u.parentLogLocation.MarshalBinary()
	if err != nil {
		return nil, err
	}

	buf.Write(locationBytes)

	pageData, err := u.modifiedPageIdentity.MarshalBinary()
	if err != nil {
		return nil, err
	}

	buf.Write(pageData)

	if err := binary.Write(buf, binary.BigEndian, u.modifiedSlotNumber); err != nil {
		return nil, err
	}

	//nolint:gosec
	if err := binary.Write(buf, binary.BigEndian, uint32(len(u.beforeValue))); err != nil {
		return nil, err
	}

	buf.Write(u.beforeValue)

	//nolint:gosec
	if err := binary.Write(buf, binary.BigEndian, uint32(len(u.afterValue))); err != nil {
		return nil, err
	}

	buf.Write(u.afterValue)

	return buf.Bytes(), nil
}

func (u *UpdateLogRecord) UnmarshalBinary(data []byte) error {
	if len(data) < 1 {
		return errors.New("insufficient data")
	}

	if data[0] != byte(TypeUpdate) {
		return fmt.Errorf("invalid type tag for UpdateLogRecord: %x", data[0])
	}

	reader := bytes.NewReader(data[1:])

	if err := binary.Read(reader, binary.BigEndian, &u.lsn); err != nil {
		return err
	}

	if err := binary.Read(reader, binary.BigEndian, &u.TransactionID); err != nil {
		return err
	}

	if err := binary.Read(reader, binary.BigEndian, &u.parentLogLocation); err != nil {
		return err
	}

	if err := binary.Read(reader, binary.BigEndian, &u.modifiedPageIdentity); err != nil {
		return err
	}

	var slotNum uint16
	if err := binary.Read(reader, binary.BigEndian, &slotNum); err != nil {
		return err
	}

	u.modifiedSlotNumber = slotNum

	var beforeLen uint32
	if err := binary.Read(reader, binary.BigEndian, &beforeLen); err != nil {
		return err
	}

	u.beforeValue = make([]byte, beforeLen)
	if _, err := io.ReadFull(reader, u.beforeValue); err != nil {
		return err
	}

	var afterLen uint32
	if err := binary.Read(reader, binary.BigEndian, &afterLen); err != nil {
		return err
	}

	u.afterValue = make([]byte, afterLen)
	_, err := io.ReadFull(reader, u.afterValue)

	return err
}

// MarshalBinary for InsertLogRecord.
func (i *InsertLogRecord) MarshalBinary() ([]byte, error) {
	buf := new(bytes.Buffer)
	buf.WriteByte(byte(TypeInsert))

	if err := binary.Write(buf, binary.BigEndian, i.lsn); err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.BigEndian, i.TransactionID); err != nil {
		return nil, err
	}

	d, err := i.parentLogLocation.MarshalBinary()
	if err != nil {
		return nil, err
	}

	buf.Write(d)

	pageData, err := i.modifiedPageIdentity.MarshalBinary()
	if err != nil {
		return nil, err
	}

	buf.Write(pageData)

	if err := binary.Write(buf, binary.BigEndian, i.modifiedSlotNumber); err != nil {
		return nil, err
	}

	//nolint:gosec
	if err := binary.Write(buf, binary.BigEndian, uint32(len(i.value))); err != nil {
		return nil, err
	}

	buf.Write(i.value)

	return buf.Bytes(), nil
}

func (i *InsertLogRecord) UnmarshalBinary(data []byte) error {
	if len(data) < 1 || data[0] != byte(TypeInsert) {
		return errors.New("invalid type tag for InsertLogRecord")
	}

	reader := bytes.NewReader(data[1:])

	if err := binary.Read(reader, binary.BigEndian, &i.lsn); err != nil {
		return err
	}

	if err := binary.Read(reader, binary.BigEndian, &i.TransactionID); err != nil {
		return err
	}

	if err := binary.Read(reader, binary.BigEndian, &i.parentLogLocation); err != nil {
		return err
	}

	if err := binary.Read(reader, binary.BigEndian, &i.modifiedPageIdentity); err != nil {
		return err
	}

	if err := binary.Read(reader, binary.BigEndian, &i.modifiedSlotNumber); err != nil {
		return err
	}

	var valueLen uint32
	if err := binary.Read(reader, binary.BigEndian, &valueLen); err != nil {
		return err
	}

	i.value = make([]byte, valueLen)
	_, err := io.ReadFull(reader, i.value)

	return err
}

// MarshalBinary for CommitLogRecord.
func (c *CommitLogRecord) MarshalBinary() ([]byte, error) {
	buf := new(bytes.Buffer)
	buf.WriteByte(byte(TypeCommit))

	if err := binary.Write(buf, binary.BigEndian, c.lsn); err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.BigEndian, c.TransactionID); err != nil {
		return nil, err
	}

	d, err := c.parentLogLocation.MarshalBinary()
	if err != nil {
		return nil, err
	}

	buf.Write(d)

	return buf.Bytes(), nil
}

func (c *CommitLogRecord) UnmarshalBinary(data []byte) error {
	if len(data) < 1 || data[0] != byte(TypeCommit) {
		return errors.New("invalid type tag for CommitLogRecord")
	}

	reader := bytes.NewReader(data[1:])

	if err := binary.Read(reader, binary.BigEndian, &c.lsn); err != nil {
		return err
	}

	if err := binary.Read(reader, binary.BigEndian, &c.TransactionID); err != nil {
		return err
	}

	if err := binary.Read(reader, binary.BigEndian, &c.parentLogLocation); err != nil {
		return err
	}

	return nil
}

// MarshalBinary for AbortLogRecord.
func (a *AbortLogRecord) MarshalBinary() ([]byte, error) {
	buf := new(bytes.Buffer)
	buf.WriteByte(byte(TypeAbort))

	if err := binary.Write(buf, binary.BigEndian, a.lsn); err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.BigEndian, a.TransactionID); err != nil {
		return nil, err
	}

	d, err := a.parentLogLocation.MarshalBinary()
	if err != nil {
		return nil, err
	}

	buf.Write(d)

	return buf.Bytes(), nil
}

func (a *AbortLogRecord) UnmarshalBinary(data []byte) error {
	if len(data) < 1 || data[0] != byte(TypeAbort) {
		return errors.New("invalid type tag for AbortLogRecord")
	}

	reader := bytes.NewReader(data[1:])

	if err := binary.Read(reader, binary.BigEndian, &a.lsn); err != nil {
		return err
	}

	if err := binary.Read(reader, binary.BigEndian, &a.TransactionID); err != nil {
		return err
	}

	if err := binary.Read(reader, binary.BigEndian, &a.parentLogLocation); err != nil {
		return err
	}

	return nil
}

// MarshalBinary for TxnEndLogRecord.
func (t *TxnEndLogRecord) MarshalBinary() ([]byte, error) {
	buf := new(bytes.Buffer)
	buf.WriteByte(byte(TypeTxnEnd))

	if err := binary.Write(buf, binary.BigEndian, t.lsn); err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.BigEndian, t.TransactionID); err != nil {
		return nil, err
	}

	d, err := t.parentLogLocation.MarshalBinary()
	if err != nil {
		return nil, err
	}

	buf.Write(d)

	return buf.Bytes(), nil
}

func (t *TxnEndLogRecord) UnmarshalBinary(data []byte) error {
	if len(data) < 1 || data[0] != byte(TypeTxnEnd) {
		return errors.New("invalid type tag for TxnEndLogRecord")
	}

	reader := bytes.NewReader(data[1:])

	if err := binary.Read(reader, binary.BigEndian, &t.lsn); err != nil {
		return err
	}

	if err := binary.Read(reader, binary.BigEndian, &t.TransactionID); err != nil {
		return err
	}

	if err := binary.Read(reader, binary.BigEndian, &t.parentLogLocation); err != nil {
		return err
	}

	return nil
}

// MarshalBinary for CompensationLogRecord.
func (c *CompensationLogRecord) MarshalBinary() ([]byte, error) {
	buf := new(bytes.Buffer)
	buf.WriteByte(byte(TypeCompensation))

	if err := binary.Write(buf, binary.BigEndian, c.lsn); err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.BigEndian, c.TransactionID); err != nil {
		return nil, err
	}

	d, err := c.parentLogLocation.MarshalBinary()
	if err != nil {
		return nil, err
	}

	buf.Write(d)

	if err := binary.Write(buf, binary.BigEndian, c.nextUndoLSN); err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.BigEndian, c.isDelete); err != nil {
		return nil, err
	}

	pageData, err := c.modifiedPageIdentity.MarshalBinary()
	if err != nil {
		return nil, err
	}

	buf.Write(pageData)

	if err := binary.Write(buf, binary.BigEndian, (c.modifiedSlotNumber)); err != nil {
		return nil, err
	}

	//nolint:gosec
	if err := binary.Write(buf, binary.BigEndian, uint32(len(c.beforeValue))); err != nil {
		return nil, err
	}

	buf.Write(c.beforeValue)

	//nolint:gosec
	if err := binary.Write(buf, binary.BigEndian, uint32(len(c.afterValue))); err != nil {
		return nil, err
	}

	buf.Write(c.afterValue)

	return buf.Bytes(), nil
}

func (c *CompensationLogRecord) UnmarshalBinary(data []byte) error {
	if len(data) < 1 || data[0] != byte(TypeCompensation) {
		return errors.New("invalid type tag for CompensationLogRecord")
	}

	reader := bytes.NewReader(data[1:])

	if err := binary.Read(reader, binary.BigEndian, &c.lsn); err != nil {
		return err
	}

	if err := binary.Read(reader, binary.BigEndian, &c.TransactionID); err != nil {
		return err
	}

	if err := binary.Read(reader, binary.BigEndian, &c.parentLogLocation); err != nil {
		return err
	}

	if err := binary.Read(reader, binary.BigEndian, &c.nextUndoLSN); err != nil {
		return err
	}

	if err := binary.Read(reader, binary.BigEndian, &c.isDelete); err != nil {
		return err
	}

	if err := binary.Read(reader, binary.BigEndian, &c.modifiedPageIdentity); err != nil {
		return err
	}

	if err := binary.Read(reader, binary.BigEndian, &c.modifiedSlotNumber); err != nil {
		return err
	}

	var beforeLen uint32
	if err := binary.Read(reader, binary.BigEndian, &beforeLen); err != nil {
		return err
	}

	c.beforeValue = make([]byte, beforeLen)
	if _, err := io.ReadFull(reader, c.beforeValue); err != nil {
		return err
	}

	var afterLen uint32
	if err := binary.Read(reader, binary.BigEndian, &afterLen); err != nil {
		return err
	}

	c.afterValue = make([]byte, afterLen)
	_, err := io.ReadFull(reader, c.afterValue)

	return err
}

func (c *CheckpointBeginLogRecord) MarshalBinary() ([]byte, error) {
	buf := new(bytes.Buffer)
	buf.WriteByte(byte(TypeCheckpointBegin))

	if err := binary.Write(buf, binary.BigEndian, c.lsn); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (c *CheckpointBeginLogRecord) UnmarshalBinary(data []byte) error {
	if len(data) < 1 || data[0] != byte(TypeCheckpointBegin) {
		return errors.New("invalid type tag for CompensationLogRecord")
	}

	reader := bytes.NewReader(data[1:])
	if err := binary.Read(reader, binary.BigEndian, &c.lsn); err != nil {
		return err
	}

	return nil
}

func (c *CheckpointEndLogRecord) MarshalBinary() ([]byte, error) {
	buf := new(bytes.Buffer)
	buf.WriteByte(byte(TypeCheckpointEnd))

	// Write LSN
	if err := binary.Write(buf, binary.BigEndian, c.lsn); err != nil {
		return nil, err
	}

	// Write active transactions
	//nolint:gosec
	if err := binary.Write(buf, binary.BigEndian, uint32(len(c.activeTransactions))); err != nil {
		return nil, err
	}

	for _, TransactionID := range c.activeTransactions {
		if err := binary.Write(buf, binary.BigEndian, TransactionID); err != nil {
			return nil, err
		}
	}

	// Write dirty page table
	//nolint:gosec
	if err := binary.Write(buf, binary.BigEndian, uint32(len(c.dirtyPageTable))); err != nil {
		return nil, err
	}

	for pageID, pageInfo := range c.dirtyPageTable {
		// Write page identity length and data
		if err := binary.Write(buf, binary.BigEndian, pageID); err != nil {
			return nil, err
		}
		// Write associated LSN
		if err := binary.Write(buf, binary.BigEndian, pageInfo); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

func (c *CheckpointEndLogRecord) UnmarshalBinary(data []byte) error {
	if len(data) < 1 {
		return errors.New("insufficient data for type tag")
	}

	if data[0] != byte(TypeCheckpointEnd) {
		return errors.New("invalid type tag for CheckpointEnd")
	}

	reader := bytes.NewReader(data[1:])

	// Read LSN
	if err := binary.Read(reader, binary.BigEndian, &c.lsn); err != nil {
		return err
	}

	// Read active transactions
	var activeTxnsLen uint32
	if err := binary.Read(reader, binary.BigEndian, &activeTxnsLen); err != nil {
		return err
	}

	c.activeTransactions = make([]txns.TxnID, activeTxnsLen)
	for i := range c.activeTransactions {
		if err := binary.Read(reader, binary.BigEndian, &c.activeTransactions[i]); err != nil {
			return err
		}
	}

	// Read dirty page table
	var dirtyPagesLen uint32
	if err := binary.Read(reader, binary.BigEndian, &dirtyPagesLen); err != nil {
		return err
	}

	c.dirtyPageTable = make(
		map[bufferpool.PageIdentity]LogRecordLocationInfo,
		dirtyPagesLen,
	)

	for i := 0; i < int(dirtyPagesLen); i++ {
		var pageID bufferpool.PageIdentity
		if err := binary.Read(reader, binary.BigEndian, &pageID); err != nil {
			return err
		}

		var logInfo LogRecordLocationInfo
		if err := binary.Read(reader, binary.BigEndian, &logInfo); err != nil {
			return err
		}

		c.dirtyPageTable[pageID] = logInfo
	}

	return nil
}

func readLogRecord(data []byte) (LogRecordTypeTag, any, error) {
	switch LogRecordTypeTag(data[0]) {
	case TypeBegin:
		r := BeginLogRecord{}
		err := r.UnmarshalBinary(data)

		return TypeBegin, r, err
	case TypeUpdate:
		r := UpdateLogRecord{}
		err := r.UnmarshalBinary(data)

		return TypeUpdate, r, err
	case TypeInsert:
		r := InsertLogRecord{}
		err := r.UnmarshalBinary(data)

		return TypeInsert, r, err
	case TypeCommit:
		r := CommitLogRecord{}
		err := r.UnmarshalBinary(data)

		return TypeCommit, r, err
	case TypeAbort:
		r := AbortLogRecord{}
		err := r.UnmarshalBinary(data)

		return TypeAbort, r, err
	case TypeTxnEnd:
		r := TxnEndLogRecord{}
		err := r.UnmarshalBinary(data)

		return TypeTxnEnd, r, err
	case TypeCompensation:
		r := CompensationLogRecord{}
		err := r.UnmarshalBinary(data)

		return TypeCompensation, r, err
	case TypeCheckpointBegin:
		r := CheckpointBeginLogRecord{}
		err := r.UnmarshalBinary(data)

		return TypeCheckpointBegin, r, err
	case TypeCheckpointEnd:
		r := CheckpointEndLogRecord{}
		err := r.UnmarshalBinary(data)

		return TypeCheckpointEnd, r, err
	default:
		assert.Assert(
			data[0] < byte(TypeUnknown),
			"unknow log type: %d",
			data[0],
		)
	}

	panic("unreachable")
}
