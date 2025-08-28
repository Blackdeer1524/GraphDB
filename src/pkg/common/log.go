package common

import (
	"bytes"
	"encoding/binary"
)

type LSN uint64

var NilLSN LSN = LSN(0)

// is considered NIL iff lsn is NIL_LSN
type LogRecordLocInfo struct {
	Lsn      LSN
	Location FileLocation
}

func NewNilLogRecordLocation() LogRecordLocInfo {
	return LogRecordLocInfo{
		Lsn:      NilLSN,
		Location: FileLocation{},
	}
}

func (p *LogRecordLocInfo) IsNil() bool {
	return p.Lsn == NilLSN
}

func (l *LogRecordLocInfo) MarshalBinary() ([]byte, error) {
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

func (l *LogRecordLocInfo) UnmarshalBinary(data []byte) error {
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
