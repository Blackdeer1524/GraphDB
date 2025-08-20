package common

import (
	"bytes"
	"encoding/binary"
)

type PageID uint64
type FileID uint64
type TableID uint64

/* a monotonically increasing counter. It is guaranteed to be unique between transactions
 * WARN: there might be problems with synchronization
 *       in distributed systems that use this kind of transaction IDs */
type TxnID uint64

type PageIdentity struct {
	FileID FileID
	PageID PageID
}

func (p PageIdentity) MarshalBinary() ([]byte, error) {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.BigEndian, p.FileID)
	if err != nil {
		return nil, err
	}
	err = binary.Write(buf, binary.BigEndian, p.PageID)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (p *PageIdentity) UnmarshalBinary(data []byte) error {
	rd := bytes.NewReader(data)
	if err := binary.Read(rd, binary.BigEndian, &p.FileID); err != nil {
		return err
	}

	return binary.Read(rd, binary.BigEndian, &p.PageID)
}

type FileLocation struct {
	PageID  PageID
	SlotNum uint16
}

func (f *FileLocation) MarshalBinary() ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.BigEndian, f.PageID); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, f.SlotNum); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (f *FileLocation) UnmarshalBinary(data []byte) error {
	rd := bytes.NewReader(data)
	if err := binary.Read(rd, binary.BigEndian, &f.PageID); err != nil {
		return err
	}
	return binary.Read(rd, binary.BigEndian, &f.SlotNum)
}

type RecordID struct {
	FileID  FileID
	PageID  PageID
	SlotNum uint16
}

func (r RecordID) PageIdentity() PageIdentity {
	return PageIdentity{
		FileID: r.FileID,
		PageID: r.PageID,
	}
}

func (r RecordID) FileLocation() FileLocation {
	return FileLocation{
		PageID:  r.PageID,
		SlotNum: r.SlotNum,
	}
}
