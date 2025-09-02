package engine

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/google/uuid"

	"github.com/Blackdeer1524/GraphDB/src/storage"
)

func parseRecord(reader *bytes.Reader, schema storage.Schema) (map[string]any, error) {
	res := make(map[string]any, len(schema))
	for _, colInfo := range schema {
		colName := colInfo.Name
		colType := colInfo.Type
		switch colType {
		case storage.ColumnTypeInt64:
			var result int64
			err := binary.Read(reader, binary.BigEndian, &result)
			if err != nil {
				return nil, err
			}
			res[colName] = result
		case storage.ColumnTypeUint64:
			var result uint64
			err := binary.Read(reader, binary.BigEndian, &result)
			if err != nil {
				return nil, err
			}
			res[colName] = result
		case storage.ColumnTypeFloat64:
			var result float64
			err := binary.Read(reader, binary.BigEndian, &result)
			if err != nil {
				return nil, err
			}
			res[colName] = result
		case storage.ColumnTypeUUID:
			uuidBytes := make([]byte, 36)
			_, err := io.ReadFull(reader, uuidBytes)
			if err != nil {
				return nil, fmt.Errorf("failed to read UUID for column %s: %w", colName, err)
			}

			uuidStr := string(uuidBytes)
			err = uuid.Validate(uuidStr)
			if err != nil {
				return nil, fmt.Errorf("invalid UUID for column %s: %w", colName, err)
			}
			res[colName] = uuidStr
		}
	}

	return res, nil
}

func parseVertexRecord(
	data []byte,
	vertexSchema storage.Schema,
) (storage.VertexInternalFields, map[string]any, error) {
	reader := bytes.NewReader(data)

	vertexInternalFields := storage.VertexInternalFields{}
	err := binary.Read(reader, binary.BigEndian, &vertexInternalFields)
	if err != nil {
		return storage.VertexInternalFields{}, nil, fmt.Errorf(
			"failed to read vertex internal fields: %w",
			err,
		)
	}

	record, err := parseRecord(reader, vertexSchema)
	if err != nil {
		return storage.VertexInternalFields{}, nil, fmt.Errorf(
			"failed to parse vertex record: %w",
			err,
		)
	}
	return vertexInternalFields, record, nil
}

func parseVertexRecordHeader(data []byte) (storage.VertexInternalFields, []byte, error) {
	reader := bytes.NewReader(data)
	vertexInternalFields := storage.VertexInternalFields{}
	err := binary.Read(reader, binary.BigEndian, &vertexInternalFields)
	if err != nil {
		return storage.VertexInternalFields{}, nil, fmt.Errorf(
			"failed to read vertex internal fields: %w",
			err,
		)
	}

	tail, err := io.ReadAll(reader)
	if err != nil {
		return storage.VertexInternalFields{}, nil, fmt.Errorf(
			"failed to read tail: %w",
			err,
		)
	}
	return vertexInternalFields, tail, nil
}

func parseEdgeRecord(
	data []byte,
	edgeSchema storage.Schema,
) (storage.EdgeInternalFields, map[string]any, error) {
	reader := bytes.NewReader(data)
	edgeInternalFields := storage.EdgeInternalFields{}
	err := binary.Read(reader, binary.BigEndian, &edgeInternalFields)
	if err != nil {
		return storage.EdgeInternalFields{}, nil, fmt.Errorf(
			"failed to read edge internal fields: %w",
			err,
		)
	}

	record, err := parseRecord(reader, edgeSchema)
	if err != nil {
		return storage.EdgeInternalFields{}, nil, fmt.Errorf(
			"failed to parse edge record: %w",
			err,
		)
	}
	return edgeInternalFields, record, nil
}

func parseEdgeRecordHeader(data []byte) (storage.EdgeInternalFields, []byte, error) {
	reader := bytes.NewReader(data)
	edgeInternalFields := storage.EdgeInternalFields{}
	err := binary.Read(reader, binary.BigEndian, &edgeInternalFields)
	if err != nil {
		return storage.EdgeInternalFields{}, nil, fmt.Errorf(
			"failed to read edge internal fields: %w",
			err,
		)
	}
	tail, err := io.ReadAll(reader)
	if err != nil {
		return storage.EdgeInternalFields{}, nil, fmt.Errorf(
			"failed to read tail: %w",
			err,
		)
	}
	return edgeInternalFields, tail, nil
}

func parseDirectoryRecord(data []byte) (storage.DirectoryItem, error) {
	reader := bytes.NewReader(data)
	directoryInternalFields := storage.DirectoryItem{}
	err := binary.Read(reader, binary.BigEndian, &directoryInternalFields)
	if err != nil {
		return storage.DirectoryItem{}, fmt.Errorf(
			"failed to read directory internal fields: %w",
			err,
		)
	}
	return directoryInternalFields, nil
}

func _serializeRecord(data map[string]any, schema storage.Schema) ([]byte, error) {
	var buf bytes.Buffer

	for _, colInfo := range schema {
		colName := colInfo.Name
		colType := colInfo.Type

		value, exists := data[colName]
		if !exists {
			return nil, fmt.Errorf("missing value for column %s", colName)
		}

		switch colType {
		case storage.ColumnTypeInt64:
			val, ok := value.(int64)
			if !ok {
				return nil, fmt.Errorf("expected int64 for column %s, got %T", colName, value)
			}
			err := binary.Write(&buf, binary.BigEndian, val)
			if err != nil {
				return nil, err
			}
		case storage.ColumnTypeUint64:
			val, ok := value.(uint64)
			if !ok {
				return nil, fmt.Errorf("expected uint64 for column %s, got %T", colName, value)
			}
			err := binary.Write(&buf, binary.BigEndian, val)
			if err != nil {
				return nil, err
			}
		case storage.ColumnTypeFloat64:
			val, ok := value.(float64)
			if !ok {
				return nil, fmt.Errorf("expected float64 for column %s, got %T", colName, value)
			}
			err := binary.Write(&buf, binary.BigEndian, val)
			if err != nil {
				return nil, err
			}
		case storage.ColumnTypeUUID:
			val, ok := value.(string)
			if !ok {
				return nil, fmt.Errorf("expected string for UUID column %s, got %T", colName, value)
			}
			err := uuid.Validate(val)
			if err != nil {
				return nil, fmt.Errorf("invalid UUID for column %s: %w", colName, err)
			}
			if len(val) != 36 {
				return nil, fmt.Errorf(
					"UUID for column %s must be exactly 36 characters, got %d",
					colName,
					len(val),
				)
			}
			_, err = buf.WriteString(val)
			if err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("unsupported column type for column %s: %v", colName, colType)
		}
	}

	return buf.Bytes(), nil
}

func serializeVertexRecord(
	vertexInternalFields storage.VertexInternalFields,
	record map[string]any,
	vertexSchema storage.Schema,
) ([]byte, error) {
	buf := bytes.Buffer{}

	err := binary.Write(&buf, binary.BigEndian, vertexInternalFields)
	if err != nil {
		return nil, fmt.Errorf("failed to write vertex internal fields: %w", err)
	}

	recordBytes, err := _serializeRecord(record, vertexSchema)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize record: %w", err)
	}

	_, err = buf.Write(recordBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to write record: %w", err)
	}

	return buf.Bytes(), nil
}

func serializeVertexRecordHeader(
	vertexInternalFields storage.VertexInternalFields,
	tail []byte,
) ([]byte, error) {
	buf := bytes.Buffer{}
	err := binary.Write(&buf, binary.BigEndian, vertexInternalFields)
	if err != nil {
		return nil, fmt.Errorf("failed to write vertex internal fields: %w", err)
	}

	_, err = buf.Write(tail)
	if err != nil {
		return nil, fmt.Errorf("failed to write tail: %w", err)
	}

	return buf.Bytes(), nil
}

func serializeEdgeRecord(
	edgeInternalFields storage.EdgeInternalFields,
	record map[string]any,
	edgeSchema storage.Schema,
) ([]byte, error) {
	buf := bytes.Buffer{}
	err := binary.Write(&buf, binary.BigEndian, edgeInternalFields)
	if err != nil {
		return nil, fmt.Errorf("failed to write edge internal fields: %w", err)
	}

	recordBytes, err := _serializeRecord(record, edgeSchema)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize record: %w", err)
	}

	_, err = buf.Write(recordBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to write record: %w", err)
	}

	return buf.Bytes(), nil
}

func serializeEdgeRecordHeader(
	edgeInternalFields storage.EdgeInternalFields,
	tail []byte,
) ([]byte, error) {
	buf := bytes.Buffer{}
	err := binary.Write(&buf, binary.BigEndian, edgeInternalFields)
	if err != nil {
		return nil, fmt.Errorf("failed to write edge internal fields: %w", err)
	}

	_, err = buf.Write(tail)
	if err != nil {
		return nil, fmt.Errorf("failed to write tail: %w", err)
	}
	return buf.Bytes(), nil
}

func serializeDirectoryRecord(
	dirItem storage.DirectoryItem,
) ([]byte, error) {
	buf := bytes.Buffer{}
	err := binary.Write(&buf, binary.BigEndian, dirItem)
	if err != nil {
		return nil, fmt.Errorf("failed to write directory internal fields: %w", err)
	}
	return buf.Bytes(), nil
}
