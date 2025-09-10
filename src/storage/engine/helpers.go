package engine

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"unsafe"

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
			uuidBytes := make([]byte, unsafe.Sizeof(uuid.UUID{}))
			_, err := io.ReadFull(reader, uuidBytes)
			if err != nil {
				return nil, fmt.Errorf("failed to read UUID for column %s: %w", colName, err)
			}

			uuidVal := uuid.UUID(uuidBytes)
			if uuidVal.String() == "" {
				return nil, fmt.Errorf("invalid UUID for column %s. uuid: %v", colName, uuidVal)
			}
			res[colName] = uuidVal
		}
	}

	return res, nil
}

func parseVertexRecord(
	data []byte,
	vertexSchema storage.Schema,
) (storage.VertexSystemFields, map[string]any, error) {
	reader := bytes.NewReader(data)

	vertexSystemFields := storage.VertexSystemFields{}
	err := binary.Read(reader, binary.BigEndian, &vertexSystemFields)
	if err != nil {
		return storage.VertexSystemFields{}, nil, fmt.Errorf(
			"failed to read vertex internal fields: %w",
			err,
		)
	}

	record, err := parseRecord(reader, vertexSchema)
	if err != nil {
		return storage.VertexSystemFields{}, nil, fmt.Errorf(
			"failed to parse vertex record: %w",
			err,
		)
	}
	return vertexSystemFields, record, nil
}

func parseVertexRecordHeader(data []byte) (storage.VertexSystemFields, []byte, error) {
	reader := bytes.NewReader(data)
	vertexSystemFields := storage.VertexSystemFields{}
	err := binary.Read(reader, binary.BigEndian, &vertexSystemFields)
	if err != nil {
		return storage.VertexSystemFields{}, nil, fmt.Errorf(
			"failed to read vertex internal fields: %w",
			err,
		)
	}

	tail, err := io.ReadAll(reader)
	if err != nil {
		return storage.VertexSystemFields{}, nil, fmt.Errorf(
			"failed to read tail: %w",
			err,
		)
	}
	return vertexSystemFields, tail, nil
}

func parseEdgeRecord(
	data []byte,
	edgeSchema storage.Schema,
) (storage.EdgeSystemFields, map[string]any, error) {
	reader := bytes.NewReader(data)
	edgeSystemFields := storage.EdgeSystemFields{}
	err := binary.Read(reader, binary.BigEndian, &edgeSystemFields)
	if err != nil {
		return storage.EdgeSystemFields{}, nil, fmt.Errorf(
			"failed to read edge internal fields: %w",
			err,
		)
	}

	record, err := parseRecord(reader, edgeSchema)
	if err != nil {
		return storage.EdgeSystemFields{}, nil, fmt.Errorf(
			"failed to parse edge record: %w",
			err,
		)
	}
	return edgeSystemFields, record, nil
}

func parseEdgeRecordHeader(data []byte) (storage.EdgeSystemFields, []byte, error) {
	reader := bytes.NewReader(data)
	edgeSystemFields := storage.EdgeSystemFields{}
	err := binary.Read(reader, binary.BigEndian, &edgeSystemFields)
	if err != nil {
		return storage.EdgeSystemFields{}, nil, fmt.Errorf(
			"failed to read edge internal fields: %w",
			err,
		)
	}
	tail, err := io.ReadAll(reader)
	if err != nil {
		return storage.EdgeSystemFields{}, nil, fmt.Errorf(
			"failed to read tail: %w",
			err,
		)
	}
	return edgeSystemFields, tail, nil
}

func parseDirectoryRecord(data []byte) (storage.DirectoryItem, error) {
	reader := bytes.NewReader(data)
	directorySystemFields := storage.DirectoryItem{}
	err := binary.Read(reader, binary.BigEndian, &directorySystemFields)
	if err != nil {
		return storage.DirectoryItem{}, fmt.Errorf(
			"failed to read directory internal fields: %w",
			err,
		)
	}
	return directorySystemFields, nil
}

func serializeRecord(data map[string]any, schema storage.Schema) ([]byte, error) {
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
			val, ok := value.(uuid.UUID)
			if !ok {
				return nil, fmt.Errorf("expected string for UUID column %s, got %T", colName, value)
			}
			if val.String() == "" {
				return nil, fmt.Errorf("invalid UUID for column %s: %v", colName, val)
			}

			_, err := buf.Write(val[:])
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
	vertexSystemFields storage.VertexSystemFields,
	record map[string]any,
	vertexSchema storage.Schema,
) ([]byte, error) {
	buf := bytes.Buffer{}

	err := binary.Write(&buf, binary.BigEndian, vertexSystemFields)
	if err != nil {
		return nil, fmt.Errorf("failed to write vertex internal fields: %w", err)
	}

	recordBytes, err := serializeRecord(record, vertexSchema)
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
	vertexSystemFields storage.VertexSystemFields,
	tail []byte,
) ([]byte, error) {
	buf := bytes.Buffer{}
	err := binary.Write(&buf, binary.BigEndian, vertexSystemFields)
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
	edgeSystemFields storage.EdgeSystemFields,
	record map[string]any,
	edgeSchema storage.Schema,
) ([]byte, error) {
	buf := bytes.Buffer{}
	err := binary.Write(&buf, binary.BigEndian, edgeSystemFields)
	if err != nil {
		return nil, fmt.Errorf("failed to write edge internal fields: %w", err)
	}

	recordBytes, err := serializeRecord(record, edgeSchema)
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
	edgeSystemFields storage.EdgeSystemFields,
	tail []byte,
) ([]byte, error) {
	buf := bytes.Buffer{}
	err := binary.Write(&buf, binary.BigEndian, edgeSystemFields)
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

func extractDirectoryColumns(directory storage.DirectoryItem, columns []string) ([]byte, error) {
	buf := bytes.Buffer{}
	if len(columns) != 1 || columns[0] != "ID" {
		return nil, fmt.Errorf("directory item has only one indexable column: `ID`")
	}

	for _, colName := range columns {
		if colName == "ID" {
			err := binary.Write(&buf, binary.BigEndian, directory.ID)
			if err != nil {
				return nil, fmt.Errorf("failed to write directory ID: %w", err)
			}
		}
	}

	return buf.Bytes(), nil
}

func extractVertexColumns(vertex storage.Vertex, columns []string) ([]byte, error) {
	buf := bytes.Buffer{}

	for _, colName := range columns {
		if colName == "ID" {
			err := binary.Write(&buf, binary.BigEndian, vertex.ID)
			if err != nil {
				return nil, fmt.Errorf("failed to write vertex ID: %w", err)
			}
			continue
		}

		value, exists := vertex.Data[colName]
		if !exists {
			return nil, fmt.Errorf("missing value for column %s", colName)
		}

		switch v := value.(type) {
		case int64:
			err := binary.Write(&buf, binary.BigEndian, v)
			if err != nil {
				return nil, fmt.Errorf("failed to write int64 column %s: %w", colName, err)
			}
		case uint64:
			err := binary.Write(&buf, binary.BigEndian, v)
			if err != nil {
				return nil, fmt.Errorf("failed to write uint64 column %s: %w", colName, err)
			}
		case float64:
			err := binary.Write(&buf, binary.BigEndian, v)
			if err != nil {
				return nil, fmt.Errorf("failed to write float64 column %s: %w", colName, err)
			}
		case uuid.UUID:
			_, err := buf.Write(v[:])
			if err != nil {
				return nil, fmt.Errorf("failed to write UUID column %s: %w", colName, err)
			}
		}
	}

	return buf.Bytes(), nil
}

func extractEdgeColumns(edge storage.Edge, columns []string) ([]byte, error) {
	buf := bytes.Buffer{}

	for _, colName := range columns {
		if colName == "ID" {
			err := binary.Write(&buf, binary.BigEndian, edge.ID)
			if err != nil {
				return nil, fmt.Errorf("failed to write edge ID: %w", err)
			}
		}

		value, exists := edge.Data[colName]
		if !exists {
			return nil, fmt.Errorf("missing value for column %s", colName)
		}

		switch v := value.(type) {
		case int64:
			err := binary.Write(&buf, binary.BigEndian, v)
			if err != nil {
				return nil, fmt.Errorf("failed to write int64 column %s: %w", colName, err)
			}
		case uint64:
			err := binary.Write(&buf, binary.BigEndian, v)
			if err != nil {
				return nil, fmt.Errorf("failed to write uint64 column %s: %w", colName, err)
			}
		case float64:
			err := binary.Write(&buf, binary.BigEndian, v)
			if err != nil {
				return nil, fmt.Errorf("failed to write float64 column %s: %w", colName, err)
			}
		case uuid.UUID:
			_, err := buf.Write(v[:])
			if err != nil {
				return nil, fmt.Errorf("failed to write UUID column %s: %w", colName, err)
			}
		}
	}

	return buf.Bytes(), nil
}
