package queryexecutor

import (
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

type MockRowSource struct {
	rows  []Row
	index int
	err   error
}

func (m *MockRowSource) Next() (Row, error) {
	if m.err != nil {
		return nil, m.err
	}
	if m.index >= len(m.rows) {
		return nil, io.EOF
	}
	row := m.rows[m.index]
	m.index++
	return row, nil
}

func TestCollectRows_WithFilter(t *testing.T) {
	source := &MockRowSource{
		rows: []Row{
			{"id": 1, "name": "Node1"},
			{"id": 2, "name": "Node2"},
		},
	}

	filter := func(row Row) bool {
		id, _ := row["id"].(int)
		return id > 1
	}

	rows, err := collectRows(source, filter)

	require.NoError(t, err)
	require.Len(t, rows, 1)

	require.Equal(t, []Row{
		{"id": 2, "name": "Node2"},
	}, rows)
}

func TestCollectRows_WithoutFilter(t *testing.T) {
	source := &MockRowSource{
		rows: []Row{
			{"id": 1, "name": "Node1"},
			{"id": 2, "name": "Node2"},
		},
	}

	rows, err := collectRows(source, nil)

	require.NoError(t, err)
	require.Len(t, rows, 2)
}

func TestCollectRows_ReadError(t *testing.T) {
	source := &MockRowSource{
		err: errors.New("read error"),
	}

	rows, err := collectRows(source, nil)

	require.Error(t, err)
	require.EqualError(t, err, "read error")
	require.Nil(t, rows)
}

func TestCollectRows_EmptySource(t *testing.T) {
	source := &MockRowSource{
		rows: []Row{},
	}

	rows, err := collectRows(source, nil)

	require.NoError(t, err)
	require.Empty(t, rows)
}

func TestCollectRows_FilterRejectAll(t *testing.T) {
	source := &MockRowSource{
		rows: []Row{
			{"id": 1, "name": "Node1"},
			{"id": 2, "name": "Node2"},
		},
	}

	filter := func(row Row) bool {
		return false
	}

	rows, err := collectRows(source, filter)

	require.NoError(t, err)
	require.Empty(t, rows)
}

func TestCollectRows_FilterAcceptAll(t *testing.T) {
	source := &MockRowSource{
		rows: []Row{
			{"id": 1, "name": "Node1"},
			{"id": 2, "name": "Node2"},
		},
	}

	filter := func(row Row) bool {
		return true
	}

	rows, err := collectRows(source, filter)

	require.NoError(t, err)
	require.Len(t, rows, 2)

	require.Equal(t, []Row{
		{"id": 1, "name": "Node1"},
		{"id": 2, "name": "Node2"},
	}, rows)
}
