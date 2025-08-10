package catalog

func (m *Manager) CreateIndex(name string, reqIndex CreateIndexRequest) error {

	return nil
}

func (m *Manager) DropIndex(name string) error {

	return nil
}

func (m *Manager) GetIndex(name string) (*Index, error) {
	return nil, nil
}
func (m *Manager) ListIndexesByTable(tableName string) []Index {
	indexes := make([]Index, 0, len(m.catalog.Indexes))

	for _, index := range m.catalog.Indexes {
		if index.TableName == tableName {
			indexes = append(indexes, index)
		}
	}

	return indexes
}
