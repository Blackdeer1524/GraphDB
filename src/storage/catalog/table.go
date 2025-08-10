package catalog

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/Blackdeer1524/GraphDB/src/pkg/utils"
	"github.com/Blackdeer1524/GraphDB/src/txns"
)

func (m *Manager) generateVertexTableFilePath(name string) string {
	return filepath.Join(m.basePath, "tables", fmt.Sprintf("vertex-%s.tbl", name))
}

func (m *Manager) CreateVertexTable(txnID txns.TxnID, name string, schema map[string]Column) error {
	systemCatalogLockReq := txns.SystemCatalogLockRequest{
		TxnID:    txnID,
		LockMode: txns.SystemCatalogExclusive,
	}

	if !m.lock.SystemCatalogLock(systemCatalogLockReq) {
		return fmt.Errorf("failed to acquire system catalog exclusive lock")
	}
	defer m.lock.SystemCatalogUnlock(systemCatalogLockReq)

	m.mx.Lock()
	defer m.mx.Unlock()

	_, ok := m.catalog.VertexTables[name]
	if ok {
		return fmt.Errorf("vertex table %s already exists", name)
	}

	newTable := VertexTable{
		Name:       name,
		Schema:     schema,
		PathToFile: m.generateVertexTableFilePath(name),
	}

	file, err := os.Create(newTable.PathToFile)
	if err != nil {
		return fmt.Errorf("failed to create table file: %w", err)
	}
	file.Close()

	newTable.ID = uint64(len(m.vertexTablesIDs))

	m.catalog.VertexTables[name] = newTable
	m.vertexTablesIDs[name] = newTable.ID

	return nil
}

func (m *Manager) DropVertexTable(txnID txns.TxnID, name string) error {
	systemCatalogLockReq := txns.SystemCatalogLockRequest{
		TxnID:    txnID,
		LockMode: txns.SystemCatalogExclusive,
	}

	sysLock := m.lock.SystemCatalogLock(systemCatalogLockReq)
	if !sysLock {
		return fmt.Errorf("failed to acquire system catalog lock")
	}
	defer m.lock.SystemCatalogUnlock(systemCatalogLockReq)

	m.mx.Lock()
	defer m.mx.Unlock()

	table, ok := m.catalog.VertexTables[name]
	if !ok {
		return fmt.Errorf("vertex table %s does not exist", name)
	}

	vertexTableID, ok := m.vertexTablesIDs[name]
	if !ok {
		return fmt.Errorf("vertex table %s does not exist", name)
	}

	tableLockReq := txns.TableLockRequest{
		TxnID:    txnID,
		TableID:  txns.TableID(vertexTableID),
		LockMode: txns.TABLE_LOCK_EXCLUSIVE,
	}

	tableLock := m.lock.TableLock(tableLockReq)
	if !tableLock {
		return fmt.Errorf("failed to acquire table lock for %s", name)
	}
	defer m.lock.TableUnlock(tableLockReq)

	err := os.Remove(table.PathToFile)
	if err != nil {
		return fmt.Errorf("failed to remove file: %w", err)
	}

	delete(m.catalog.VertexTables, name)
	delete(m.vertexTablesIDs, name)

	return nil
}

func (m *Manager) GetVertexTable(txnID txns.TxnID, name string) (utils.WithUnlock[VertexTable], error) {
	tableLock := false

	scSharedLockReq := txns.SystemCatalogLockRequest{
		TxnID:    txnID,
		LockMode: txns.SystemCatalogIntentionShared,
	}

	sysLock := m.lock.SystemCatalogLock(scSharedLockReq)
	if !sysLock {
		return utils.WithUnlock[VertexTable]{}, fmt.Errorf("failed to acquire system catalog lock")
	}
	defer func() {
		if !tableLock {
			m.lock.SystemCatalogUnlock(scSharedLockReq)
		}
	}()

	m.mx.RLock()

	table, ok := m.catalog.VertexTables[name]
	if !ok {
		m.mx.RUnlock()

		return utils.WithUnlock[VertexTable]{}, fmt.Errorf("vetex table %s does not exist", name)
	}

	vertexTableID, ok := m.vertexTablesIDs[name]
	if !ok {
		m.mx.RUnlock()

		return utils.WithUnlock[VertexTable]{}, fmt.Errorf("vertex table %s does not exist", name)
	}

	m.mx.RUnlock()

	tableLockReq := txns.TableLockRequest{
		TxnID:    txnID,
		TableID:  txns.TableID(vertexTableID),
		LockMode: txns.TABLE_LOCK_SHARED,
	}

	tableLock = m.lock.TableLock(tableLockReq)
	if !tableLock {
		return utils.WithUnlock[VertexTable]{}, fmt.Errorf("failed to acquire table lock for %s", name)
	}

	return utils.WithUnlock[VertexTable]{
		Resource: table,
		UnlockFn: func() error {
			m.lock.TableUnlock(tableLockReq)
			m.lock.SystemCatalogUnlock(scSharedLockReq)

			return nil
		},
	}, nil
}

func (m *Manager) generateNodesTableFilePath(name string) string {
	return filepath.Join(m.basePath, "tables", fmt.Sprintf("nodes-%s.tbl", name))
}

func (m *Manager) CreateNodesTable(txnID txns.TxnID, name string, schema map[string]Column) error {
	systemCatalogLockReq := txns.SystemCatalogLockRequest{
		TxnID:    txnID,
		LockMode: txns.SystemCatalogExclusive,
	}

	if !m.lock.SystemCatalogLock(systemCatalogLockReq) {
		return fmt.Errorf("failed to acquire system catalog exclusive lock")
	}
	defer m.lock.SystemCatalogUnlock(systemCatalogLockReq)

	m.mx.Lock()
	defer m.mx.Unlock()

	_, ok := m.catalog.NodeTables[name]
	if ok {
		return fmt.Errorf("nodes table %s already exists", name)
	}

	newTable := NodeTable{
		Name:       name,
		Schema:     schema,
		PathToFile: m.generateNodesTableFilePath(name),
	}

	file, err := os.Create(newTable.PathToFile)
	if err != nil {
		return fmt.Errorf("failed to create table file: %w", err)
	}
	file.Close()

	newTable.ID = uint64(len(m.nodesTablesIDs))

	m.catalog.NodeTables[name] = newTable
	m.nodesTablesIDs[name] = newTable.ID

	return nil
}

func (m *Manager) DropNodesTable(txnID txns.TxnID, name string) error {
	systemCatalogLockReq := txns.SystemCatalogLockRequest{
		TxnID:    txnID,
		LockMode: txns.SystemCatalogExclusive,
	}

	sysLock := m.lock.SystemCatalogLock(systemCatalogLockReq)
	if !sysLock {
		return fmt.Errorf("failed to acquire system catalog lock")
	}
	defer m.lock.SystemCatalogUnlock(systemCatalogLockReq)

	m.mx.Lock()
	defer m.mx.Unlock()

	table, ok := m.catalog.NodeTables[name]
	if !ok {
		return fmt.Errorf("nodes table %s does not exist", name)
	}

	nodesTableID, ok := m.nodesTablesIDs[name]
	if !ok {
		return fmt.Errorf("nodes table %s does not exist", name)
	}

	tableLockReq := txns.TableLockRequest{
		TxnID:    txnID,
		TableID:  txns.TableID(nodesTableID),
		LockMode: txns.TABLE_LOCK_EXCLUSIVE,
	}

	tableLock := m.lock.TableLock(tableLockReq)
	if !tableLock {
		return fmt.Errorf("failed to acquire table lock for %s", name)
	}
	defer m.lock.TableUnlock(tableLockReq)

	err := os.Remove(table.PathToFile)
	if err != nil {
		return fmt.Errorf("failed to remove file: %w", err)
	}

	delete(m.catalog.NodeTables, name)
	delete(m.nodesTablesIDs, name)

	return nil
}

func (m *Manager) GetNodesTable(txnID txns.TxnID, name string) (utils.WithUnlock[NodeTable], error) {
	tableLock := false

	scSharedLockReq := txns.SystemCatalogLockRequest{
		TxnID:    txnID,
		LockMode: txns.SystemCatalogIntentionShared,
	}

	sysLock := m.lock.SystemCatalogLock(scSharedLockReq)
	if !sysLock {
		return utils.WithUnlock[NodeTable]{}, fmt.Errorf("failed to acquire system catalog lock")
	}
	defer func() {
		if !tableLock {
			m.lock.SystemCatalogUnlock(scSharedLockReq)
		}
	}()

	m.mx.RLock()

	table, ok := m.catalog.NodeTables[name]
	if !ok {
		m.mx.RUnlock()

		return utils.WithUnlock[NodeTable]{}, fmt.Errorf("nodes table %s does not exist", name)
	}

	vertexTableID, ok := m.nodesTablesIDs[name]
	if !ok {
		m.mx.RUnlock()

		return utils.WithUnlock[NodeTable]{}, fmt.Errorf("nodes table %s does not exist", name)
	}

	m.mx.RUnlock()

	tableLockReq := txns.TableLockRequest{
		TxnID:    txnID,
		TableID:  txns.TableID(vertexTableID),
		LockMode: txns.TABLE_LOCK_SHARED,
	}

	tableLock = m.lock.TableLock(tableLockReq)
	if !tableLock {
		return utils.WithUnlock[NodeTable]{}, fmt.Errorf("failed to acquire table lock for %s", name)
	}

	return utils.WithUnlock[NodeTable]{
		Resource: table,
		UnlockFn: func() error {
			m.lock.TableUnlock(tableLockReq)
			m.lock.SystemCatalogUnlock(scSharedLockReq)

			return nil
		},
	}, nil
}
