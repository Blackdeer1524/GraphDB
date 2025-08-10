package catalog

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/Blackdeer1524/GraphDB/src/txns"
)

const (
	catalogFile = "system_catalog.json"
)

type Locker interface {
	TableLock(txns.TableLockRequest) bool
	TableUnlock(txns.TableLockRequest)

	SystemCatalogLock(txns.SystemCatalogLockRequest) bool
	SystemCatalogUnlock(txns.SystemCatalogLockRequest)
}

type Manager struct {
	basePath    string
	catalogPath string
	catalog     *SystemCatalog

	nodesTablesIDs  map[string]uint64
	vertexTablesIDs map[string]uint64

	lock Locker
	mx   sync.RWMutex
}

func exists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}

	if os.IsNotExist(err) {
		return false, nil
	}

	return false, err
}

func NewManager(basePath string, mx Locker) (*Manager, error) {
	path := filepath.Join(basePath, catalogFile)

	ok, err := exists(path)
	if err != nil {
		return nil, fmt.Errorf("failed to check existence of catalog file: %w", err)
	}

	if !ok {
		sc := new(SystemCatalog)

		data, err := json.Marshal(sc)
		if err != nil {
			return nil, fmt.Errorf("failed to marsh to yaml: %w", err)
		}

		err = os.WriteFile(path, data, 0644)
		if err != nil {
			return nil, fmt.Errorf("failed to write catalog file: %w", err)
		}
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read catalog file: %w", err)
	}

	var sc SystemCatalog

	err = json.Unmarshal(data, &sc)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal catalog file: %w", err)
	}

	return &Manager{
		basePath:    basePath,
		catalogPath: path,
		catalog:     &sc,
		lock:        mx,
	}, nil
}
