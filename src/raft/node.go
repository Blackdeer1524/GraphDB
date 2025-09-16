package raft

import (
	"fmt"
	"github.com/Blackdeer1524/GraphDB/src/generated/proto"
	hraft "github.com/hashicorp/raft"
	"github.com/spf13/afero"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"net"
	"sync/atomic"

	"github.com/Jille/raft-grpc-leader-rpc/leaderhealth"
	transport "github.com/Jille/raft-grpc-transport"

	"github.com/Blackdeer1524/GraphDB/src"
	"github.com/Blackdeer1524/GraphDB/src/bufferpool"
	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/query"
	"github.com/Blackdeer1524/GraphDB/src/recovery"
	"github.com/Blackdeer1524/GraphDB/src/storage"
	"github.com/Blackdeer1524/GraphDB/src/storage/disk"
	"github.com/Blackdeer1524/GraphDB/src/storage/engine"
	"github.com/Blackdeer1524/GraphDB/src/storage/index"
	"github.com/Blackdeer1524/GraphDB/src/storage/page"
	"github.com/Blackdeer1524/GraphDB/src/storage/systemcatalog"
	"github.com/Blackdeer1524/GraphDB/src/txns"
)

type Node struct {
	id     string
	addr   string
	raft   *hraft.Raft
	grpc   *grpc.Server
	logger src.Logger

	executor    *query.Executor
	txnLogger   *recovery.TxnLogger
	debugPool   *bufferpool.DebugBufferPool
	lockManager *txns.LockManager
}

func StartNode(id, addr string, logger src.Logger, peers []hraft.Server) (*Node, error) {
	cfg := hraft.DefaultConfig()
	cfg.LocalID = hraft.ServerID(id)

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	tr := transport.New(hraft.ServerAddress(addr), []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	})

	fs := afero.NewOsFs()
	basePath := fmt.Sprintf("/tmp/graphdb_%s", id)
	if err := systemcatalog.InitSystemCatalog(basePath, fs); err != nil {
		return nil, fmt.Errorf("init system catalog failed: %w", err)
	}
	if err := systemcatalog.CreateLogFileIfDoesntExist(basePath, fs); err != nil {
		return nil, fmt.Errorf("init WAL log file failed: %w", err)
	}

	diskMgr := disk.New(
		basePath,
		func(fileID common.FileID, pageID common.PageID) page.SlottedPage {
			return page.NewSlottedPage()
		},
		fs,
	)

	pool := bufferpool.New(1000, bufferpool.NewLRUReplacer(), diskMgr)
	debugPool := bufferpool.NewDebugBufferPool(pool)

	sysCat, err := systemcatalog.New(basePath, fs, debugPool)
	if err != nil {
		return nil, fmt.Errorf("system catalog init failed: %w", err)
	}

	debugPool.MarkPageAsLeaking(systemcatalog.CatalogVersionPageIdent())
	debugPool.MarkPageAsLeaking(recovery.GetMasterPageIdent(systemcatalog.LogFileID))

	txnLogger := recovery.NewTxnLogger(pool, systemcatalog.LogFileID)

	locker := txns.NewLockManager()

	indexLoader := func(idxMeta storage.IndexMeta, pool bufferpool.BufferPool, locker *txns.LockManager,
		logger common.ITxnLoggerWithContext) (storage.Index, error) {
		return index.NewLinearProbingIndex(idxMeta, pool, locker, logger, false, 42)
	}

	se := engine.New(
		sysCat,
		debugPool,
		diskMgr.GetLastFilePage,
		diskMgr.GetEmptyPage,
		locker,
		fs,
		indexLoader,
		false,
	)

	exec := query.New(se, locker)

	fsmInstance := &fsm{
		nodeID:    id,
		log:       logger,
		executor:  exec,
		txnLogger: txnLogger,
	}

	logStore := hraft.NewInmemStore()
	stableStore := hraft.NewInmemStore()
	snapStore := hraft.NewInmemSnapshotStore()
	r, err := hraft.NewRaft(cfg, fsmInstance, logStore, stableStore, snapStore, tr.Transport())
	if err != nil {
		return nil, fmt.Errorf("failed to create raft node: %w", err)
	}
	if len(peers) > 0 {
		r.BootstrapCluster(hraft.Configuration{Servers: peers})
	}

	//TODO remove when we have DDL open api support
	ticker := atomic.Uint64{}
	err = addMoneyNodeType(&ticker, exec, txnLogger)
	if err != nil {
		return nil, fmt.Errorf("failed to create money node type: %w", err)
	}

	s := grpc.NewServer()

	proto.RegisterRaftServiceServer(s, New(r, &ticker, logger))
	tr.Register(s)
	leaderhealth.Setup(r, s, []string{"graphdb"})

	node := &Node{
		id:          id,
		addr:        addr,
		raft:        r,
		grpc:        s,
		logger:      logger,
		executor:    exec,
		txnLogger:   txnLogger,
		debugPool:   debugPool,
		lockManager: locker,
	}

	go func() {
		if serveErr := s.Serve(lis); serveErr != nil {
			logger.Errorw("Raft transport gRPC server stopped", zap.Error(serveErr))
		}
	}()

	return node, nil
}

func execute(
	ticker *atomic.Uint64,
	executor *query.Executor,
	logger common.ITxnLogger,
	fn query.Task,
) (err error) {
	txnID := common.TxnID(ticker.Add(1))
	defer executor.Locker.Unlock(txnID)

	ctxLogger := logger.WithContext(txnID)
	if err := ctxLogger.AppendBegin(); err != nil {
		return fmt.Errorf("failed to append begin: %w", err)
	}

	defer func() {
		if err != nil {
			ctxLogger.Rollback()
			return
		}
		if err = ctxLogger.AppendCommit(); err != nil {
			err = fmt.Errorf("failed to append commit: %w", err)
		} else if err = ctxLogger.AppendTxnEnd(); err != nil {
			err = fmt.Errorf("failed to append txn end: %w", err)
		}
	}()

	return fn(txnID, executor, ctxLogger)
}

func addMoneyNodeType(
	ticker *atomic.Uint64,
	e *query.Executor,
	logger common.ITxnLogger,
) error {
	return execute(
		ticker,
		e,
		logger,
		func(txnID common.TxnID, e *query.Executor, logger common.ITxnLoggerWithContext) (err error) {
			tableName := "main"
			schema := storage.Schema{
				{Name: "money", Type: storage.ColumnTypeFloat64},
			}
			err = e.CreateVertexType(txnID, tableName, schema, logger)
			return nil
		},
	)
}

func (n *Node) Close() {
	if err := n.raft.Shutdown().Error(); err != nil {
		n.logger.Errorw("raft node failed to close raft", zap.Error(err))
	}
	n.grpc.GracefulStop()
	n.logger.Infow("raft node gracefully stopped", zap.String("address", n.addr))
}
