package raft

import (
	"fmt"
	"net"

	"go.uber.org/zap"

	"github.com/Jille/raft-grpc-leader-rpc/leaderhealth"
	transport "github.com/Jille/raft-grpc-transport"
	hraft "github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/Blackdeer1524/GraphDB/src"
)

type Node struct {
	id   string
	addr string
	raft *hraft.Raft
	grpc *grpc.Server

	logger src.Logger
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

	logStore := hraft.NewInmemStore()
	stableStore := hraft.NewInmemStore()
	snapStore := hraft.NewInmemSnapshotStore()

	r, err := hraft.NewRaft(cfg, nil, logStore, stableStore, snapStore, tr.Transport())
	if err != nil {
		return nil, fmt.Errorf("failed to create raft node: %w", err)
	}

	if len(peers) > 0 {
		r.BootstrapCluster(hraft.Configuration{Servers: peers})
	}

	s := grpc.NewServer()
	tr.Register(s)
	leaderhealth.Setup(r, s, []string{"graphdb"})

	n := &Node{
		id:     id,
		addr:   addr,
		raft:   r,
		grpc:   s,
		logger: logger,
	}

	go func() {
		err := s.Serve(lis)
		if err != nil {
			n.logger.Errorw("raft node failed to serve", zap.Error(err))
		}
	}()

	return n, nil
}

func (n *Node) Close() {
	if err := n.raft.Shutdown().Error(); err != nil {
		n.logger.Errorw("raft node failed to close raft", zap.Error(err))
	}
	n.grpc.GracefulStop()
	n.logger.Infow("raft node gracefully stopped", zap.String("address", n.addr))
}
