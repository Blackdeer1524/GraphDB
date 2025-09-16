package app

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	hraft "github.com/hashicorp/raft"

	"github.com/Blackdeer1524/GraphDB/src"
	"github.com/Blackdeer1524/GraphDB/src/delivery"
	"github.com/Blackdeer1524/GraphDB/src/pkg/utils"
	"github.com/Blackdeer1524/GraphDB/src/raft"
)

const CloseTimeout = 15 * time.Second

type ServerEntrypoint struct {
	ConfigPath string
	Env        envVars
	Log        src.Logger

	server *delivery.Server
	nodes  []*raft.Node
}

func (e *ServerEntrypoint) Init(_ context.Context) error {
	e.Env = mustLoadEnv()

	var log src.Logger
	if e.Env.Environment == EnvDev {
		log = utils.Must(zap.NewDevelopment()).Sugar()
	} else {
		log = utils.Must(zap.NewProduction()).Sugar()
	}

	e.Log = log

	e.server = delivery.NewServer(e.Env.ServerHost, e.Env.ServerPort, e.Env.NodesAddr, log)

	return nil
}

func (e *ServerEntrypoint) Run(_ context.Context) error {
	peers := make([]hraft.Server, 0, len(e.Env.NodesAddr)-1)
	for i := 0; i < len(e.Env.NodesAddr); i++ {
		peers = append(peers, hraft.Server{
			Suffrage: hraft.Voter,
			ID:       hraft.ServerID(fmt.Sprintf("node%d", i)),
			Address:  hraft.ServerAddress(e.Env.NodesAddr[i]),
		})
	}

	e.nodes = make([]*raft.Node, 0, len(e.Env.NodesAddr))
	for i, addr := range e.Env.NodesAddr {
		var nodePeers []hraft.Server
		if i == 0 {
			nodePeers = peers
		}

		node, err := raft.StartNode(fmt.Sprintf("node%d", i), addr, e.Log, nodePeers)
		if err != nil {
			return fmt.Errorf("failed to create node %d: %w", i, err)
		}

		e.nodes = append(e.nodes, node)
	}

	return e.server.Run()
}

func (e *ServerEntrypoint) Close() (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), CloseTimeout)
	defer cancel()

	if e.server != nil {
		err = e.server.Close(ctx)
	}

	for _, node := range e.nodes {
		node.Close()
	}

	if e.Log != nil {
		if err != nil {
			e.Log.Error("failed to close server", zap.Error(err))
		}
	}

	return
}
