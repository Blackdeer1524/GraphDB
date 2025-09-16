package delivery

import (
	"context"
	"errors"
	"fmt"
	"github.com/Blackdeer1524/GraphDB/src/generated/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"net/http"
	"strings"
	"time"

	"github.com/Blackdeer1524/GraphDB/src"
	"github.com/Blackdeer1524/GraphDB/src/generated/api"

	_ "github.com/Jille/grpc-multi-resolver"
	"github.com/grpc-ecosystem/go-grpc-middleware/retry"
)

type Server struct {
	Host string
	Port int

	nodesAddr []string
	log       src.Logger
	http      *http.Server
}

func NewServer(host string, port int, nodes []string, log src.Logger) *Server {
	return &Server{
		Host:      host,
		Port:      port,
		nodesAddr: nodes,
		log:       log,
	}
}

func (s *Server) Run() error {
	serviceConfig := `{"healthCheckConfig": {"serviceName": "graphdb"}, "loadBalancingConfig": [ { "round_robin": {} } ]}`
	retryOpts := []grpc_retry.CallOption{
		grpc_retry.WithBackoff(grpc_retry.BackoffExponential(100 * time.Millisecond)),
		grpc_retry.WithMax(5),
	}

	cl, err := grpc.NewClient(
		fmt.Sprintf("multi:///%s", strings.Join(s.nodesAddr, ",")),
		grpc.WithDefaultServiceConfig(serviceConfig),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
		grpc.WithUnaryInterceptor(grpc_retry.UnaryClientInterceptor(retryOpts...)))
	if err != nil {
		return fmt.Errorf("Server.Run grpc.NewClient: %w", err)
	}

	cl.Connect()
	defer func(cl *grpc.ClientConn) {
		err := cl.Close()
		if err != nil {
			s.log.Errorf("Server.Run grpc.NewClient cl.Close: %w", err)
		}
	}(cl)

	raftCl := proto.NewRaftServiceClient(cl)

	h := &APIHandler{
		Client: raftCl,
		Logger: s.log,
	}

	srv, err := api.NewServer(h)
	if err != nil {
		return err
	}

	mux := http.DefaultServeMux
	mux.Handle("/", srv)

	s.http = &http.Server{
		Addr: fmt.Sprintf(
			"%s:%d",
			s.Host,
			s.Port,
		),
		Handler:           mux,
		ReadHeaderTimeout: time.Second * 10,
	}

	s.log.Infof(
		"Server is running on %s:%d",
		s.Host,
		s.Port,
	)

	if err := s.http.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("Server.Run http.ListenAndServe: %w", err)
	}

	return nil
}

func (s *Server) Close(ctx context.Context) error {
	if s.http == nil {
		return nil
	}

	if err := s.http.Shutdown(ctx); err != nil &&
		!errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("Server.Close http.Shutdown: %w", err)
	}

	s.log.Info("Server is closed")

	return nil
}
