package agent

import (
	"context"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/vlamug/pdlog/api/v1"
	"github.com/vlamug/pdlog/internal/discovery"
	"github.com/vlamug/pdlog/internal/log"
	"github.com/vlamug/pdlog/internal/server"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Agent runs on every service instance, setting up and connecting all the different components
type (
	Agent struct {
		Config

		log         *log.Log
		httpServer  *http.Server
		grpcServer  *grpc.Server
		debugServer *server.DebugServer
		membership  *discovery.Membership
		replicator  *log.Replicator
		logger      *zap.Logger

		shutdown     bool
		shutdowns    chan struct{}
		shutdownLock sync.Mutex
	}

	Config struct {
		DataDir        string
		HTTPBindAddr   string
		RPCBindAddr    string
		SerfBindAddr   string
		NodeName       string
		StartJoinAddrs []string
		ACLModelFile   string
		ACLPolicyFile  string
		DebugPort      int
	}
)

func New(config Config, logger *zap.Logger) (*Agent, error) {
	a := &Agent{
		Config:    config,
		shutdowns: make(chan struct{}),
		logger:    logger,
	}

	setup := []func() error{
		a.setupLogger,
		a.setupLog,
		a.setupServer,
		a.setupDebugServer,
		a.setupMembership,
		a.setupDebugServer,
	}

	for _, fn := range setup {
		if err := fn(); err != nil {
			return nil, err
		}
	}

	return a, nil
}

func (a *Agent) setupLogger() error {
	logger, err := zap.NewDevelopment()
	if err != nil {
		return err
	}

	zap.ReplaceGlobals(logger)

	return nil
}

func (a *Agent) setupLog() error {
	var err error

	a.log, err = log.NewLog(a.Config.DataDir, log.Config{})

	return err
}

func (a *Agent) setupServer() error {
	serverConfig := &server.Config{
		CommitLog: a.log,
	}

	if err := a.setupGRPC(serverConfig); err != nil {
		return err
	}

	return a.setupHTTP(serverConfig)
}

func (a *Agent) setupHTTP(serverConfig *server.Config) error {
	var err error
	a.httpServer, err = server.NewHTTPServer(a.HTTPBindAddr, serverConfig)
	if err != nil {
		return err
	}

	go func() {
		if err := a.httpServer.ListenAndServe(); err != nil {
			withTimeoutCtx, cancel := context.WithTimeout(context.Background(), time.Second*5)
			defer cancel()
			_ = a.httpServer.Shutdown(withTimeoutCtx)
		}
	}()

	return nil
}

func (a *Agent) setupGRPC(serverConfig *server.Config) error {
	var err error
	a.grpcServer, err = server.NewGRPCServer(serverConfig)
	if err != nil {
		return err
	}

	ln, err := net.Listen("tcp", a.RPCBindAddr)
	if err != nil {
		return err
	}

	go func() {
		if err := a.grpcServer.Serve(ln); err != nil {
			_ = a.Shutdown()
		}
	}()

	return nil
}

func (a *Agent) setupDebugServer() error {
	a.debugServer = server.NewDebugServer(a.DebugPort, a.logger)
	a.debugServer.Run()
	return nil
}

func (a *Agent) setupMembership() error {
	// TODO(threadedstream): add support for secure communication in future
	conn, err := grpc.Dial(a.RPCBindAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}

	client := api.NewLogClient(conn)
	a.replicator = &log.Replicator{
		LocalServer: client,
	}

	a.membership, err = discovery.New(a.replicator, &discovery.Config{
		NodeName: a.NodeName,
		BindAddr: a.SerfBindAddr,
		Tags: map[string]string{
			discovery.RpcAddrTagKey: a.RPCBindAddr,
		},
		StartJoinAddrs: a.StartJoinAddrs,
	})

	return err
}

func (a *Agent) Shutdown() error {
	a.shutdownLock.Lock()
	defer a.shutdownLock.Unlock()

	if a.shutdown {
		return nil
	}

	a.shutdown = true
	close(a.shutdowns)

	shutdowns := []func() error{
		a.membership.Leave,
		a.replicator.Close,
		func() error {
			a.grpcServer.GracefulStop()
			return nil
		},
		a.log.Close,
		a.debugServer.Shutdown,
	}

	for _, fn := range shutdowns {
		if err := fn(); err != nil {
			return err
		}
	}

	return nil
}
