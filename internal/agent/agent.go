package agent

import (
	"bytes"
	"context"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/soheilhy/cmux"
	"github.com/vlamug/pdlog/internal/discovery"
	"github.com/vlamug/pdlog/internal/log"
	"github.com/vlamug/pdlog/internal/server"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// Agent runs on every service instance, setting up and connecting all the different components
type (
	Agent struct {
		Config

		mux         cmux.CMux
		log         *log.DistributedLog
		httpServer  *http.Server
		grpcServer  *grpc.Server
		debugServer *server.DebugServer
		membership  *discovery.Membership
		logger      *zap.Logger

		shutdown     bool
		shutdowns    chan struct{}
		shutdownLock sync.Mutex
	}

	Config struct {
		DataDir                        string
		HTTPBindAddr                   string
		RPCBindAddr                    string
		SerfBindAddr                   string
		NodeName                       string
		StartJoinAddrs                 []string
		ACLModelFile                   string
		ACLPolicyFile                  string
		ServerTLSConfig, PeerTLSConfig *tls.Config
		DebugPort                      int
		Bootstrap                      bool
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
		a.setupMux,
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

	go a.serve()

	return a, nil
}

func (a *Agent) setupMux() error {
	ln, err := net.Listen("tcp", a.Config.RPCBindAddr)
	if err != nil {
		return err
	}
	a.mux = cmux.New(ln)
	return nil
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
	raftLn := a.mux.Match(func(reader io.Reader) bool {
		b := make([]byte, 1)
		if _, err := reader.Read(b); err != nil {
			return false
		}
		return bytes.Compare(b, []byte{byte(log.RaftRPC)}) == 0
	})
	logConfig := log.Config{}
	logConfig.Raft.StreamLayer = log.NewStreamLayer(
		raftLn,
		a.Config.ServerTLSConfig,
		a.Config.PeerTLSConfig,
	)
	logConfig.Raft.LocalID = raft.ServerID(a.Config.NodeName)
	logConfig.Raft.Bootstrap = a.Config.Bootstrap

	var err error
	a.log, err = log.NewDistributedLog(
		a.Config.DataDir,
		logConfig,
	)
	if err != nil {
		return err
	}
	if a.Config.Bootstrap {
		err = a.log.WaitForLeader(3 * time.Second)
	}
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

	grpcLn := a.mux.Match(cmux.Any())

	go func() {
		if err := a.grpcServer.Serve(grpcLn); err != nil {
			_ = a.Shutdown()
		}
	}()

	return nil
}

func (a *Agent) serve() error {
	if err := a.mux.Serve(); err != nil {
		_ = a.Shutdown()
		return err
	}
	return nil
}

func (a *Agent) setupDebugServer() error {
	a.debugServer = server.NewDebugServer(a.DebugPort, a.logger)
	a.debugServer.Run()
	return nil
}

func (a *Agent) setupMembership() error {
	// TODO(threadedstream): add support for secure communication in future
	var err error
	a.membership, err = discovery.New(a.log, &discovery.Config{
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
