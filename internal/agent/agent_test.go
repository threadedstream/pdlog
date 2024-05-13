package agent

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/go-dynaport"
	"github.com/vlamug/pdlog/api/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

var ports []int

func getAddr() string {
	return fmt.Sprintf("127.0.0.1:%d", dynaport.Get(1)[0])
}

func init() {
	ports = []int{9101, 9102, 9103, 9104, 9105, 9106}
}

func TestAgent(t *testing.T) {
	var agents []*Agent
	for i := 0; i < 3; i++ {
		bindAddr := getAddr()

		dataDir, err := os.MkdirTemp("", "agent-test-log")
		require.NoError(t, err)

		var startJoinAddrs []string
		if i != 0 {
			startJoinAddrs = append(startJoinAddrs, agents[0].RPCBindAddr)
		}

		logger := zap.NewNop()
		agent, err := New(Config{
			NodeName:       fmt.Sprintf("node_%d", i),
			StartJoinAddrs: startJoinAddrs,
			HTTPBindAddr:   getAddr(),
			RPCBindAddr:    bindAddr,
			DataDir:        dataDir,
			Bootstrap:      i == 0,
		}, logger)
		require.NoError(t, err)

		agents = append(agents, agent)
	}

	time.Sleep(3 * time.Second)

	leaderClient := client(t, agents[0])
	message := []byte("test_message")

	produceResponse, err := leaderClient.Produce(
		context.Background(),
		&api.ProduceRequest{Record: &api.Record{Value: message}},
	)
	require.NoError(t, err)

	consumeRequest, err := leaderClient.Consume(
		context.Background(),
		&api.ConsumeRequest{Offset: produceResponse.Offset},
	)
	require.NoError(t, err)
	require.Equal(t, consumeRequest.Record.Value, message)

	// wait until replication has finished
	time.Sleep(3 * time.Second)

	followerClient := client(t, agents[1])
	consumeRequest, err = followerClient.Consume(
		context.Background(),
		&api.ConsumeRequest{Offset: produceResponse.Offset},
	)
	require.NoError(t, err)
	require.Equal(t, consumeRequest.Record.Value, message)

	for _, agent := range agents {
		err := agent.Shutdown()
		require.NoError(t, err)
		require.NoError(t, os.RemoveAll(agent.Config.DataDir))
	}

	consumeResponse, err := leaderClient.Consume(
		context.Background(),
		&api.ConsumeRequest{Offset: produceResponse.Offset + 1},
	)
	require.Nil(t, consumeResponse)
	require.NoError(t, err)

	got := status.Code(err)
	want := status.Code(api.ErrOffsetOutOfRange{}.GRPCStatus().Err())
	require.Equal(t, got, want)
}

func client(t *testing.T, agent *Agent) api.LogClient {
	addr := agent.RPCBindAddr

	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)

	cl := api.NewLogClient(conn)

	return cl
}
