package nomad

import (
	"fmt"
	"testing"

	"github.com/hashicorp/go-multierror"
	"github.com/hashicorp/nomad/client"
	"github.com/hashicorp/nomad/client/config"
	"github.com/hashicorp/nomad/nomad/structs"
	"github.com/hashicorp/nomad/testutil"
)

// TestCluster is a cluster of test serves and clients suitable for integration testing.
type TestCluster struct {
	T         *testing.T
	rpcServer *Server
	Servers   map[string]*Server
	Clients   map[string]*client.Client
}

func NewTestCluster(t *testing.T, serverFn map[string]func(*Config), clientFn map[string]func(*config.Config)) (*TestCluster, func() error, error) {
	if len(serverFn) == 0 || len(clientFn) == 0 {
		return nil, nil,
			fmt.Errorf("invalid test cluster: requires both servers and client - server count %d client count %d", len(serverFn), len(clientFn))
	}

	cluster := &TestCluster{
		Servers: make(map[string]*Server, len(serverFn)),
		Clients: make(map[string]*client.Client, len(clientFn)),
	}

	serverCleanups := make(map[string]func(), len(serverFn))
	for name, fn := range serverFn {
		testServer, cleanup := TestServer(t, fn)
		cluster.Servers[name] = testServer
		serverCleanups[name] = cleanup
		if cluster.rpcServer == nil {
			cluster.rpcServer = testServer
		}
	}

	testutil.WaitForLeader(t, cluster.rpcServer.RPC)

	clientCleanups := make(map[string]func() error, len(clientFn))
	for name, fn := range clientFn {
		configFn := func(c *config.Config) {
			c.RPCHandler = cluster.rpcServer
			fn(c)
		}
		testClient, cleanup := client.TestClient(t, configFn)

		cluster.Clients[name] = testClient
		clientCleanups[name] = cleanup
	}

	deferFn := func() error {
		for _, serverCleanup := range serverCleanups {
			serverCleanup()
		}

		var mErr *multierror.Error

		for _, clientCleanup := range clientCleanups {
			err := clientCleanup()
			if err != nil {
				mErr = multierror.Append(mErr, err)
			}
		}

		return mErr.ErrorOrNil()
	}

	return cluster, deferFn, nil
}

func (tc *TestCluster) WaitForReady(clientName string) (err error) {
	testClient, ok := tc.Clients[clientName]
	if !ok || testClient == nil {
		return fmt.Errorf("error: client %s not found", clientName)
	}

	clientReady := false
	testutil.WaitForResult(func() (bool, error) {
		clientNode, nodeErr := tc.rpcServer.State().NodeByID(nil, testClient.Node().ID)
		if nodeErr != nil {
			return false, nodeErr
		}
		if clientNode != nil {
			clientReady = clientNode.Status == structs.NodeStatusReady
		}
		return clientReady, nil
	}, func(waitErr error) {
		err = waitErr
	})

	if !clientReady {
		return fmt.Errorf("client %s failed to enter ready state", clientName)
	}

	return
}
