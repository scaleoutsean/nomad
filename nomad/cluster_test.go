package nomad

import (
	"fmt"
	"testing"

	"github.com/hashicorp/go-multierror"
	"github.com/hashicorp/nomad/client"
	"github.com/hashicorp/nomad/client/config"
	"github.com/hashicorp/nomad/nomad/structs"
	"github.com/hashicorp/nomad/testutil"
	"github.com/stretchr/testify/require"
)

// TestCluster is a cluster of test serves and clients suitable for integration testing.
type TestCluster struct {
	T                   *testing.T
	rpcServer           *Server
	Servers             map[string]*Server
	Clients             map[string]*client.Client
	ExpectedAllocStates []*ClientAllocState
}

func NewTestCluster(t *testing.T, serverFn map[string]func(*Config), clientFn map[string]func(*config.Config), expectedAllocStates []*ClientAllocState) (*TestCluster, func() error, error) {
	if len(serverFn) == 0 || len(clientFn) == 0 {
		return nil, nil,
			fmt.Errorf("invalid test cluster: requires both servers and client - server count %d client count %d", len(serverFn), len(clientFn))
	}

	cluster := &TestCluster{
		Servers:             make(map[string]*Server, len(serverFn)),
		Clients:             make(map[string]*client.Client, len(clientFn)),
		ExpectedAllocStates: expectedAllocStates,
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

func (tc *TestCluster) WaitForNodeStatus(clientName, nodeStatus string) (err error) {
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
			clientReady = clientNode.Status == nodeStatus
		}
		return clientReady, nil
	}, func(waitErr error) {
		err = waitErr
	})

	if !clientReady {
		return fmt.Errorf("client %s failed to enter %s state", clientName, nodeStatus)
	}

	return
}

func (tc *TestCluster) LatestJobEvalForTrigger(job *structs.Job, triggerBy string) (*structs.Evaluation, error) {
	var outEval *structs.Evaluation
	testutil.WaitForResult(func() (bool, error) {
		evals, err := tc.rpcServer.State().EvalsByJob(nil, job.Namespace, job.ID)
		if err != nil {
			return false, err
		}
		for _, eval := range evals {
			if eval.TriggeredBy == triggerBy {
				outEval = eval
				break
			}
		}
		return outEval != nil, nil
	}, func(err error) {
		require.NoError(tc.T, err, "error retrieving evaluations %s", err)
	})

	if outEval == nil {
		return nil, fmt.Errorf("failed to find eval triggered by %s", triggerBy)
	}

	return outEval, nil
}

func (tc *TestCluster) AsExpected() error {
	var mErr *multierror.Error

	for _, clientState := range tc.ExpectedAllocStates {
		testClient, ok := tc.Clients[clientState.clientName]
		if !ok || testClient == nil {
			mErr = multierror.Append(mErr, fmt.Errorf("error validating client state: invalid client name %s", clientState.clientName))
			continue
		}

		if err := clientState.AsExpected(tc.rpcServer, testClient.NodeID()); err != nil {
			mErr = multierror.Append(mErr, fmt.Errorf("error validating client state for %s: \n\t%s", clientState.clientName, err))
		}
	}

	return mErr.ErrorOrNil()
}

type ClientAllocState struct {
	T          *testing.T
	clientName string
	failed     int
	running    int
	pending    int
	stop       int
}

func (ecs *ClientAllocState) AsExpected(server *Server, nodeID string) error {
	var err error
	var allocs []*structs.Allocation
	testutil.WaitForResult(func() (bool, error) {
		allocs, err = server.State().AllocsByNode(nil, nodeID)
		if err != nil {
			return false, err
		}
		if len(allocs) == 0 {
			return false, nil
		}
		return true, nil
	}, func(err error) {
		require.NoError(ecs.T, err, "error retrieving allocs for %s - %s", ecs.clientName, err)
	})

	require.NotEqual(ecs.T, 0, len(allocs))

	failed := 0
	running := 0
	pending := 0
	stop := 0

	for _, alloc := range allocs {
		switch alloc.ClientStatus {
		case structs.AllocClientStatusFailed:
			failed++
		case structs.AllocClientStatusRunning:
			running++
		case structs.AllocClientStatusPending:
			pending++
		}
		if alloc.DesiredStatus == structs.AllocDesiredStatusStop {
			stop++
		}
	}

	var mErr *multierror.Error

	if failed != ecs.failed {
		mErr = multierror.Append(mErr, fmt.Errorf("expected %d failed on %s found %d", ecs.failed, ecs.clientName, failed))
	}
	if running != ecs.running {
		mErr = multierror.Append(mErr, fmt.Errorf("expected %d running on %s found %d", ecs.running, ecs.clientName, running))
	}
	if pending != ecs.pending {
		mErr = multierror.Append(mErr, fmt.Errorf("expected %d pending on %s found %d", ecs.pending, ecs.clientName, pending))
	}
	if stop != ecs.stop {
		mErr = multierror.Append(mErr, fmt.Errorf("expected %d stop on %s found %d", ecs.stop, ecs.clientName, stop))
	}

	return mErr.ErrorOrNil()
}
