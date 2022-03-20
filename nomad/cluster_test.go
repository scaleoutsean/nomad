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
	cfg       *TestClusterConfig
	rpcServer *Server
	Servers   map[string]*Server
	Clients   map[string]*client.Client
}

type TestClusterConfig struct {
	T                   *testing.T
	serverFn            map[string]func(*Config)
	clientFn            map[string]func(*config.Config)
	expectedAllocStates []*ClientAllocState
	expectedEvalStates  []*EvalState
}

func NewTestCluster(cfg *TestClusterConfig) (*TestCluster, func() error, error) {
	if len(cfg.serverFn) == 0 || len(cfg.clientFn) == 0 {
		return nil, nil,
			fmt.Errorf("invalid test cluster: requires both servers and client - server count %d client count %d", len(cfg.serverFn), len(cfg.clientFn))
	}

	cluster := &TestCluster{
		Servers: make(map[string]*Server, len(cfg.serverFn)),
		Clients: make(map[string]*client.Client, len(cfg.clientFn)),
		cfg:     cfg,
	}

	serverCleanups := make(map[string]func(), len(cfg.serverFn))
	for name, fn := range cfg.serverFn {
		testServer, cleanup := TestServer(cfg.T, fn)
		cluster.Servers[name] = testServer
		serverCleanups[name] = cleanup
		if cluster.rpcServer == nil {
			cluster.rpcServer = testServer
		}
	}

	testutil.WaitForLeader(cfg.T, cluster.rpcServer.RPC)

	clientCleanups := make(map[string]func() error, len(cfg.clientFn))
	for name, fn := range cfg.clientFn {
		configFn := func(c *config.Config) {
			c.RPCHandler = cluster.rpcServer
			fn(c)
		}
		testClient, cleanup := client.TestClient(cfg.T, configFn)

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

func (tc *TestCluster) RegisterJob(job *structs.Job) error {
	regReq := &structs.JobRegisterRequest{
		Job:          job,
		WriteRequest: structs.WriteRequest{Region: "global"},
	}

	var regResp structs.JobRegisterResponse
	err := tc.rpcServer.RPC("Job.Register", regReq, &regResp)
	if err != nil {
		return err
	}

	if regResp.Index == 0 {
		return fmt.Errorf("error registering job: index is 0")
	}

	return nil
}

func (tc *TestCluster) NodeID(clientName string) (string, bool) {
	testClient, ok := tc.Clients[clientName]
	if !ok {
		return "", ok
	}
	return testClient.NodeID(), ok
}

func (tc *TestCluster) FailHeartbeat(clientName string) error {
	testClient, ok := tc.Clients[clientName]
	if !ok {
		return fmt.Errorf("error failing hearbeat: client %s not found", clientName)
	}
	return client.FailHeartbeat(testClient)
}

func (tc *TestCluster) ResumeHeartbeat(clientName string) error {
	testClient, ok := tc.Clients[clientName]
	if !ok {
		return fmt.Errorf("error resuming hearbeat: client %s not found", clientName)
	}
	return client.ResumeHeartbeat(testClient)
}

func (tc *TestCluster) FailTask(clientName, allocID, taskName, taskEvent string) error {
	testClient, ok := tc.Clients[clientName]
	if !ok {
		return fmt.Errorf("error failing hearbeat: client %s not found", clientName)
	}
	return client.FailTask(testClient, allocID, taskName, taskEvent)
}

func (tc *TestCluster) WaitForJobAllocsRunning(job *structs.Job, expectedAllocCount int) ([]*structs.Allocation, error) {
	var err error
	var allocs []*structs.Allocation
	testutil.WaitForResult(func() (bool, error) {
		allocs, err = tc.rpcServer.State().AllocsByJob(nil, job.Namespace, job.ID, true)
		if err != nil {
			return false, err
		}

		if len(allocs) != expectedAllocCount {
			return false, nil
		}

		for _, alloc := range allocs {
			if alloc.ClientStatus != structs.AllocClientStatusRunning {
				return false, nil
			}
		}
		return true, nil
	}, func(err error) {
		require.NoError(tc.cfg.T, err, "error retrieving allocs %s", err)
	})

	var mErr *multierror.Error
	for _, alloc := range allocs {
		if alloc.ClientStatus != structs.AllocClientStatusRunning {
			mErr = multierror.Append(mErr, fmt.Errorf("error retrieving allocs: expected status running but alloc %s has status %s", alloc.Name, alloc.ClientStatus))
		}
	}

	return allocs, mErr.ErrorOrNil()
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

func (tc *TestCluster) WaitForAllocClientStatusOnClient(clientName, allocID, clientStatus string) error {
	testClient, ok := tc.Clients[clientName]
	if !ok || testClient == nil {
		return fmt.Errorf("error: client %s not found", clientName)
	}

	var err error
	var outAlloc *structs.Allocation
	testutil.WaitForResult(func() (bool, error) {
		outAlloc, err = testClient.GetAlloc(allocID)
		if err != nil {
			return false, err
		}
		if outAlloc != nil && outAlloc.ClientStatus == clientStatus {
			return true, nil
		}
		return false, nil
	}, func(err error) {
		require.NoError(tc.cfg.T, err, "error retrieving alloc %s", err)
	})

	if outAlloc == nil {
		return fmt.Errorf("expected alloc on client %s with id %s to not be nil", clientName, allocID)
	}

	if outAlloc.ClientStatus != clientStatus {
		return fmt.Errorf("expected alloc on client %s with id %s to have status %s but had %s", clientName, allocID, clientStatus, outAlloc.ClientStatus)
	}

	return nil
}

func (tc *TestCluster) WaitForAllocClientStatusOnServer(allocID, clientStatus string) error {
	var err error
	var outAlloc *structs.Allocation
	testutil.WaitForResult(func() (bool, error) {
		outAlloc, err = tc.rpcServer.State().AllocByID(nil, allocID)
		if err != nil {
			return false, err
		}
		if outAlloc != nil && outAlloc.ClientStatus == clientStatus {
			return true, nil
		}
		return false, nil
	}, func(err error) {
		require.NoError(tc.cfg.T, err, "error retrieving alloc %s", err)
	})

	if outAlloc == nil {
		return fmt.Errorf("expected alloc at server with id %s to not be nil", allocID)
	}

	if outAlloc.ClientStatus != clientStatus {
		return fmt.Errorf("expected alloc at server with id %s to have status %s but had %s", allocID, clientStatus, outAlloc.ClientStatus)
	}

	return nil
}

func (tc *TestCluster) LatestJobEvalForTrigger(job *structs.Job, triggerBy string) (*structs.Evaluation, error) {
	var outEval *structs.Evaluation
	testutil.WaitForResult(func() (bool, error) {
		evals, err := tc.rpcServer.State().EvalsByJob(nil, job.Namespace, job.ID)
		if err != nil {
			return false, err
		}
		for _, eval := range evals {
			tc.cfg.T.Logf("found eval with triggered by %s", eval.TriggeredBy)

			if eval.TriggeredBy == triggerBy {
				outEval = eval
				break
			}
		}
		if outEval == nil {
			return false, nil
		}
		return true, nil
	}, func(err error) {
		require.NoError(tc.cfg.T, err, "error retrieving evaluations %s", err)
	})

	if outEval == nil {
		return nil, fmt.Errorf("failed to find eval triggered by %s", triggerBy)
	}

	return outEval, nil
}

func (tc *TestCluster) AsExpected(job *structs.Job) error {
	var mErr *multierror.Error

	for _, clientState := range tc.cfg.expectedAllocStates {
		testClient, ok := tc.Clients[clientState.clientName]
		if !ok || testClient == nil {
			mErr = multierror.Append(mErr, fmt.Errorf("error validating client state: invalid client name %s", clientState.clientName))
			continue
		}

		if err := clientState.AsExpected(tc.cfg.T, tc.rpcServer, testClient.NodeID()); err != nil {
			mErr = multierror.Append(mErr, fmt.Errorf("error validating client state for %s: \n\t%s", clientState.clientName, err))
		}
	}

	for _, evalState := range tc.cfg.expectedEvalStates {
		if err := evalState.AsExpected(tc.rpcServer, job); err != nil {
			mErr = multierror.Append(mErr, err)
		}
	}

	return mErr.ErrorOrNil()
}

type EvalState struct {
	TriggerBy string
	Count     int
}

func (es *EvalState) AsExpected(server *Server, job *structs.Job) error {
	evals, err := server.State().EvalsByJob(nil, job.Namespace, job.ID)
	if err != nil {
		return err
	}
	count := 0
	for _, eval := range evals {
		if eval.TriggeredBy == es.TriggerBy {
			count++
		}
	}

	if count != es.Count {
		return fmt.Errorf("error validating eval state: expected %d eval(s) found %d", es.Count, count)
	}

	return nil
}

type ClientAllocState struct {
	clientName string
	failed     int
	running    int
	pending    int
	stop       int
}

func (ecs *ClientAllocState) AsExpected(t *testing.T, server *Server, nodeID string) error {
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
		require.NoError(t, err, "error retrieving allocs for %s - %s", ecs.clientName, err)
	})

	require.NotEqual(t, 0, len(allocs))

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
