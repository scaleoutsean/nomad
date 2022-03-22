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
	cfg    *TestClusterConfig
	Server *Server
	//rpcServer *Server
	//Servers   map[string]*Server
	Clients map[string]*client.Client
}

func (tc *TestCluster) PrintState(job *structs.Job) {
	fmt.Printf("state for job %s\n", job.ID)

	var err error
	var evals []*structs.Evaluation
	evals, err = tc.Server.State().EvalsByJob(nil, job.Name, job.ID)
	if err != nil {
		fmt.Printf("\terror getting evals: %s\n", err)
	} else {
		fmt.Println("\tevals:")
		for _, eval := range evals {
			fmt.Printf("\t\t%#v\n", eval)
		}
	}

	deployments, err := tc.Server.State().DeploymentsByJobID(nil, job.Namespace, job.ID, true)
	if err != nil {
		fmt.Printf("\terror getting deployments: %s\n", err)
	} else {
		fmt.Println("\tdeployments:")
		for _, deployment := range deployments {
			fmt.Printf("\t\t%#v\n", deployment)
		}
	}

	var allocs []*structs.Allocation
	allocs, err = tc.Server.State().AllocsByJob(nil, job.Namespace, job.ID, true)
	if err != nil {
		fmt.Printf("\terror getting allocs: %s\n", err)
		return
	}

	for name, testClient := range tc.Clients {
		fmt.Printf("\tclient allocs: %s\n", name)
		for _, alloc := range allocs {
			if alloc.NodeID != testClient.NodeID() {
				continue
			}
			fmt.Printf("\t\t%s: %s\n", alloc.Name, alloc.ClientStatus)
		}
		fmt.Println("")
	}
}

type TestClusterConfig struct {
	T                   *testing.T
	Trace               bool
	ServerFns           map[string]func(*Config)
	ClientFns           map[string]func(*config.Config)
	ExpectedAllocStates []*ExpectedAllocState
	ExpectedEvalStates  []*ExpectedEvalState
}

func NewTestCluster(cfg *TestClusterConfig) (*TestCluster, func() error, error) {
	if len(cfg.ServerFns) == 0 || len(cfg.ClientFns) == 0 {
		return nil, nil,
			fmt.Errorf("invalid test cluster: requires both servers and client - server count %d client count %d", len(cfg.ServerFns), len(cfg.ClientFns))
	}

	cluster := &TestCluster{
		//Servers: make(map[string]*Server, len(cfg.serverFn)),
		Clients: make(map[string]*client.Client, len(cfg.ClientFns)),
		cfg:     cfg,
	}

	serverCleanups := make(map[string]func(), len(cfg.ServerFns))
	for name, fn := range cfg.ServerFns {
		testServer, cleanup := TestServer(cfg.T, fn)
		cluster.Server = testServer
		serverCleanups[name] = cleanup
		if cluster.Server == nil {
			cluster.Server = testServer
		}
	}

	testutil.WaitForLeader(cfg.T, cluster.Server.RPC)

	clientCleanups := make(map[string]func() error, len(cfg.ClientFns))
	for name, fn := range cfg.ClientFns {
		configFn := func(c *config.Config) {
			c.RPCHandler = cluster.Server
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
	err := tc.Server.RPC("Job.Register", regReq, &regResp)
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
	err := client.FailHeartbeat(testClient)
	if err != nil {
		return err
	}
	return nil
	//return tc.Server.State().UpdateNodeStatus(structs.MsgTypeTestSetup, tc.index, testClient.NodeID(), structs.NodeStatusDisconnected, time.Now().UnixNano(), &structs.NodeEvent{Message: "down"})
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
		allocs, err = tc.Server.State().AllocsByJob(nil, job.Namespace, job.ID, true)
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
	var nodeErr error
	var outNode *structs.Node
	testutil.WaitForResult(func() (bool, error) {
		outNode, nodeErr = tc.Server.State().NodeByID(nil, testClient.Node().ID)
		if nodeErr != nil {
			return false, nodeErr
		}
		if outNode != nil {
			clientReady = outNode.Status == nodeStatus
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
		outAlloc, err = tc.Server.State().AllocByID(nil, allocID)
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

func (tc *TestCluster) WaitForJobEvalByTrigger(job *structs.Job, triggerBy string) (*structs.Evaluation, error) {
	var outEval *structs.Evaluation
	testutil.WaitForResult(func() (bool, error) {
		evals, err := tc.Server.State().EvalsByJob(nil, job.Namespace, job.ID)
		if err != nil {
			return false, err
		}
		for _, eval := range evals {
			if tc.cfg.Trace {
				fmt.Println("triggered by: " + eval.TriggeredBy)
			}
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

func (tc *TestCluster) WaitForAsExpected(job *structs.Job) error {
	var mErr *multierror.Error

	testutil.WaitForResult(func() (bool, error) {
		mErr = nil
		for _, clientState := range tc.cfg.ExpectedAllocStates {
			testClient, ok := tc.Clients[clientState.clientName]
			if !ok || testClient == nil {
				mErr = multierror.Append(mErr, fmt.Errorf("error validating client state: invalid client name %s", clientState.clientName))
				continue
			}

			if err := clientState.AsExpected(tc, testClient.NodeID()); err != nil {
				mErr = multierror.Append(mErr, fmt.Errorf("error validating client state for %s: \n\t%s", clientState.clientName, err))
			}
		}

		for _, evalState := range tc.cfg.ExpectedEvalStates {
			if err := evalState.AsExpected(tc, job); err != nil {
				mErr = multierror.Append(mErr, err)
			}
		}

		expErr := mErr.ErrorOrNil()
		if expErr != nil {
			return false, nil
		}
		return true, nil
	}, func(err error) {
		require.NoError(tc.cfg.T, err, "error retrieving evaluations %s", err)
	})

	return mErr.ErrorOrNil()
}

type ExpectedEvalState struct {
	TriggerBy string
	Count     int
}

func (es *ExpectedEvalState) AsExpected(tc *TestCluster, job *structs.Job) error {
	evals, err := tc.Server.State().EvalsByJob(nil, job.Namespace, job.ID)
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

type ExpectedAllocState struct {
	clientName string
	failed     int
	running    int
	pending    int
	stop       int
}

func (ecs *ExpectedAllocState) AsExpected(tc *TestCluster, nodeID string) error {
	var err error
	var allocs []*structs.Allocation
	testutil.WaitForResult(func() (bool, error) {
		allocs, err = tc.Server.State().AllocsByNode(nil, nodeID)
		if err != nil {
			return false, err
		}
		if len(allocs) == 0 {
			return false, nil
		}
		return true, nil
	}, func(err error) {
		require.NoError(tc.cfg.T, err, "error retrieving allocs for %s - %s", ecs.clientName, err)
	})

	require.NotEqual(tc.cfg.T, 0, len(allocs))

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
		case structs.AllocClientStatusComplete:
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
