package nomad

import (
	"fmt"
	"github.com/hashicorp/go-multierror"
	"github.com/hashicorp/nomad/client"
	"github.com/hashicorp/nomad/helper"
	"github.com/hashicorp/nomad/nomad/mock"
	"github.com/hashicorp/nomad/nomad/structs"
	"github.com/hashicorp/nomad/testutil"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func spreadJob(jobID string) *structs.Job {
	noRestart := &structs.RestartPolicy{
		Attempts: 0,
		Interval: 5 * time.Second,
		Mode:     structs.RestartPolicyModeFail,
	}
	// Inject mock job
	job := mock.Job()
	job.ID = jobID
	job.Constraints = []*structs.Constraint{}
	job.TaskGroups[0].MaxClientDisconnect = helper.TimeToPtr(time.Second * 30)
	job.TaskGroups[0].Count = 2
	job.TaskGroups[0].Spreads = []*structs.Spread{
		{
			Attribute:    "${node.unique.id}",
			Weight:       50,
			SpreadTarget: []*structs.SpreadTarget{},
		},
	}
	job.TaskGroups[0].RestartPolicy = noRestart
	job.TaskGroups[0].Tasks[0].RestartPolicy = noRestart
	job.TaskGroups[0].Tasks[0].Driver = "mock_driver"
	job.TaskGroups[0].Tasks[0].Config = map[string]interface{}{
		"run_for": "60s",
	}
	job.TaskGroups[0].Constraints = []*structs.Constraint{}
	job.TaskGroups[0].Tasks[0].Constraints = []*structs.Constraint{}

	return job
}

type expectedClientState struct {
	T          *testing.T
	clientName string
	server     *Server
	client     *client.Client
	failed     int
	running    int
	pending    int
	stop       int
}

func (ecs *expectedClientState) asExpected() error {
	var err error
	var allocs []*structs.Allocation
	testutil.WaitForResult(func() (bool, error) {
		allocs, err = ecs.server.State().AllocsByNode(nil, ecs.client.NodeID())
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
		ecs.T.Logf("client %s: %s - %s - %s\n", ecs.clientName, alloc.Name, alloc.ClientStatus, alloc.DesiredStatus)
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
