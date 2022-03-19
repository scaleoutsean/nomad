package nomad

import (
	"github.com/hashicorp/nomad/helper"
	"github.com/hashicorp/nomad/nomad/mock"
	"github.com/hashicorp/nomad/nomad/structs"
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
