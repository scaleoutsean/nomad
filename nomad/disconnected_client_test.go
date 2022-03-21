package nomad

import (
	"time"

	"github.com/hashicorp/nomad/client/config"
	"github.com/hashicorp/nomad/helper"
	"github.com/hashicorp/nomad/nomad/mock"
	"github.com/hashicorp/nomad/nomad/structs"
	"testing"
)

func disconnectJob(jobID string) *structs.Job {
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

func reconnectFailedAllocTestConfig(t *testing.T) *TestClusterConfig {
	return &TestClusterConfig{
		T:     t,
		Trace: false,
		ServerFns: map[string]func(*Config){
			"server1": func(c *Config) {
				c.HeartbeatGrace = 500 * time.Millisecond
				c.MaxHeartbeatsPerSecond = 1
				c.MinHeartbeatTTL = 1
			},
		},
		ClientFns: map[string]func(*config.Config){
			"client1": func(c *config.Config) {
				c.DevMode = true
				c.Options = make(map[string]string)
				c.Options["test.alloc_failer.enabled"] = "true"
				c.Options["test.heartbeat_failer.enabled"] = "true"
			},
			"client2": func(c *config.Config) {
				c.DevMode = true
			},
		},
		ExpectedAllocStates: []*ClientAllocState{
			{
				clientName: "client1",
				failed:     1,
				pending:    0,
				running:    0,
				stop:       0,
			},
			{
				clientName: "client2",
				failed:     0,
				pending:    0,
				running:    2,
				stop:       0,
			},
		},
		ExpectedEvalStates: []*EvalState{
			{
				TriggerBy: structs.EvalTriggerReconnect,
				Count:     1,
			},
			{
				TriggerBy: structs.EvalTriggerMaxDisconnectTimeout,
				Count:     1,
			},
		},
	}
}

func reconnectRunningAllocTestConfig(t *testing.T) *TestClusterConfig {
	return &TestClusterConfig{
		T:     t,
		Trace: false,
		ServerFns: map[string]func(*Config){
			"server1": func(c *Config) {
				c.HeartbeatGrace = 500 * time.Millisecond
				c.MaxHeartbeatsPerSecond = 1
				c.MinHeartbeatTTL = 1
			},
		},
		ClientFns: map[string]func(*config.Config){
			"client1": func(c *config.Config) {
				c.DevMode = true
				c.Options = make(map[string]string)
				c.Options["test.heartbeat_failer.enabled"] = "true"
			},
			"client2": func(c *config.Config) {
				c.DevMode = true
			},
		},
		ExpectedAllocStates: []*ClientAllocState{
			{
				clientName: "client1",
				failed:     0,
				pending:    0,
				running:    1,
				stop:       0,
			},
			{
				clientName: "client2",
				failed:     0,
				pending:    0,
				running:    1,
				stop:       1,
			},
		},
		ExpectedEvalStates: []*EvalState{
			{
				TriggerBy: structs.EvalTriggerNodeUpdate,
				Count:     3,
			},
			{
				TriggerBy: structs.EvalTriggerReconnect,
				Count:     0,
			},
			{
				TriggerBy: structs.EvalTriggerMaxDisconnectTimeout,
				Count:     1,
			},
		},
	}
}

func reconnectPendingAllocTestConfig(t *testing.T) *TestClusterConfig {
	return &TestClusterConfig{
		T:     t,
		Trace: false,
		ServerFns: map[string]func(*Config){
			"server1": func(c *Config) {
				c.HeartbeatGrace = 500 * time.Millisecond
				c.MaxHeartbeatsPerSecond = 1
				c.MinHeartbeatTTL = 1
			},
		},
		ClientFns: map[string]func(*config.Config){
			"client1": func(c *config.Config) {
				c.DevMode = true
				c.Options = make(map[string]string)
				c.Options["test.heartbeat_failer.enabled"] = "true"
			},
			"client2": func(c *config.Config) {
				c.DevMode = true
			},
		},
		ExpectedAllocStates: []*ClientAllocState{
			{
				clientName: "client1",
				failed:     0,
				pending:    0,
				running:    0,
				stop:       1,
			},
			{
				clientName: "client2",
				failed:     0,
				pending:    0,
				running:    2,
				stop:       2,
			},
		},
		ExpectedEvalStates: []*EvalState{
			{
				TriggerBy: structs.EvalTriggerReconnect,
				Count:     1,
			},
			{
				TriggerBy: structs.EvalTriggerMaxDisconnectTimeout,
				Count:     1,
			},
		},
	}
}
