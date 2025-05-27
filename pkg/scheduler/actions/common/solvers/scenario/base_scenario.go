// Copyright 2025 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package scenario

import (
	"strings"

	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/api"
	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/api/common_info"
	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/api/pod_info"
	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/api/podgroup_info"
	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/framework"
)

var _ api.ScenarioInfo = &BaseScenario{}

type BaseScenario struct {
	session *framework.Session

	preemptor             *podgroup_info.PodGroupInfo
	pendingTasks          []*pod_info.PodInfo
	potentialVictimsTasks []*pod_info.PodInfo
	recordedVictimsJobs   []*podgroup_info.PodGroupInfo
	recordedVictimsTasks  []*pod_info.PodInfo

	// Deprecated: Use preemptor instead
	pendingTasksAsJob *podgroup_info.PodGroupInfo

	victimsJobsTaskGroups map[common_info.PodGroupID][]*podgroup_info.PodGroupInfo
}

func NewBaseScenario(
	session *framework.Session, pendingTasksAsJob *podgroup_info.PodGroupInfo, victimsTasks []*pod_info.PodInfo,
	recordedVictimsJobs []*podgroup_info.PodGroupInfo,
) *BaseScenario {
	s := &BaseScenario{
		session:               session,
		preemptor:             session.PodGroupInfos[pendingTasksAsJob.UID],
		pendingTasks:          make([]*pod_info.PodInfo, 0),
		pendingTasksAsJob:     pendingTasksAsJob,
		potentialVictimsTasks: make([]*pod_info.PodInfo, 0),
		recordedVictimsJobs:   make([]*podgroup_info.PodGroupInfo, len(recordedVictimsJobs)),
		recordedVictimsTasks:  nil,
		victimsJobsTaskGroups: make(map[common_info.PodGroupID][]*podgroup_info.PodGroupInfo),
	}

	for _, task := range pendingTasksAsJob.PodInfos {
		s.pendingTasks = append(s.pendingTasks, task)
	}
	for _, task := range victimsTasks {
		s.AddPotentialVictimsTasks([]*pod_info.PodInfo{task})
	}
	for index, recordedVictimJob := range recordedVictimsJobs {
		s.recordedVictimsJobs[index] = recordedVictimJob
		var tasks []*pod_info.PodInfo
		for _, podInfo := range recordedVictimJob.PodInfos {
			tasks = append(tasks, podInfo)
		}
		s.appendTasksAsVictimJob(tasks)
	}
	s.recordedVictimsTasks = s.RecordedVictimsTasks()

	return s
}

// Deprecated: Use GetPreemptor instead
func (s *BaseScenario) PendingTasks() []*pod_info.PodInfo {
	return s.pendingTasks
}

// Deprecated: Use GetPreemptor instead
func (s *BaseScenario) PendingJob() *podgroup_info.PodGroupInfo {
	return s.pendingTasksAsJob
}

func (s *BaseScenario) RecordedVictimsTasks() []*pod_info.PodInfo {
	if s.recordedVictimsTasks != nil {
		return s.recordedVictimsTasks
	}

	var recordedVictimsTasks []*pod_info.PodInfo
	for _, victimJob := range s.recordedVictimsJobs {
		for _, podInfo := range victimJob.PodInfos {
			recordedVictimsTasks = append(recordedVictimsTasks, podInfo)
		}
	}
	return recordedVictimsTasks
}

func (s *BaseScenario) RecordedVictimsJobs() []*podgroup_info.PodGroupInfo {
	return s.recordedVictimsJobs
}

func (s *BaseScenario) LatestPotentialVictim() *podgroup_info.PodGroupInfo {
	if len(s.potentialVictimsTasks) > 0 {
		return s.getJobForTask(s.potentialVictimsTasks[len(s.potentialVictimsTasks)-1])
	} else {
		return nil
	}
}

func (s *BaseScenario) PotentialVictimsTasks() []*pod_info.PodInfo {
	return s.potentialVictimsTasks
}

func (s *BaseScenario) AddPotentialVictimsTasks(tasks []*pod_info.PodInfo) {
	if len(tasks) == 0 {
		return
	}

	s.potentialVictimsTasks = append(s.potentialVictimsTasks, tasks...)
	s.appendTasksAsVictimJob(tasks)
}

func (s *BaseScenario) appendTasksAsVictimJob(tasks []*pod_info.PodInfo) {
	originalJob := s.getJobForTask(tasks[0])
	job := originalJob.CloneWithTasks(tasks)

	s.victimsJobsTaskGroups[job.UID] = append(s.victimsJobsTaskGroups[job.UID], job)
}

func (s *BaseScenario) GetVictimJobRepresentativeById(victimPodInfo *pod_info.PodInfo) *podgroup_info.PodGroupInfo {
	jobsWithMatchingId := s.victimsJobsTaskGroups[victimPodInfo.Job]
	for _, jobRepresentative := range jobsWithMatchingId {
		for _, podFromRepresentative := range jobRepresentative.PodInfos {
			if victimPodInfo.UID == podFromRepresentative.UID {
				return jobRepresentative
			}
		}
	}
	return nil
}

func (s *BaseScenario) String() string {
	stringBuilder := strings.Builder{}
	stringBuilder.WriteString("Pending tasks:")
	for _, task := range s.pendingTasks {
		stringBuilder.WriteString(task.Namespace)
		stringBuilder.WriteString("/")
		stringBuilder.WriteString(task.Name)
		stringBuilder.WriteString(" ")
	}

	stringBuilder.WriteString("\t Recorded victim jobs:")
	for _, victim := range s.recordedVictimsJobs {
		stringBuilder.WriteString(victim.Namespace)
		stringBuilder.WriteString("/")
		stringBuilder.WriteString(victim.Name)
		stringBuilder.WriteString(" ")
	}

	stringBuilder.WriteString("\t Potential victim tasks:")
	for _, victim := range s.potentialVictimsTasks {
		stringBuilder.WriteString(victim.Namespace)
		stringBuilder.WriteString("/")
		stringBuilder.WriteString(victim.Name)
		stringBuilder.WriteString(" ")
	}

	return stringBuilder.String()
}

func (s *BaseScenario) getJobForTask(task *pod_info.PodInfo) *podgroup_info.PodGroupInfo {
	return s.session.PodGroupInfos[task.Job]
}

func (s *BaseScenario) GetPreemptor() *podgroup_info.PodGroupInfo {
	return s.preemptor
}

func (s *BaseScenario) GetVictims() map[common_info.PodGroupID]*api.VictimInfo {
	victims := make(map[common_info.PodGroupID]*api.VictimInfo)

	// Process recorded victims
	for _, victimJob := range s.recordedVictimsJobs {
		var tasks []*pod_info.PodInfo
		for _, podInfo := range victimJob.PodInfos {
			tasks = append(tasks, podInfo)
		}
		victimInfo := &api.VictimInfo{
			Job:               victimJob,
			RepresentativeJob: s.GetVictimJobRepresentativeById(tasks[0]),
			Tasks:             tasks,
		}
		victims[victimJob.UID] = victimInfo
	}

	// Process potential victims
	for _, victimTask := range s.potentialVictimsTasks {
		if _, exists := victims[victimTask.Job]; !exists {
			job := s.getJobForTask(victimTask)
			victimInfo := &api.VictimInfo{
				Job:               job,
				RepresentativeJob: s.GetVictimJobRepresentativeById(victimTask),
				Tasks:             []*pod_info.PodInfo{victimTask},
			}
			victims[victimTask.Job] = victimInfo
		}
	}

	return victims
}
