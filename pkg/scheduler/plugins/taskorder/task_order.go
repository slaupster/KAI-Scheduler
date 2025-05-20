// Copyright 2025 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package taskorder

import (
	"strconv"

	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/api/pod_info"
	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/framework"
	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/plugins/taskorder/constants"
)

type taskOrderPlugin struct {
	// Arguments given for the plugin
	pluginArguments map[string]string

	// The label key to use for task ordering
	taskOrderLabelKey string
}

func New(arguments map[string]string) framework.Plugin {
	plugin := &taskOrderPlugin{
		pluginArguments:   arguments,
		taskOrderLabelKey: constants.DefaultTaskOrderLabelKey,
	}

	// Override with argument if provided
	if labelKey, exists := arguments[constants.TaskOrderLabelKeyArg]; exists && labelKey != "" {
		plugin.taskOrderLabelKey = labelKey
	}

	return plugin
}

func (pp *taskOrderPlugin) Name() string {
	return "taskorder"
}

func (pp *taskOrderPlugin) OnSessionOpen(ssn *framework.Session) {
	ssn.AddTaskOrderFn(func(l, r interface{}) int {
		return pp.taskOrderFn(l, r)
	})
}

func (pp *taskOrderPlugin) taskOrderFn(l, r interface{}) int {
	lv := l.(*pod_info.PodInfo)
	rv := r.(*pod_info.PodInfo)

	lPodPriorityString, lLabelExists := lv.Pod.Labels[pp.taskOrderLabelKey]
	rPodPriorityString, rLabelExists := rv.Pod.Labels[pp.taskOrderLabelKey]

	if lLabelExists && !rLabelExists {
		return -1
	}
	if !lLabelExists && rLabelExists {
		return 1
	}
	if !lLabelExists && !rLabelExists {
		return 0
	}

	lPodPriority, err := strconv.Atoi(lPodPriorityString)
	if err != nil {
		return 1
	}

	rPodPriority, err := strconv.Atoi(rPodPriorityString)
	if err != nil {
		return -1
	}

	if lPodPriority > rPodPriority {
		return -1
	}
	if lPodPriority < rPodPriority {
		return 1
	}

	return 0
}

func (pp *taskOrderPlugin) OnSessionClose(_ *framework.Session) {}
