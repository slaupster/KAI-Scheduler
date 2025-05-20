// Copyright 2025 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package taskorder

import (
	"testing"

	"gotest.tools/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/api/pod_info"
)

func TestTaskOrder(t *testing.T) {
	plugin := &taskOrderPlugin{
		taskOrderLabelKey: DefaultTaskOrderLabelKey,
	}

	lPod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				DefaultTaskOrderLabelKey: "1",
			},
		},
	}

	rPod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				DefaultTaskOrderLabelKey: "2",
			},
		},
	}

	assert.Equal(t, plugin.taskOrderFn(pod_info.NewTaskInfo(lPod), pod_info.NewTaskInfo(rPod)), 1)

	lPod.Labels[DefaultTaskOrderLabelKey] = "2"
	assert.Equal(t, plugin.taskOrderFn(pod_info.NewTaskInfo(lPod), pod_info.NewTaskInfo(rPod)), 0)

	rPod.Labels[DefaultTaskOrderLabelKey] = "1"
	assert.Equal(t, plugin.taskOrderFn(pod_info.NewTaskInfo(lPod), pod_info.NewTaskInfo(rPod)), -1)

	lPod.Labels = map[string]string{}
	assert.Equal(t, plugin.taskOrderFn(pod_info.NewTaskInfo(lPod), pod_info.NewTaskInfo(rPod)), 1)

	lPod.Labels = rPod.Labels
	rPod.Labels = map[string]string{}
	assert.Equal(t, plugin.taskOrderFn(pod_info.NewTaskInfo(lPod), pod_info.NewTaskInfo(rPod)), -1)

	lPod.Labels = map[string]string{}
	assert.Equal(t, plugin.taskOrderFn(pod_info.NewTaskInfo(lPod), pod_info.NewTaskInfo(rPod)), 0)

	// Test with custom label key
	customPlugin := &taskOrderPlugin{
		taskOrderLabelKey: "custom-priority",
	}
	lPod.Labels = map[string]string{"custom-priority": "3"}
	rPod.Labels = map[string]string{"custom-priority": "2"}
	assert.Equal(t, customPlugin.taskOrderFn(pod_info.NewTaskInfo(lPod), pod_info.NewTaskInfo(rPod)), -1)
}
