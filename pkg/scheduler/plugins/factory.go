// Copyright 2025 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package plugins

import (
	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/framework"
	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/plugins/dynamicresources"
	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/plugins/elastic"
	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/plugins/gpupack"
	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/plugins/gpusharingorder"
	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/plugins/gpuspread"
	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/plugins/kubeflow"
	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/plugins/minruntime"
	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/plugins/nodeavailability"
	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/plugins/nodeplacement"
	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/plugins/nominatednode"
	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/plugins/podaffinity"
	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/plugins/predicates"
	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/plugins/priority"
	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/plugins/proportion"
	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/plugins/ray"
	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/plugins/resourcetype"
	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/plugins/snapshot"
	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/plugins/taskorder"
)

func InitDefaultPlugins() {
	// Plugins for PodGroupInfos
	framework.RegisterPluginBuilder("predicates", predicates.New)
	framework.RegisterPluginBuilder("priority", priority.New)
	framework.RegisterPluginBuilder("nodeplacement", nodeplacement.New)
	framework.RegisterPluginBuilder("nominatednode", nominatednode.New)
	framework.RegisterPluginBuilder("nodeavailability", nodeavailability.New)
	framework.RegisterPluginBuilder("gpusharingorder", gpusharingorder.New)
	framework.RegisterPluginBuilder("gpupack", gpupack.New)
	framework.RegisterPluginBuilder("gpuspread", gpuspread.New)
	framework.RegisterPluginBuilder("resourcetype", resourcetype.New)
	framework.RegisterPluginBuilder("podaffinity", podaffinity.New)
	framework.RegisterPluginBuilder("elastic", elastic.New)
	framework.RegisterPluginBuilder("kubeflow", kubeflow.New)
	framework.RegisterPluginBuilder("ray", ray.New)
	framework.RegisterPluginBuilder("taskorder", taskorder.New)
	framework.RegisterPluginBuilder("dynamicresources", dynamicresources.New)

	// Plugins for Queues
	framework.RegisterPluginBuilder("proportion", proportion.New)
	framework.RegisterPluginBuilder("minruntime", minruntime.New)

	// Other Plugins
	framework.RegisterPluginBuilder("snapshot", snapshot.New)
}
