/*
Copyright 2025 NVIDIA CORPORATION
SPDX-License-Identifier: Apache-2.0
*/
package predicates

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"golang.org/x/exp/maps"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	runtimeClient "sigs.k8s.io/controller-runtime/pkg/client"

	v2 "github.com/NVIDIA/KAI-scheduler/pkg/apis/scheduling/v2"
	"github.com/NVIDIA/KAI-scheduler/pkg/common/constants"
	"github.com/NVIDIA/KAI-scheduler/test/e2e/modules/constant"
	testcontext "github.com/NVIDIA/KAI-scheduler/test/e2e/modules/context"
	"github.com/NVIDIA/KAI-scheduler/test/e2e/modules/resources/capacity"
	"github.com/NVIDIA/KAI-scheduler/test/e2e/modules/resources/rd"
	"github.com/NVIDIA/KAI-scheduler/test/e2e/modules/resources/rd/pod_group"
	"github.com/NVIDIA/KAI-scheduler/test/e2e/modules/resources/rd/queue"
	"github.com/NVIDIA/KAI-scheduler/test/e2e/modules/utils"
	"github.com/NVIDIA/KAI-scheduler/test/e2e/modules/wait"
)

const (
	podAffinityLabelName  = "pod-affinity-test"
	podAffinityLabelValue = "true"
)

var _ = Describe("Affinity", Ordered, func() {
	var (
		testCtx   *testcontext.TestContext
		namespace string
	)

	BeforeAll(func(ctx context.Context) {
		testCtx = testcontext.GetConnectivity(ctx, Default)
		parentQueue := queue.CreateQueueObject(utils.GenerateRandomK8sName(10), "")
		childQueue := queue.CreateQueueObject(utils.GenerateRandomK8sName(10), parentQueue.Name)
		testCtx.InitQueues([]*v2.Queue{childQueue, parentQueue})
		namespace = queue.GetConnectedNamespaceToQueue(testCtx.Queues[0])
	})

	AfterAll(func(ctx context.Context) {
		testCtx.ClusterCleanup(ctx)
	})

	AfterEach(func(ctx context.Context) {
		testCtx.TestContextCleanup(ctx)
	})

	Context("Node Affinity", func() {
		var (
			nodeWithGPU string
		)

		BeforeAll(func(ctx context.Context) {
			capacity.SkipIfInsufficientClusterTopologyResources(testCtx.KubeClientset, []capacity.ResourceList{
				{
					Gpu:      resource.MustParse("1"),
					PodCount: 1,
				},
			})
			pod := submitWaitAndGetPod(ctx, testCtx, testCtx.Queues[0], nil, map[string]string{})
			nodeWithGPU = pod.Spec.NodeName
			err := testCtx.ControllerClient.Delete(ctx, pod)
			Expect(err).To(Succeed())
		})

		It("schedules pod to node with correct affinity", func(ctx context.Context) {
			pod := submitWaitAndGetPod(
				ctx, testCtx, testCtx.Queues[0], rd.NodeAffinity(nodeWithGPU, v1.NodeSelectorOpIn), map[string]string{},
			)
			Expect(pod.Spec.NodeName).To(Equal(nodeWithGPU))
		})

		It("doesn't schedule pod to nodes without requested affinity", func(ctx context.Context) {
			cpuNode := rd.FindNodeWithNoGPU(ctx, testCtx.ControllerClient)
			if cpuNode == nil {
				Skip("Failed to find node without GPUs")
			}
			pod := submitPod(ctx, testCtx.KubeClientset,
				testCtx.Queues[0], rd.NodeAffinity(cpuNode.Name, v1.NodeSelectorOpIn), map[string]string{},
			)
			wait.ForPodUnschedulable(ctx, testCtx.ControllerClient, pod)
		})
	})

	Context("Pod Affinity", func() {
		It("Not scheduling if pod affinity condition is not met", func(ctx context.Context) {
			capacity.SkipIfInsufficientClusterTopologyResources(testCtx.KubeClientset, []capacity.ResourceList{
				{
					Gpu:      resource.MustParse("1"),
					PodCount: 1,
				},
				{
					Gpu:      resource.MustParse("1"),
					PodCount: 1,
				},
			})
			podAffinityLabels := map[string]string{podAffinityLabelName: podAffinityLabelValue}
			podAffinity := &v1.Affinity{
				PodAffinity: &v1.PodAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
						{
							LabelSelector: &metav1.LabelSelector{
								MatchLabels: podAffinityLabels,
							},
							Namespaces:  []string{namespace},
							TopologyKey: constant.NodeNamePodLabelName,
						},
					},
				},
			}
			pod1 := submitWaitAndGetPod(
				ctx, testCtx, testCtx.Queues[0], podAffinity, podAffinityLabels)

			podAffinityToPod1WithNodeAffinityToDifferentNode := &v1.Affinity{
				PodAffinity:  podAffinity.PodAffinity,
				NodeAffinity: rd.NodeAffinity(pod1.Spec.NodeName, v1.NodeSelectorOpNotIn).NodeAffinity,
			}
			pod2 := submitPod(ctx, testCtx.KubeClientset, testCtx.Queues[0],
				podAffinityToPod1WithNodeAffinityToDifferentNode, podAffinityLabels)

			wait.ForPodUnschedulable(ctx, testCtx.ControllerClient, pod2)
		})

		Context("distributed jobs", func() {
			const testGroupLabel = "test-group"
			var (
				gpuNodes   []*v1.Node = []*v1.Node{}
				fillerPods []*v1.Pod  = []*v1.Pod{}
			)
			BeforeEach(func(ctx context.Context) {
				capacity.SkipIfNonHomogeneousGpuCounts(testCtx.KubeClientset)

				resourceList := []capacity.ResourceList{}
				for range 4 {
					resourceList = append(
						resourceList,
						capacity.ResourceList{
							Gpu:      resource.MustParse("1"),
							PodCount: 1,
						},
					)
				}
				capacity.SkipIfInsufficientClusterTopologyResources(testCtx.KubeClientset, resourceList)

				nodes, err := testCtx.KubeClientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{
					LabelSelector: constants.GpuCountLabel,
				})
				Expect(err).NotTo(HaveOccurred())

				for _, node := range nodes.Items {
					if _, hasGPU := node.Status.Capacity[constants.GpuResource]; hasGPU {
						gpuNodes = append(gpuNodes, &node)
					}
				}
				if len(gpuNodes) < 4 {
					Skip("Need at least 4 GPU nodes for this test")
				}

				Expect(rd.LabelNode(ctx, testCtx.KubeClientset, gpuNodes[0], testGroupLabel, "a")).To(Succeed())
				Expect(rd.LabelNode(ctx, testCtx.KubeClientset, gpuNodes[1], testGroupLabel, "a")).To(Succeed())
				Expect(rd.LabelNode(ctx, testCtx.KubeClientset, gpuNodes[2], testGroupLabel, "b")).To(Succeed())
				Expect(rd.LabelNode(ctx, testCtx.KubeClientset, gpuNodes[3], testGroupLabel, "b")).To(Succeed())

				targetNode := gpuNodes[1]
				nodeAffinity := rd.NodeAffinity(targetNode.Name, v1.NodeSelectorOpIn)
				pod1 := submitWaitAndGetPod(ctx, testCtx, testCtx.Queues[0], nodeAffinity, nil)
				Expect(pod1.Spec.NodeName).To(Equal(targetNode.Name))
				fillerPods = append(fillerPods, pod1)
			})

			AfterEach(func(ctx context.Context) {
				for _, node := range gpuNodes {
					Expect(rd.UnLabelNode(ctx, testCtx.KubeClientset, node, testGroupLabel)).To(Succeed())
				}
				for _, pod := range fillerPods {
					Expect(testCtx.ControllerClient.Delete(ctx, pod, &runtimeClient.DeleteOptions{})).To(Succeed())
				}
			})

			FIt("try multiple node groups", func(ctx context.Context) {

				podAffinity := &v1.Affinity{
					PodAffinity: &v1.PodAffinity{
						RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
							{
								LabelSelector: &metav1.LabelSelector{
									MatchExpressions: []metav1.LabelSelectorRequirement{
										{
											Key:      testGroupLabel,
											Operator: metav1.LabelSelectorOpExists,
										},
									},
								},
								Namespaces:  []string{namespace},
								TopologyKey: testGroupLabel,
							},
						},
					},
				}

				requirements := v1.ResourceRequirements{
					Limits: map[v1.ResourceName]resource.Quantity{
						constants.GpuResource: gpuNodes[1].Status.Capacity[constants.GpuResource],
					},
				}

				podGroup := pod_group.Create(queue.GetConnectedNamespaceToQueue(testCtx.Queues[0]), "pod-affinity-"+utils.GenerateRandomK8sName(10), testCtx.Queues[0].Name)
				podGroup.Spec.PriorityClassName = "train"
				podGroup.Spec.MinMember = 2
				Expect(testCtx.ControllerClient.Create(ctx, podGroup, &runtimeClient.CreateOptions{})).To(Succeed())

				pod1 := rd.CreatePodWithPodGroupReference(testCtx.Queues[0], podGroup.Name, requirements)
				pod1.Labels[testGroupLabel] = "group-1"
				pod1.Spec.Affinity = podAffinity
				pod1.Name = "affinity-test-group-1-" + utils.GenerateRandomK8sName(5)
				pod2 := pod1.DeepCopy()
				pod2.Name = "affinity-test-group-2-" + utils.GenerateRandomK8sName(5)
				Expect(testCtx.ControllerClient.Create(ctx, pod1, &runtimeClient.CreateOptions{})).To(Succeed())
				Expect(testCtx.ControllerClient.Create(ctx, pod2, &runtimeClient.CreateOptions{})).To(Succeed())

				wait.ForAtLeastNPodsScheduled(ctx, testCtx.ControllerClient, namespace, []*v1.Pod{pod1, pod2}, 2)

				pod1FromCluster := &v1.Pod{}
				Expect(testCtx.ControllerClient.Get(ctx, runtimeClient.ObjectKeyFromObject(pod1), pod1FromCluster, &runtimeClient.GetOptions{})).To(Succeed())
				pod2FromCluster := &v1.Pod{}
				Expect(testCtx.ControllerClient.Get(ctx, runtimeClient.ObjectKeyFromObject(pod2), pod2FromCluster, &runtimeClient.GetOptions{})).To(Succeed())

				nodesToGroup := map[string]string{
					gpuNodes[0].Name: gpuNodes[0].Labels[testGroupLabel],
					gpuNodes[1].Name: gpuNodes[1].Labels[testGroupLabel],
					gpuNodes[2].Name: gpuNodes[2].Labels[testGroupLabel],
					gpuNodes[3].Name: gpuNodes[3].Labels[testGroupLabel],
				}

				Expect(nodesToGroup[pod1FromCluster.Spec.NodeName]).To(Equal(nodesToGroup[pod2FromCluster.Spec.NodeName]))
			})
		})
	})
})

func submitPod(
	ctx context.Context, client *kubernetes.Clientset,
	queue *v2.Queue, affinity *v1.Affinity,
	extraLabels map[string]string,
) *v1.Pod {
	requirements := v1.ResourceRequirements{
		Limits: map[v1.ResourceName]resource.Quantity{
			constants.GpuResource: resource.MustParse("1"),
		},
	}
	pod := rd.CreatePodObject(queue, requirements)
	pod.Spec.Affinity = affinity
	maps.Copy(pod.ObjectMeta.Labels, extraLabels)
	pod, err := rd.CreatePod(ctx, client, pod)
	Expect(err).To(Succeed())
	return pod
}

func submitWaitAndGetPod(
	ctx context.Context, testCtx *testcontext.TestContext,
	queue *v2.Queue, affinity *v1.Affinity,
	extraLabels map[string]string,
) *v1.Pod {
	pod := submitPod(ctx, testCtx.KubeClientset, queue, affinity, extraLabels)
	wait.ForPodScheduled(ctx, testCtx.ControllerClient, pod)
	Expect(testCtx.ControllerClient.Get(ctx, runtimeClient.ObjectKeyFromObject(pod), pod)).To(Succeed())
	return pod
}
