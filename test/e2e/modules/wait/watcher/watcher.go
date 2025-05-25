/*
Copyright 2025 NVIDIA CORPORATION
SPDX-License-Identifier: Apache-2.0
*/
package watcher

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/watch"
	runtimeClient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/NVIDIA/KAI-scheduler/test/e2e/modules/utils"
)

type Interface interface {
	watch(ctx context.Context) watch.Interface
	sync(ctx context.Context)
	processEvent(ctx context.Context, event watch.Event)
	satisfied() bool
}

type CheckCondition func(watch.Event) bool

var (
	FlowTimeout         = 5 * time.Minute
	SteadyStateDuration = 10 * time.Second
)

func ForEvent(ctx context.Context, client runtimeClient.WithWatch, eventWatcher Interface) bool {
	return ForEventCustomTimeout(ctx, client, eventWatcher, FlowTimeout, SteadyStateDuration)
}

func ForEventCustomTimeout(ctx context.Context, client runtimeClient.WithWatch, eventWatcher Interface,
	waitTime time.Duration, steadyStateTime time.Duration) bool {
	logger := log.FromContext(ctx)

	watcher := eventWatcher.watch(ctx)
	defer watcher.Stop()

	eventWatcher.sync(ctx)

	timer := time.NewTimer(waitTime)
	defer timer.Stop()

	pollInterval := 2 * time.Second
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	var satisfiedSince *time.Time = nil

	for {
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.Canceled) {
				logger.Error(ctx.Err(), "WaitForEvent has been canceled")
				utils.LogClusterState(client, logger)
				return true
			}
			return false
		case <-timer.C:
			eventWatcher.sync(ctx)
			if eventWatcher.satisfied() {
				fmt.Printf("Time out satisfied\n")
				return true
			}
			logger.Error(nil, "WaitForEvent timed out")
			utils.LogClusterState(client, logger)
			return false
		case <-ticker.C:
			eventWatcher.sync(ctx)
			if eventWatcher.satisfied() {
				now := time.Now()
				if satisfiedSince == nil {
					satisfiedSince = &now
				} else {
					if now.Sub(*satisfiedSince) > steadyStateTime {
						fmt.Printf("Steady state duration %s reached\n", steadyStateTime)
						return true
					}
				}
			} else {
				if satisfiedSince != nil {
					satisfiedSince = nil
				}
			}
		case event := <-watcher.ResultChan():
			if event.Type == watch.Error {
				err := ignoreContextCancelled(event)
				if err != nil {
					logger.Error(err, "Error event received for WaitForEvent function")
					utils.LogClusterState(client, logger)
					return false
				}
				return true
			}

			eventWatcher.processEvent(ctx, event)
			if eventWatcher.satisfied() {
				now := time.Now()
				if satisfiedSince == nil {
					satisfiedSince = &now
				} else {
					if now.Sub(*satisfiedSince) > steadyStateTime {
						fmt.Printf("Steady state duration %s reached\n", steadyStateTime)
						return true
					}
				}
			} else {
				if satisfiedSince != nil {
					satisfiedSince = nil
				}
			}
		}
	}
}

func ignoreContextCancelled(event watch.Event) error {
	objectAsString := fmt.Sprintf("%v", event.Object)
	if strings.Contains(objectAsString, "context cancelled") {
		fmt.Printf("Cancellation event received %v, %v", event.Type, event.Object)
		return nil
	}

	return fmt.Errorf("error event received %v, %v", event.Type, event.Object)
}
