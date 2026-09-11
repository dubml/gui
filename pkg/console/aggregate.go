// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package console

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"
)

// Aggregator fans a console request out to every discovered control plane and
// merges the replies. Merge rules live in overview.go and metrics.go; scope
// selection lives in scope.go.
type Aggregator struct {
	Discovery *Discovery
	Client    *Client
}

// snapshot is one control plane's reply to a single probe round.
type snapshot struct {
	endpoint Endpoint
	overview Overview
	latency  time.Duration
	err      error
}

func (aggregator *Aggregator) Overview(ctx context.Context, scope string) (Overview, error) {
	snapshots, statuses, err := aggregator.probe(ctx)
	if err != nil {
		return Overview{}, err
	}
	selected, err := selectSnapshots(snapshots, scope)
	if err != nil {
		return Overview{}, err
	}
	return aggregateOverviews(selected, statuses, normalizeScope(scope)), nil
}

func (aggregator *Aggregator) Metrics(ctx context.Context, scope string) (MetricsResponse, error) {
	snapshots, statuses, err := aggregator.probe(ctx)
	if err != nil {
		return MetricsResponse{}, err
	}
	selected, err := selectSnapshots(snapshots, scope)
	if err != nil {
		return MetricsResponse{}, err
	}

	responses, fetchErrors := fetchAll(selected, func(item snapshot) (MetricsResponse, error) {
		return aggregator.Client.Metrics(ctx, item.endpoint)
	})
	if len(responses) == 0 {
		return MetricsResponse{}, errors.Join(fetchErrors...)
	}
	return aggregateMetrics(responses, statuses, normalizeScope(scope)), nil
}

// Logs collects pod logs. Replicas of one control plane see the same pods, so
// an all-scope request queries one replica per cluster; duplicates that slip
// through are still dropped by cluster/pod/container.
func (aggregator *Aggregator) Logs(ctx context.Context, scope string, query url.Values) (LogsResponse, error) {
	snapshots, _, err := aggregator.probe(ctx)
	if err != nil {
		return LogsResponse{}, err
	}
	selected, err := selectSnapshots(snapshots, scope)
	if err != nil {
		return LogsResponse{}, err
	}
	if normalizeScope(scope) == AllControlPlanes {
		selected = representativeSnapshots(selected)
	}

	type reply struct {
		snapshot snapshot
		logs     LogsResponse
	}
	replies, fetchErrors := fetchAll(selected, func(item snapshot) (reply, error) {
		logs, fetchErr := aggregator.Client.Logs(ctx, item.endpoint, query)
		return reply{snapshot: item, logs: logs}, fetchErr
	})

	out := LogsResponse{UpdatedAt: time.Now().UTC()}
	seen := map[string]struct{}{}
	for _, item := range replies {
		if out.Kind == "" {
			out.Kind = item.logs.Kind
			out.Name = item.logs.Name
			out.Namespace = item.logs.Namespace
		}
		cluster := clusterKey(item.snapshot)
		for _, pod := range item.logs.Pods {
			if !claim(seen, cluster, pod.Name, pod.Container) {
				continue
			}
			pod.ControlPlane = item.snapshot.endpoint.ID
			pod.Cluster = cluster
			out.Pods = append(out.Pods, pod)
		}
	}
	if len(out.Pods) == 0 && len(fetchErrors) > 0 {
		return LogsResponse{}, errors.Join(fetchErrors...)
	}
	sort.Slice(out.Pods, func(i, j int) bool {
		return joinKey(out.Pods[i].Cluster, out.Pods[i].Name, out.Pods[i].Container) <
			joinKey(out.Pods[j].Cluster, out.Pods[j].Name, out.Pods[j].Container)
	})
	return out, nil
}

// probe fetches an overview from every discovered endpoint in parallel. Failures
// are recorded as unhealthy statuses rather than aborting the round, so one dead
// replica does not blank out the console.
func (aggregator *Aggregator) probe(ctx context.Context) ([]snapshot, []ControlPlaneStatus, error) {
	endpoints := aggregator.Discovery.Endpoints()
	if len(endpoints) == 0 {
		details := make([]string, 0)
		for _, err := range aggregator.Discovery.Errors() {
			details = append(details, err.Error())
		}
		if len(details) == 0 {
			return nil, nil, errors.New("no control-plane endpoints discovered")
		}
		return nil, nil, fmt.Errorf("no control-plane endpoints discovered: %s", strings.Join(details, "; "))
	}

	snapshots := make([]snapshot, len(endpoints))
	var wait sync.WaitGroup
	for index, endpoint := range endpoints {
		wait.Add(1)
		go func(index int, endpoint Endpoint) {
			defer wait.Done()
			overview, latency, err := aggregator.Client.Overview(ctx, endpoint)
			snapshots[index] = snapshot{endpoint: endpoint, overview: overview, latency: latency, err: err}
		}(index, endpoint)
	}
	wait.Wait()

	statuses := make([]ControlPlaneStatus, 0, len(snapshots))
	for _, item := range snapshots {
		status := ControlPlaneStatus{
			Endpoint:  item.endpoint,
			ClusterID: item.overview.ClusterID,
			Namespace: item.overview.Namespace,
			PodName:   item.overview.PodName,
			Version:   item.overview.Version,
			Latency:   item.latency,
			LatencyMS: item.latency.Milliseconds(),
			Healthy:   item.err == nil && overviewHealthy(item.overview.Status),
		}
		if item.err != nil {
			status.Error = item.err.Error()
		}
		statuses = append(statuses, status)
	}
	sort.Slice(snapshots, func(i, j int) bool { return snapshots[i].endpoint.ID < snapshots[j].endpoint.ID })
	sort.Slice(statuses, func(i, j int) bool { return statuses[i].ID < statuses[j].ID })
	return snapshots, statuses, nil
}

// fetchAll runs fetch against every snapshot in parallel, returning the
// successful replies and the errors separately so callers can decide whether a
// partial result is still worth serving.
func fetchAll[T any](snapshots []snapshot, fetch func(snapshot) (T, error)) ([]T, []error) {
	values := make([]T, len(snapshots))
	errs := make([]error, len(snapshots))
	var wait sync.WaitGroup
	for index, item := range snapshots {
		wait.Add(1)
		go func(index int, item snapshot) {
			defer wait.Done()
			values[index], errs[index] = fetch(item)
		}(index, item)
	}
	wait.Wait()

	out := make([]T, 0, len(values))
	var fetchErrors []error
	for index, err := range errs {
		if err != nil {
			fetchErrors = append(fetchErrors, err)
			continue
		}
		out = append(out, values[index])
	}
	return out, fetchErrors
}

// joinKey builds a composite key from parts that may themselves contain any
// printable character, using NUL as the separator.
func joinKey(parts ...string) string {
	return strings.Join(parts, "\x00")
}

// claim records a composite key and reports whether it was new, collapsing the
// "have I already merged this?" checks into one call.
func claim(seen map[string]struct{}, parts ...string) bool {
	key := joinKey(parts...)
	if _, exists := seen[key]; exists {
		return false
	}
	seen[key] = struct{}{}
	return true
}
