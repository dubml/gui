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
	"errors"
	"fmt"
	"sort"
	"strings"
)

// AllControlPlanes is the scope that spans every discovered control plane. It
// is also the scope an empty request falls back to.
const AllControlPlanes = "all"

func normalizeScope(scope string) string {
	scope = strings.TrimSpace(scope)
	if scope == "" {
		return AllControlPlanes
	}
	return scope
}

// selectSnapshots narrows probe results to the requested scope, dropping the
// endpoints that failed. It only reports an error when nothing usable is left,
// so one unreachable replica never blanks out a healthy peer.
func selectSnapshots(snapshots []snapshot, scope string) ([]snapshot, error) {
	scope = normalizeScope(scope)
	var selected []snapshot
	var fetchErrors []error
	for _, item := range snapshots {
		if scope != AllControlPlanes && item.endpoint.ID != scope {
			continue
		}
		if item.err != nil {
			fetchErrors = append(fetchErrors, item.err)
			continue
		}
		selected = append(selected, item)
	}
	if len(selected) > 0 {
		return selected, nil
	}
	if scope != AllControlPlanes {
		for _, item := range snapshots {
			if item.endpoint.ID == scope && item.err != nil {
				return nil, item.err
			}
		}
		return nil, fmt.Errorf("unknown control plane %q", scope)
	}
	if len(fetchErrors) > 0 {
		return nil, errors.Join(fetchErrors...)
	}
	return nil, errors.New("no healthy control-plane endpoints")
}

// clusterKey identifies the cluster a snapshot describes. Replicas of one
// control plane report the same cluster ID, which is what lets the aggregator
// collapse them instead of counting their shared state twice.
func clusterKey(item snapshot) string {
	if item.overview.ClusterID != "" {
		return item.overview.ClusterID
	}
	return item.endpoint.ID
}

// representativeSnapshots keeps one snapshot per cluster. Cluster-wide state
// (services, routes, gateways) is identical across replicas, so merging every
// replica would inflate the totals.
func representativeSnapshots(snapshots []snapshot) []snapshot {
	byCluster := make(map[string]snapshot)
	for _, item := range snapshots {
		key := clusterKey(item)
		if _, exists := byCluster[key]; !exists {
			byCluster[key] = item
		}
	}
	out := make([]snapshot, 0, len(byCluster))
	for _, item := range byCluster {
		out = append(out, item)
	}
	sort.Slice(out, func(i, j int) bool { return clusterKey(out[i]) < clusterKey(out[j]) })
	return out
}
