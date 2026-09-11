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
	"sort"
	"time"
)

// aggregateOverviews merges per-control-plane overviews into one view.
//
// Cluster-scoped collections come from representatives only (one snapshot per
// cluster), while per-replica collections — xDS clients and the data plane —
// are merged across every snapshot, because a workload connects to exactly one
// replica and would otherwise disappear.
func aggregateOverviews(snapshots []snapshot, statuses []ControlPlaneStatus, scope string) Overview {
	if len(snapshots) == 1 && scope != AllControlPlanes {
		out := snapshots[0].overview
		annotateOverview(&out, snapshots[0])
		out.ControlPlanes = statuses
		out.Clusters = clusterSummaries(statuses)
		out.Scope = scope
		out.UpdatedAt = time.Now().UTC()
		return out
	}

	representatives := representativeSnapshots(snapshots)
	out := Overview{
		Product:       "dubbod GUI",
		Version:       commonVersion(snapshots),
		ClusterID:     commonCluster(representatives),
		Namespace:     commonNamespace(representatives),
		Status:        allStatuses(snapshots),
		DataPlanePods: make(map[string]int),
		ControlPlanes: statuses,
		Clusters:      clusterSummaries(statuses),
		Scope:         scope,
		UpdatedAt:     time.Now().UTC(),
	}
	if len(representatives) > 0 {
		out.Mesh = representatives[0].overview.Mesh
	}

	mergeClusterState(&out, representatives, snapshots)
	mergeReplicaState(&out, snapshots)

	sortOverview(&out)
	out.Counts.Services = len(out.Services)
	out.Counts.Registries = len(out.Registries)
	out.Counts.XDSConnections = len(out.XDSClients)
	out.Counts.HTTPRoutes = len(out.Routes)
	return out
}

// mergeClusterState folds in the collections that every replica of a cluster
// reports identically. allSnapshots is only consulted to attribute an instance
// to the replica that actually is that pod.
func mergeClusterState(out *Overview, representatives, allSnapshots []snapshot) {
	serviceKeys := map[string]struct{}{}
	instanceKeys := map[string]struct{}{}
	gatewayKeys := map[string]struct{}{}
	routeKeys := map[string]struct{}{}
	registryKeys := map[string]struct{}{}
	configKinds := map[string]ConfigKind{}

	for _, item := range representatives {
		cluster := clusterKey(item)
		overview := item.overview
		out.Counts.EndpointServices += overview.Counts.EndpointServices
		out.Counts.PeerAuthentications += overview.Counts.PeerAuthentications
		out.Counts.RequestAuthentications += overview.Counts.RequestAuthentications
		out.Counts.AuthorizationPolicies += overview.Counts.AuthorizationPolicies
		out.Counts.GatewayClasses += overview.Counts.GatewayClasses
		out.Counts.Gateways += overview.Counts.Gateways

		for _, kind := range overview.ConfigKinds {
			current := configKinds[kind.Kind]
			current.Kind = kind.Kind
			current.Description = kind.Description
			current.Count += kind.Count
			configKinds[kind.Kind] = current
		}
		for _, registry := range overview.Registries {
			if !claim(registryKeys, cluster, registry.Provider, registry.Cluster) {
				continue
			}
			out.Registries = append(out.Registries, registry)
		}
		for _, service := range overview.Services {
			if !claim(serviceKeys, cluster, service.Namespace, service.Hostname) {
				continue
			}
			service.Cluster = cluster
			service.ControlPlane = item.endpoint.ID
			out.Services = append(out.Services, service)
		}
		for _, instance := range overview.Instances {
			if !claim(instanceKeys, cluster, instance.Namespace, instance.Name) {
				continue
			}
			instance.Cluster = cluster
			instance.ControlPlane = endpointIDForPod(allSnapshots, cluster, instance.Name, item.endpoint.ID)
			out.Instances = append(out.Instances, instance)
		}
		for _, instance := range overview.GatewayInstances {
			if !claim(gatewayKeys, cluster, instance.Namespace, instance.Name) {
				continue
			}
			instance.Cluster = cluster
			instance.ControlPlane = item.endpoint.ID
			out.GatewayInstances = append(out.GatewayInstances, instance)
		}
		for _, route := range overview.Routes {
			if !claim(routeKeys, cluster, route.Namespace, route.Name) {
				continue
			}
			route.Cluster = cluster
			route.ControlPlane = item.endpoint.ID
			out.Routes = append(out.Routes, route)
		}
		for namespace, count := range overview.DataPlanePods {
			out.DataPlanePods[cluster+"/"+namespace] += count
			if len(representatives) == 1 {
				out.DataPlanePods[namespace] = count
			}
		}
	}

	for _, kind := range configKinds {
		out.ConfigKinds = append(out.ConfigKinds, kind)
	}
}

// mergeReplicaState folds in state that differs per replica. A proxy holds its
// xDS stream against a single replica, so these must come from every snapshot;
// when replicas disagree the connected view wins.
func mergeReplicaState(out *Overview, snapshots []snapshot) {
	workloads := map[string]Workload{}
	xdsClients := map[string]XDSClient{}

	for _, item := range snapshots {
		cluster := clusterKey(item)
		for _, client := range item.overview.XDSClients {
			client.Cluster = cluster
			client.ControlPlane = item.endpoint.ID
			key := joinKey(cluster, client.NodeID)
			current, exists := xdsClients[key]
			if !exists || client.ConnectedAt.After(current.ConnectedAt) {
				xdsClients[key] = client
			}
		}
		for _, workload := range item.overview.DataPlane {
			key := joinKey(cluster, workload.Namespace, workload.Name)
			workload.Cluster = cluster
			workload.ControlPlane = item.endpoint.ID
			current, exists := workloads[key]
			if !exists || workload.Connected && !current.Connected {
				workloads[key] = workload
			}
		}
	}

	for _, workload := range workloads {
		out.DataPlane = append(out.DataPlane, workload)
	}
	for _, client := range xdsClients {
		out.XDSClients = append(out.XDSClients, client)
	}
}

// annotateOverview stamps cluster and control-plane attribution onto a single
// control plane's own overview, which is otherwise returned unmerged.
func annotateOverview(overview *Overview, item snapshot) {
	cluster := clusterKey(item)
	for index := range overview.Services {
		overview.Services[index].Cluster = cluster
		overview.Services[index].ControlPlane = item.endpoint.ID
	}
	for index := range overview.Instances {
		overview.Instances[index].Cluster = cluster
		overview.Instances[index].ControlPlane = endpointIDForPod([]snapshot{item}, cluster, overview.Instances[index].Name, item.endpoint.ID)
	}
	for index := range overview.GatewayInstances {
		overview.GatewayInstances[index].Cluster = cluster
		overview.GatewayInstances[index].ControlPlane = item.endpoint.ID
	}
	for index := range overview.Routes {
		overview.Routes[index].Cluster = cluster
		overview.Routes[index].ControlPlane = item.endpoint.ID
	}
	for index := range overview.XDSClients {
		overview.XDSClients[index].Cluster = cluster
		overview.XDSClients[index].ControlPlane = item.endpoint.ID
	}
	for index := range overview.DataPlane {
		overview.DataPlane[index].Cluster = cluster
		overview.DataPlane[index].ControlPlane = item.endpoint.ID
	}
}

// endpointIDForPod resolves a dubbod pod name back to the endpoint that is that
// pod, so a replica listed as an instance links to its own control plane rather
// than to whichever peer reported it.
func endpointIDForPod(snapshots []snapshot, cluster, podName, fallback string) string {
	for _, item := range snapshots {
		if clusterKey(item) == cluster && item.overview.PodName == podName {
			return item.endpoint.ID
		}
	}
	return fallback
}

func overviewHealthy(status OverviewStatus) bool {
	return status.XDSServerReady && status.CachesSynced && status.ServicesSynced && status.ConfigSynced
}

// allStatuses ANDs the readiness flags: the fleet is ready only when every
// control plane is.
func allStatuses(snapshots []snapshot) OverviewStatus {
	out := OverviewStatus{
		XDSServerReady:  true,
		CachesSynced:    true,
		ServicesSynced:  true,
		ConfigSynced:    true,
		ProxylessSynced: true,
		InjectorReady:   true,
		ValidationReady: true,
	}
	for _, item := range snapshots {
		status := item.overview.Status
		out.XDSServerReady = out.XDSServerReady && status.XDSServerReady
		out.CachesSynced = out.CachesSynced && status.CachesSynced
		out.ServicesSynced = out.ServicesSynced && status.ServicesSynced
		out.ConfigSynced = out.ConfigSynced && status.ConfigSynced
		out.ProxylessSynced = out.ProxylessSynced && status.ProxylessSynced
		out.InjectorReady = out.InjectorReady && status.InjectorReady
		out.ValidationReady = out.ValidationReady && status.ValidationReady
	}
	return out
}

func commonVersion(snapshots []snapshot) string {
	return common(snapshots, "mixed", func(item snapshot) string { return item.overview.Version })
}

func commonCluster(snapshots []snapshot) string {
	return common(snapshots, "multiple", clusterKey)
}

func commonNamespace(snapshots []snapshot) string {
	return common(snapshots, "multiple", func(item snapshot) string { return item.overview.Namespace })
}

// common returns the value shared by every snapshot, or disagreed when they
// differ.
func common(snapshots []snapshot, disagreed string, value func(snapshot) string) string {
	out := ""
	for _, item := range snapshots {
		current := value(item)
		if out == "" {
			out = current
		} else if current != out {
			return disagreed
		}
	}
	return out
}

func clusterSummaries(statuses []ControlPlaneStatus) []ClusterSummary {
	byID := map[string]ClusterSummary{}
	for _, status := range statuses {
		id := status.ClusterID
		if id == "" {
			id = status.ID
		}
		current := byID[id]
		current.ID = id
		current.ControlPlanes++
		if status.Healthy {
			current.HealthyInstances++
		}
		byID[id] = current
	}
	out := make([]ClusterSummary, 0, len(byID))
	for _, summary := range byID {
		out = append(out, summary)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// sortOverview gives the merged collections a stable order, so the console does
// not reshuffle rows between refreshes just because map iteration changed.
func sortOverview(overview *Overview) {
	sort.Slice(overview.Services, func(i, j int) bool {
		return joinKey(overview.Services[i].Cluster, overview.Services[i].Hostname) <
			joinKey(overview.Services[j].Cluster, overview.Services[j].Hostname)
	})
	sort.Slice(overview.Instances, func(i, j int) bool {
		return joinKey(overview.Instances[i].Cluster, overview.Instances[i].Name) <
			joinKey(overview.Instances[j].Cluster, overview.Instances[j].Name)
	})
	sort.Slice(overview.GatewayInstances, func(i, j int) bool {
		return joinKey(overview.GatewayInstances[i].Cluster, overview.GatewayInstances[i].Name) <
			joinKey(overview.GatewayInstances[j].Cluster, overview.GatewayInstances[j].Name)
	})
	sort.Slice(overview.Routes, func(i, j int) bool {
		return joinKey(overview.Routes[i].Cluster, overview.Routes[i].Namespace, overview.Routes[i].Name) <
			joinKey(overview.Routes[j].Cluster, overview.Routes[j].Namespace, overview.Routes[j].Name)
	})
	sort.Slice(overview.XDSClients, func(i, j int) bool {
		return joinKey(overview.XDSClients[i].Cluster, overview.XDSClients[i].ControlPlane, overview.XDSClients[i].NodeID) <
			joinKey(overview.XDSClients[j].Cluster, overview.XDSClients[j].ControlPlane, overview.XDSClients[j].NodeID)
	})
	sort.Slice(overview.DataPlane, func(i, j int) bool {
		return joinKey(overview.DataPlane[i].Cluster, overview.DataPlane[i].Namespace, overview.DataPlane[i].Name) <
			joinKey(overview.DataPlane[j].Cluster, overview.DataPlane[j].Namespace, overview.DataPlane[j].Name)
	})
	sort.Slice(overview.ConfigKinds, func(i, j int) bool { return overview.ConfigKinds[i].Kind < overview.ConfigKinds[j].Kind })
}
