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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

func TestAggregatorGroupsReplicasWithoutDuplicatingClusterState(t *testing.T) {
	endpoints := []Endpoint{}
	servers := []*httptest.Server{}
	for _, item := range []struct {
		name     string
		overview Overview
	}{
		{name: "a", overview: testOverview("cluster-a", "dubbod-a", "node-a")},
		{name: "b", overview: testOverview("cluster-a", "dubbod-b", "node-b")},
		{name: "c", overview: testOverview("cluster-b", "dubbod-c", "node-c")},
	} {
		item := item
		server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			switch request.URL.Path {
			case "/api/v1/overview":
				writeTestOverview(writer, item.overview)
			case "/api/v1/metrics":
				value := float64(1)
				_ = json.NewEncoder(writer).Encode(MetricsResponse{
					Families: []MetricFamily{{
						Name: "dubbod_inbound_updates", Type: "COUNTER",
						Metrics: []MetricSample{{Labels: map[string]string{"type": "service"}, Value: &value}},
					}},
				})
			case "/api/v1/logs":
				_ = json.NewEncoder(writer).Encode(LogsResponse{
					Kind: "dubbod", Name: "dubbod", Namespace: "dubbo-system",
					Pods: []PodLog{{Name: item.overview.PodName, Container: "execute", Ready: true}},
				})
			default:
				http.NotFound(writer, request)
			}
		}))
		servers = append(servers, server)
		endpoints = append(endpoints, Endpoint{ID: "static/" + item.name, Name: item.name, URL: server.URL, Source: "static"})
	}
	defer func() {
		for _, server := range servers {
			server.Close()
		}
	}()

	discovery := NewDiscovery(StaticProvider(endpoints))
	discovery.Refresh(context.Background())
	aggregator := &Aggregator{Discovery: discovery, Client: NewClient(2 * time.Second)}

	overview, err := aggregator.Overview(context.Background(), AllControlPlanes)
	if err != nil {
		t.Fatalf("Overview() error = %v", err)
	}
	if overview.ClusterID != "multiple" {
		t.Fatalf("clusterId = %q, want multiple", overview.ClusterID)
	}
	if got := len(overview.Services); got != 2 {
		t.Fatalf("services = %d, want one per cluster", got)
	}
	if got := len(overview.Instances); got != 3 {
		t.Fatalf("instances = %d, want 3", got)
	}
	if got := len(overview.XDSClients); got != 3 {
		t.Fatalf("xDS clients = %d, want 3", got)
	}
	if got := len(overview.Clusters); got != 2 {
		t.Fatalf("clusters = %d, want 2", got)
	}
	if got := len(overview.ControlPlanes); got != 3 {
		t.Fatalf("control planes = %d, want 3", got)
	}

	metrics, err := aggregator.Metrics(context.Background(), AllControlPlanes)
	if err != nil {
		t.Fatalf("Metrics() error = %v", err)
	}
	if got := *metrics.Families[0].Metrics[0].Value; got != 3 {
		t.Fatalf("aggregated metric = %v, want 3", got)
	}

	logs, err := aggregator.Logs(context.Background(), AllControlPlanes, url.Values{"kind": {"dubbod"}})
	if err != nil {
		t.Fatalf("Logs() error = %v", err)
	}
	if got := len(logs.Pods); got != 2 {
		t.Fatalf("logs pods = %d, want one representative query per cluster", got)
	}

	selected, err := aggregator.Overview(context.Background(), "static/b")
	if err != nil {
		t.Fatalf("selected Overview() error = %v", err)
	}
	if selected.PodName != "dubbod-b" || selected.Scope != "static/b" {
		t.Fatalf("selected overview = pod %q scope %q", selected.PodName, selected.Scope)
	}
}

func TestAggregatorKeepsHealthyControlPlaneWhenPeerFails(t *testing.T) {
	healthy := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/api/v1/overview" {
			http.NotFound(writer, request)
			return
		}
		writeTestOverview(writer, testOverview("cluster-a", "dubbod-a", "node-a"))
	}))
	defer healthy.Close()
	failed := httptest.NewServer(http.NotFoundHandler())
	failedURL := failed.URL
	failed.Close()

	discovery := NewDiscovery(StaticProvider{
		{ID: "static/healthy", Name: "healthy", URL: healthy.URL, Source: "static"},
		{ID: "static/failed", Name: "failed", URL: failedURL, Source: "static"},
	})
	discovery.Refresh(context.Background())
	aggregator := &Aggregator{Discovery: discovery, Client: NewClient(200 * time.Millisecond)}

	overview, err := aggregator.Overview(context.Background(), AllControlPlanes)
	if err != nil {
		t.Fatalf("Overview(all) error = %v", err)
	}
	if len(overview.ControlPlanes) != 2 {
		t.Fatalf("control planes = %d, want 2", len(overview.ControlPlanes))
	}
	if overview.ControlPlanes[0].Healthy == overview.ControlPlanes[1].Healthy {
		t.Fatalf("statuses = %#v, want one healthy and one failed", overview.ControlPlanes)
	}
	if _, err := aggregator.Overview(context.Background(), "static/failed"); err == nil {
		t.Fatal("Overview(failed) succeeded")
	}
}

func testOverview(cluster, pod, node string) Overview {
	status := OverviewStatus{
		XDSServerReady: true, CachesSynced: true, ServicesSynced: true, ConfigSynced: true,
		ProxylessSynced: true, InjectorReady: true, ValidationReady: true,
	}
	instances := []DubbodInstance{{Name: pod, Namespace: "dubbo-system", IP: "10.0.0.1", IsReady: true}}
	if cluster == "cluster-a" {
		instances = []DubbodInstance{
			{Name: "dubbod-a", Namespace: "dubbo-system", IP: "10.0.0.1", IsReady: true},
			{Name: "dubbod-b", Namespace: "dubbo-system", IP: "10.0.0.2", IsReady: true},
		}
	}
	return Overview{
		Product: "Dubbo", Version: "test", ClusterID: cluster, Namespace: "dubbo-system", PodName: pod,
		Status:        status,
		Counts:        OverviewCounts{Services: 1, EndpointServices: 1, XDSConnections: 1},
		Services:      []Service{{Name: "echo", Hostname: "echo.default.svc.cluster.local", Namespace: "default"}},
		Instances:     instances,
		DataPlane:     []Workload{{Name: "echo-1", Namespace: "default", Connected: true, NodeID: node}},
		DataPlanePods: map[string]int{"default": 1},
		XDSClients:    []XDSClient{{NodeID: node, ConnectedAt: time.Now().UTC()}},
	}
}

func writeTestOverview(writer http.ResponseWriter, overview Overview) {
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(overview)
}
