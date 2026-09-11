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
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestParseEndpoint(t *testing.T) {
	endpoint, err := ParseEndpoint("west=https://cp.example.test:26080/")
	if err != nil {
		t.Fatalf("ParseEndpoint() error = %v", err)
	}
	if endpoint.ID != "static/west" || endpoint.Name != "west" || endpoint.URL != "https://cp.example.test:26080" {
		t.Fatalf("ParseEndpoint() = %#v", endpoint)
	}
	if _, err := ParseEndpoint("missing-scheme"); err == nil {
		t.Fatal("ParseEndpoint() accepted endpoint without a scheme")
	}
	if _, err := ParseEndpoint("https://user:secret@example.test"); err == nil {
		t.Fatal("ParseEndpoint() accepted URL credentials")
	}
}

func TestKubernetesProviderDiscoversEveryReadyEndpoint(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer test-token" {
			t.Fatalf("Authorization = %q", request.Header.Get("Authorization"))
		}
		if !strings.Contains(request.URL.RawQuery, "kubernetes.io%2Fservice-name%3Ddubbod-management") {
			t.Fatalf("query = %q", request.URL.RawQuery)
		}
		_, _ = writer.Write([]byte(`{
		  "items": [{
		    "metadata": {"name": "dubbod-management-1"},
		    "ports": [{"name": "other", "port": 9}, {"name": "management", "port": 26080}],
		    "endpoints": [
		      {"addresses": ["10.0.0.1"], "conditions": {"ready": true}, "targetRef": {"name": "dubbod-a"}},
		      {"addresses": ["10.0.0.2"], "conditions": {"ready": true}, "targetRef": {"name": "dubbod-b"}},
		      {"addresses": ["10.0.0.3"], "conditions": {"ready": false}, "targetRef": {"name": "dubbod-c"}}
		    ]
		  }]
		}`))
	}))
	defer server.Close()

	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("test-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	provider, enabled, err := NewKubernetesProvider(
		server.URL, "dubbo-system", "dubbod-management", "management", "http", "", tokenFile, "",
	)
	if err != nil || !enabled {
		t.Fatalf("NewKubernetesProvider() = %v, %v", enabled, err)
	}
	endpoints, err := provider.Discover(context.Background())
	if err != nil {
		t.Fatalf("Discover() error = %v", err)
	}
	if len(endpoints) != 2 {
		t.Fatalf("len(endpoints) = %d, want 2", len(endpoints))
	}
	if endpoints[0].URL != "http://10.0.0.1:26080" || endpoints[1].URL != "http://10.0.0.2:26080" {
		t.Fatalf("endpoints = %#v", endpoints)
	}
}

func TestKubeconfigProvidersDiscoverAcrossContextsAndUsePodProxy(t *testing.T) {
	newAPIServer := func(cluster, pod string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			switch {
			case strings.Contains(request.URL.Path, "/endpointslices"):
				_, _ = fmt.Fprintf(writer, `{
				  "items": [{
				    "ports": [{"name": "management", "port": 26080}],
				    "endpoints": [{"addresses": ["10.0.0.1"], "conditions": {"ready": true}, "targetRef": {"name": %q}}]
				  }]
				}`, pod)
			case strings.Contains(request.URL.Path, "/pods/") && strings.HasSuffix(request.URL.Path, "/api/v1/overview"):
				writeTestOverview(writer, testOverview(cluster, pod, pod+"-node"))
			default:
				http.NotFound(writer, request)
			}
		}))
	}
	east := newAPIServer("east", "dubbod-east")
	defer east.Close()
	west := newAPIServer("west", "dubbod-west")
	defer west.Close()

	kubeconfig := filepath.Join(t.TempDir(), "config")
	config := fmt.Sprintf(`{
	  "apiVersion": "v1",
	  "kind": "Config",
	  "clusters": [
	    {"name": "east", "cluster": {"server": %q}},
	    {"name": "west", "cluster": {"server": %q}}
	  ],
	  "users": [{"name": "test", "user": {}}],
	  "contexts": [
	    {"name": "east", "context": {"cluster": "east", "user": "test"}},
	    {"name": "west", "context": {"cluster": "west", "user": "test"}}
	  ],
	  "current-context": "east"
	}`, east.URL, west.URL)
	if err := os.WriteFile(kubeconfig, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	providers, err := KubeconfigProviders(
		kubeconfig, []string{"east", "west"}, "dubbo-system", "dubbod-management", "management",
	)
	if err != nil {
		t.Fatalf("KubeconfigProviders() error = %v", err)
	}
	discovery := NewDiscovery(providers...)
	discovery.Refresh(context.Background())
	if len(discovery.Errors()) != 0 {
		t.Fatalf("discovery errors = %v", discovery.Errors())
	}
	if got := len(discovery.Endpoints()); got != 2 {
		t.Fatalf("len(endpoints) = %d, want 2", got)
	}

	aggregator := &Aggregator{Discovery: discovery, Client: NewClient(2 * time.Second)}
	overview, err := aggregator.Overview(context.Background(), AllControlPlanes)
	if err != nil {
		t.Fatalf("Overview() error = %v", err)
	}
	if len(overview.Clusters) != 2 || len(overview.ControlPlanes) != 2 {
		t.Fatalf("clusters/controlPlanes = %d/%d, want 2/2", len(overview.Clusters), len(overview.ControlPlanes))
	}
}
